#!/usr/bin/env python3
"""Test the provisioned queries and pending durations with the pinned promtool."""

import json
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "monitoring/grafana/provisioning/alerting/alerts.json"


def main():
    config = json.loads(CONFIG.read_text())
    assert all(group["interval"] == "30s" for group in config["groups"])
    rules = {r["uid"].removeprefix("om-"): r for g in config["groups"] for r in g["rules"]}
    tests = []

    def series(name, values, **labels):
        labels = {"job": "open-meteo", "instance": "router", **labels}
        selector = ",".join(f'{k}="{v}"' for k, v in labels.items())
        return {"series": f"{name}{{{selector}}}", "values": values}

    def case(name, uid, inputs, checks, dimensions=None):
        rule = rules[uid]
        dimensions = {"instance": "router"} if dimensions is None else dimensions
        if uid in {"instance-unavailable", "memtx-pressure"}:
            dimensions = {"job": "open-meteo", **dimensions}
        tests.append({
            "name": name,
            "interval": "1m",
            "input_series": inputs,
            "alert_rule_test": [{
                "eval_time": when, "alertname": rule["title"],
                "exp_alerts": [{"exp_labels": {**rule["labels"], **dimensions}}] if firing else [],
            } for when, firing in checks],
        })

    case("healthy instance", "instance-unavailable", [series("up", "1+0x15")], [("5m", False)])
    case("down instance waits then fires and recovers", "instance-unavailable",
         [series("up", "0+0x3 1+0x10")], [("1m", False), ("2m", True), ("4m", False)])
    case("missing availability data", "instance-unavailable", [], [("2m", True)], {"job": "open-meteo"})

    def requests(ok, errors, cancelled=None):
        values = [series("http_server_request_latency_count", ok, path="/forecast", status="200"),
                  series("http_server_request_latency_count", errors, path="/forecast", status="503")]
        if cancelled:
            values.append(series("http_server_request_latency_count", cancelled,
                                 path="/forecast", status="cancelled"))
        return values

    case("5xx fires after pending period; cancellations excluded", "forecast-errors",
         requests("0+90x15", "0+10x15", "0+1000x15"), [("5m", False), ("6m", True)])
    case("exactly five percent is healthy", "forecast-errors",
         requests("0+95x15", "0+5x15"), [("10m", False)])
    case("too little traffic", "forecast-errors", requests("0+1x15", "0+1x15"), [("10m", False)])
    case("counter resets retain error ratio", "forecast-errors",
         requests("0+90x6 0+90x10", "0+10x6 0+10x10"), [("6m", True), ("12m", True)])
    case("errors recover after the rolling window", "forecast-errors",
         requests("0+90x6 640+100x10", "0+10x6 60+0x10"), [("6m", True), ("13m", False)])

    def latency(bucket_one, count="0+100x15"):
        return [series("http_server_request_latency_bucket", bucket_one, path="/forecast", status="200", le="1"),
                series("http_server_request_latency_bucket", count, path="/forecast", status="200", le="5"),
                series("http_server_request_latency_bucket", count, path="/forecast", status="200", le="+Inf"),
                series("http_server_request_latency_count", count, path="/forecast", status="200")]

    case("slow p95", "forecast-latency", latency("0+0x15"), [("5m", False), ("6m", True)])
    case("fast p95", "forecast-latency", latency("0+100x15"), [("10m", False)])
    case("slow but insufficient traffic", "forecast-latency", latency("0+0x15", "0+1x15"), [("10m", False)])
    case("exactly two seconds p95 is healthy", "forecast-latency", [
        series("http_server_request_latency_bucket", "0+95x15", path="/forecast", status="200", le="2"),
        series("http_server_request_latency_bucket", "0+100x15", path="/forecast", status="200", le="4"),
        series("http_server_request_latency_bucket", "0+100x15", path="/forecast", status="200", le="+Inf"),
        series("http_server_request_latency_count", "0+100x15", path="/forecast", status="200"),
    ], [("10m", False)])
    case("idle counters are healthy", "forecast-errors", requests("0+0x15", "0+0x15"), [("10m", False)])
    case("idle histogram is healthy", "forecast-latency", latency("0+0x15", "0+0x15"), [("10m", False)])

    rejection = lambda v: [series("forecast_rejected_requests_total", v, reason="concurrency_limit")]
    case("rejections meet threshold and pending duration", "fetch-capacity", rejection("0+1x15"),
         [("5m", False), ("7m", True)])
    case("rejections below threshold", "fetch-capacity", rejection("0+0x15"), [("10m", False)])
    case("cache operation labels preserved", "cache-failures",
         [series("forecast_cache_operations_total", "0+2x15", outcome="error", operation="get")],
         [("6m", False), ("8m", True)], {"instance": "router", "operation": "get"})

    def upstream(success, failure, **labels):
        return [series("forecast_upstream_attempts_total", success, outcome="response", status="200"),
                series("forecast_upstream_attempts_total", failure, **labels)]

    for outcome, status in [("transport", "none"), ("timeout", "none"), ("response", "429"), ("response", "503")]:
        case(f"upstream failure {outcome}/{status}", "upstream-instability",
             upstream("0+7x15", "0+3x15", outcome=outcome, status=status), [("6m", False), ("7m", True)])
    case("upstream exact twenty percent", "upstream-instability",
         upstream("0+8x15", "0+2x15", outcome="response", status="502"), [("10m", False)])
    case("upstream 400 is not instability", "upstream-instability",
         upstream("0+7x15", "0+3x15", outcome="response", status="400"), [("10m", False)])
    case("upstream minimum attempt count", "upstream-instability",
         upstream("0+1x15", "0+1x15", outcome="timeout", status="none"), [("10m", False)])
    case("memory pressure pending and recovery", "memtx-pressure",
         [series("tnt_slab_quota_used", "95+0x10 80+0x5"), series("tnt_slab_quota_size", "100+0x16")],
         [("9m", False), ("10m", True), ("11m", False)])
    case("memory exact ninety percent", "memtx-pressure",
         [series("tnt_slab_quota_used", "90+0x15"), series("tnt_slab_quota_size", "100+0x15")],
         [("15m", False)])
    for uid in rules:
        if uid != "instance-unavailable":
            case(f"{uid} absent or idle is healthy", uid, [], [("15m", False)])

    # Check minimum traffic and exact count boundaries independently of 'for'.
    tests.append({
        "name": "minimum twenty requests and five rejections are inclusive",
        "interval": "1m",
        "input_series": requests("0+3x15", "0+1x15") + rejection("0+1x15"),
        "promql_expr_test": [{
            "expr": rules[uid]["data"][0]["model"]["expr"], "eval_time": when,
            "exp_samples": [{"labels": '{instance="router"}', "value": value}],
        } for uid in ["forecast-errors", "fetch-capacity"] for when, value in [("4m", 0), ("5m", 1)]],
    })

    for rule in rules.values():
        assert rule["execErrState"] == "Error"
        assert rule["condition"] == "C"
        threshold = rule["data"][1]["model"]
        assert threshold["type"] == "threshold" and threshold["expression"] == "A"
        assert threshold["conditions"][0]["evaluator"] == {"type": "gt", "params": [0]}
    assert rules["instance-unavailable"]["noDataState"] == "Alerting"
    assert all(r["noDataState"] == "OK" for uid, r in rules.items() if uid != "instance-unavailable")

    prometheus_rules = {"groups": [{"name": "provisioned-alerts", "rules": [{
        "alert": rule["title"], "expr": f'({rule["data"][0]["model"]["expr"]}) > 0',
        "for": rule["for"], "labels": rule["labels"],
    } for rule in rules.values()]}]}
    compose = json.loads(subprocess.check_output(
        ["docker", "compose", "-f", str(ROOT / "compose.monitoring.yml"), "config", "--format", "json"]))
    image = compose["services"]["prometheus"]["image"]
    with tempfile.TemporaryDirectory(prefix="open-meteo-alert-tests-") as directory:
        path = Path(directory)
        path.chmod(0o755)  # promtool runs as the image's unprivileged user.
        (path / "rules.json").write_text(json.dumps(prometheus_rules))
        (path / "tests.json").write_text(json.dumps({
            "rule_files": ["rules.json"], "evaluation_interval": "1m", "tests": tests,
        }))
        subprocess.run(["docker", "run", "--rm", "--volume", f"{path}:/tests:ro", "--workdir", "/tests",
                        "--entrypoint", "promtool", image, "test", "rules", "tests.json"], check=True)
    print(f"Validated {len(rules)} provisioned rules across {len(tests)} deterministic scenarios")


if __name__ == "__main__":
    main()
