# Monitoring configuration

See the main README for startup, login, target configuration, and data removal.

The local stack uses Prometheus 3.16.0 and Grafana 13.3.1. Stored metrics and
Grafana settings survive monitoring restarts.

## Request IDs and metric reference

Every forecast response includes `X-Request-ID`, including cache hits. Incoming
IDs must contain 1–128 ASCII letters, digits, periods, underscores, colons, or
hyphens. Invalid or repeated IDs are replaced with a generated UUID.
Use the ID to correlate router and storage logs; it is not sent upstream.

Request completion summaries are logged at info, stage details at debug, and
failures at warning/error. Request events omit response bodies and full queries.

| Metric | Meaning |
| --- | --- |
| `http_server_request_latency_count`, `_sum`, `_bucket` | HTTP request counts and latency |
| `forecast_cache_operations_total` | Cache operations by operation and outcome |
| `forecast_upstream_attempts_total` | Upstream attempts by outcome and status |
| `forecast_upstream_attempt_duration_seconds` | Upstream attempt duration |
| `forecast_upstream_retries_total` | Retries started, by reason |
| `forecast_active_fetches` | Fetches occupying a concurrency slot |
| `forecast_rejected_requests_total` | Requests rejected by the concurrency limit |

HTTP metrics use `status="cancelled"` for cancellations without a response.
Request IDs, query strings, and bucket IDs are not metric labels. Disconnected
clients can leave a fetch running, so active fetch counts can remain elevated
until those fetches finish.

## Dashboard attribution

`grafana/dashboards/tarantool.json` is the official **Tarantool Cartridge dashboard**,
Grafana dashboard 13054, revision 22:

- Source: https://grafana.com/api/dashboards/13054/revisions/22/download
- Project: https://github.com/tarantool/grafana-dashboard
- Copyright (c) 2020–2021 Tarantool, MIT license; see
  `grafana/dashboards/TARANTOOL-LICENSE`.

The dashboard is configured to use this stack's Prometheus data source. Its
queries and panels are retained from upstream.

`grafana/dashboards/forecast.json` is the application dashboard. Its instance
selector includes all five configured targets; request metrics are emitted on
routers. The cache hit ratio excludes lookup errors.

## Configuration checks

```bash
docker compose -f compose.monitoring.yml config --quiet
docker compose -f compose.monitoring.yml run --rm --no-deps \
  --entrypoint promtool prometheus check config /etc/prometheus/prometheus.yml
tarantool monitoring/test_alerts.lua
```

The alert tests require Tarantool and Docker. They check thresholds, pending
durations, low traffic, missing metrics, and recovery.

## Alerts

Grafana-managed rules are provisioned from `grafana/provisioning/alerting/alerts.json`
in the **Open-Meteo** folder. See [alert thresholds and response steps](ALERTS.md).
Rules use the existing Prometheus data source; no separate Alertmanager is needed.
After adding the provisioning mount, apply it with:

```bash
docker compose -f compose.monitoring.yml up -d grafana
```

For subsequent changes to the rules file, restart Grafana:

```bash
docker compose -f compose.monitoring.yml restart grafana
```

No external contact points or notification policies are provisioned. Existing
policies are retained; on a fresh stack, view alerts in Grafana until notification
delivery is configured. Stopping the application intentionally makes its targets
unavailable and will trigger availability alerts after two minutes.

No logs or request bodies are exported by this stack. It collects metrics only;
it does not provision a log or distributed-trace backend.
