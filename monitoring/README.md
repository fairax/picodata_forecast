# Monitoring configuration

See the main README for startup, login, target configuration, and data removal.

The local stack pins Prometheus 3.16.0 and Grafana 13.3.1. Configuration and dashboard
definitions are mounted read-only; metrics and Grafana settings use named volumes.
The provisioned Prometheus data source has UID `open-meteo-prometheus`.

## Dashboard attribution

`grafana/dashboards/tarantool.json` is the official **Tarantool Cartridge dashboard**,
Grafana dashboard 13054, revision 22:

- Source: https://grafana.com/api/dashboards/13054/revisions/22/download
- Project: https://github.com/tarantool/grafana-dashboard
- Copyright (c) 2020–2021 Tarantool, MIT license; see
  `grafana/dashboards/TARANTOOL-LICENSE`.

Local adaptations set a stable dashboard UID, clear the database ID, disable direct
editing, and select the provisioned Prometheus data source in the datasource
variable. Queries and panels are retained from upstream. Grafana automatically
migrates legacy graph panels when opening the dashboard.

`grafana/dashboards/forecast.json` is the application dashboard. It selects only the
`open-meteo` scrape job, uses Grafana's rate interval, and aggregates histogram
buckets before calculating p95. The instance selector includes all five configured
targets; request metrics are emitted on routers.

## Configuration checks

```bash
docker compose -f compose.monitoring.yml config --quiet
docker compose -f compose.monitoring.yml run --rm --no-deps \
  --entrypoint promtool prometheus check config /etc/prometheus/prometheus.yml
python3 monitoring/test_alerts.py
```

The alert tests use Python 3.9+ (standard library only) and Docker. They extract
the exact provisioned queries, thresholds, and pending durations and test them
with the pinned Prometheus image; there is no duplicate set of production rules.

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
