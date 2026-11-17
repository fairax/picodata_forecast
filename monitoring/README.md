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
```

No logs or request bodies are exported by this stack. It collects metrics only;
it does not provision a log or distributed-trace backend.
