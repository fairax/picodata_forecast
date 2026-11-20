# Alert runbooks

Open [Grafana → Alerting → Alert rules](http://localhost:3000/alerting/list).
Rules are evaluated every 30 seconds. **Pending** means a condition is currently
true; **Alerting** means it has remained true for the pending duration below.
These are initial operational thresholds, not a measured service-level objective.

| Alert | Condition | Pending | Severity |
| --- | --- | --- | --- |
| Instance unavailable | Scrape target is down, or all availability data is missing | 2m | Critical |
| Forecast errors | More than 5% of completed responses are 5xx | 5m | Critical |
| Forecast latency | p95 completed-request latency exceeds 2 seconds | 5m | Warning |
| Fetch capacity exhausted | At least 5 concurrency rejections in the last 5 minutes | 2m | Warning |
| Cache failures | At least 5 failed operations in the last 5 minutes, by operation | 5m | Warning |
| Upstream instability | More than 20% of attempts encounter transport errors, timeouts, HTTP 429 or 5xx | 5m | Warning |
| Memtx quota pressure | Allocated memtx quota exceeds 90% | 10m | Warning |

Conditions are per instance. Percentage and latency rules require at least 20
requests/attempts in the last 5 minutes. Cancellations are excluded from completed
responses and latency; retries count as separate upstream attempts. Cache lookup
and insertion errors are separate alert instances. Short bursts and sparse traffic
can therefore remain below the alert threshold even when an individual request fails.

## Instance unavailable

1. Inspect [Prometheus targets](http://localhost:9090/targets) and the scrape error.
2. Check that the named instance is running and its configured `/metrics` port
   responds. Inspect startup logs if it exits or fails to become ready.
3. Check Docker-to-host networking and the target address. A failed scrape does
   not distinguish a stopped application from an inaccessible HTTP endpoint.
4. Restore the process or connectivity. Do not remove a failed target just to
   clear the alert. For planned downtime, create a time-limited silence.

The missing-data fallback detects an empty `open-meteo` job. The static target
list is the inventory: deleting a single target from it removes monitoring for
that instance. Keep the list synchronized with the deployment.

## Forecast errors

1. Open the linked status panel and identify 500, 502, 503 or 504 responses.
2. Use a client's `X-Request-ID` to correlate router and storage events.
3. Check upstream errors/timeouts, cache errors and concurrency rejections to
   distinguish dependency failure, application failure, and saturation.
4. Restore the failing component. Increasing retry counts during an outage can
   amplify load; examine the existing attempts before changing policy.

## Forecast latency

1. Compare p95 with upstream attempt durations, retries, cache outcomes and active
   fetches. A cold cache after restart or promotion can raise upstream traffic.
2. Check upstream response times, storage connectivity, and host CPU/scheduler
   pressure. The existing budget is not a strict end-to-end deadline.
3. Resolve the bottleneck before raising timeouts or concurrency. Histogram p95
   is an estimate based on configured bucket boundaries.

## Fetch capacity

1. Check active fetches against `OPEN_METEO_MAX_CONCURRENT_FETCHES` and inspect
   slow attempts, retries and cache misses.
2. Restore upstream health or reduce avoidable incoming traffic. Disconnected
   clients can leave fetches running until those fetches finish.
3. Increase the limit only after measuring CPU and memory headroom. Each upstream
   response is currently buffered without a size limit.

## Cache failures

1. Identify `operation=get` or `operation=put` in the alert and logs.
2. Check storage masters, vshard routing, moving buckets and inter-instance
   connectivity. Inspect memory/insertion errors on storage instances.
3. Restore the storage path and verify cache errors subside. Best-effort caching
   can keep HTTP requests successful while increasing upstream traffic.

## Upstream instability

1. Inspect attempt outcomes and HTTP statuses. Separate transport/timeouts from
   throttling (429) and upstream 5xx responses.
2. Check DNS, TLS diagnostics, connectivity, upstream health and `Retry-After`.
3. Reduce avoidable upstream load and restore connectivity as appropriate. A
   successful final response can hide repeated failed attempts; check retry rates
   before increasing retries or request budgets.

## Memtx pressure

1. Inspect the linked Tarantool slab panel, live data/index usage, cache insertion
   errors, and cleanup/eviction behavior on the affected instance.
2. This ratio measures allocated quota, not live cache size or total process RAM.
   Slabs can remain reserved after eviction; the alert does not prove imminent OOM.
3. Check fragmentation and host memory headroom before changing `memtx_memory`.
   Do not automatically restart instances or raise limits to clear the alert.

## Evaluation failures and missing metrics

Traffic metrics may not exist until an event first occurs. Missing traffic or
memtx series are treated as normal by these rules; target availability is checked
separately. This does not detect the disappearance of an individual metric while
the scrape endpoint stays healthy.

Query and evaluation failures use Grafana's **Error** handling and remain visible
as `DatasourceError` alerts. Check Prometheus health, the Grafana data-source
connection, rule query errors, and networking before silencing them. Availability
No Data is configured to alert. Grafana itself must be running to evaluate rules;
this stack cannot notify you about its own complete outage.

## Silences and future delivery

Use Grafana's silences for planned work, matching `service=open-meteo` plus the
specific `alertname`/`instance`, with an expiry and reason. Avoid muting the whole
service for an isolated failure.

This change configures no external notification destination and does not replace
existing contact points or notification policies. On a fresh stack, external
delivery is not operational; use the alerting UI. If you have already configured
policies, these new rules inherit them. To enable delivery later, configure a
contact point and route `service=open-meteo` to it. Configure SMTP first for email;
keep credentials outside tracked provisioning files.

Rules live in `grafana/provisioning/alerting/alerts.json`; edit the file and restart
Grafana to change thresholds. Do not edit the provisioned rules in the UI.
Host disk, OS memory and replication-specific alerts are outside this initial
set; they require additional metrics or topology-specific thresholds.
