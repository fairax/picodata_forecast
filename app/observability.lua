local clock = require('clock')
local log = require('log')
local metrics = require('metrics')
local uuid = require('uuid')

local M = {}
local buckets = {0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120}
local collectors = {}
local function register(kind, name, help)
    local ok, collector = pcall(metrics[kind], name, help, kind == 'histogram' and buckets or nil)
    if ok then collectors[name] = collector end
end
register('counter', 'forecast_cache_operations_total', 'Router cache operations')
register('counter', 'forecast_upstream_attempts_total', 'Upstream HTTP attempts')
register('histogram', 'forecast_upstream_attempt_duration_seconds', 'Upstream attempt duration')
register('counter', 'forecast_upstream_retries_total', 'Upstream retries started')
register('gauge', 'forecast_active_fetches', 'Active upstream fetches including cache insertion')
register('counter', 'forecast_rejected_requests_total', 'Rejected forecast requests')
register('histogram', 'http_server_request_latency', 'HTTP Server Request Latency')

local function update(name, method, value, labels)
    local collector = collectors[name]
    if collector then pcall(collector[method], collector, value, labels) end
end

function M.request_id(value)
    if type(value) == 'string' and #value >= 1 and #value <= 128
        and value:match('^[A-Za-z0-9._:-]+$') then return value end
    return uuid.str()
end

function M.event(level, event, request_id, fields)
    pcall(function()
        local record = table.copy(fields or {})
        record.event, record.request_id = event, request_id
        record.instance_uuid = box.info.uuid
        if record.detail then
            -- Transport diagnostics may include URLs. Never include their query strings.
            record.detail = tostring(record.detail):gsub('%?[^%s]+', '?<redacted>')
        end
        log[level](record)
    end)
end

function M.cache(request_id, operation, outcome, started, bucket_id, detail)
    update('forecast_cache_operations_total', 'inc', 1, {operation = operation, outcome = outcome})
    M.event(outcome == 'error' and 'warn' or 'debug', 'cache_' .. operation, request_id,
        {operation = operation, outcome = outcome, duration_seconds = clock.monotonic() - started,
            bucket_id = bucket_id, detail = detail, error_kind = outcome == 'error' and 'cache' or nil})
end

function M.active(value)
    update('forecast_active_fetches', 'set', value)
end

function M.reject(request_id)
    update('forecast_rejected_requests_total', 'inc', 1, {reason = 'concurrency_limit'})
    M.event('warn', 'request_rejected', request_id, {reason = 'concurrency_limit', status = 503})
end

function M.native_event(level, event, request_id, operation, outcome, attempt, status, duration, bucket_id, detail)
    local fields = {outcome = outcome, duration_seconds = duration}
    if operation ~= '' then fields.operation = operation end
    if attempt > 0 then fields.attempt = attempt end
    if status > 0 then fields.status = status end
    if bucket_id > 0 then fields.bucket_id = bucket_id end
    if detail ~= '' then fields.detail = detail end
    if outcome == 'timeout' or outcome == 'transport' or outcome == 'internal' then
        fields.error_kind = outcome
    elseif outcome == 'error' then fields.error_kind = 'storage'
    elseif status >= 400 then fields.error_kind = 'http' end
    if event == 'upstream_attempt' then
        update('forecast_upstream_attempts_total', 'inc', 1,
            {outcome = outcome, status = status > 0 and tostring(status) or 'none'})
        update('forecast_upstream_attempt_duration_seconds', 'observe', duration, {outcome = outcome})
    elseif event == 'upstream_retry' then
        update('forecast_upstream_retries_total', 'inc', 1, {reason = outcome})
    end
    M.event(level, event, request_id, fields)
end

-- metrics 1.0.0's http_middleware.v1 expects a response table even when the handler
-- raises: it replaces cancellation errors and skips their latency observation.
-- Use the collector's latency helper with our own labels so cancellation errors
-- survive and cancellations are counted without requiring a response table.
function M.wrap_http(handler)
    local collector = collectors.http_server_request_latency
    if not collector then return handler end
    return function(request)
        return collector:observe_latency(function(ok, result)
            local cancelled = not ok and type(result) == 'cdata' and result.type == 'FiberIsCancelled'
            local status = ok and (result.status or 200) or (cancelled and 'cancelled' or 500)
            return {path = request.endpoint.path, method = request.endpoint.method, status = status}
        end, handler, request)
    end
end

return M
