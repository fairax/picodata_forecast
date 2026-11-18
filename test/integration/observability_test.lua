local t = require('luatest')
local fiber = require('fiber')
local g = t.group('integration_observability')

local fixture = require('test.helpers.forecast')
fixture.bind(g, {name = 'observability', port = 7800, transport = false})

local observations = require('test.helpers.observability')
local configure = fixture.configure
local request_with_id = observations.request_with_id
local capture_events = observations.capture_events
local metric = observations.metric
local raw_http_response = observations.raw_http_response

g.test_observability_request_ids_are_current_and_never_cached = function(cg)
    cg.auto_respond = true
    cg.actions = {{response_request_id = 'upstream-response-id'}}
    local first = request_with_id(cg, 'trace=cache', 'client.first:1')
    t.assert_equals(first.headers['x-request-id'], 'client.first:1')
    t.assert_equals(cg.upstream_request_ids, {false})
    local hit = request_with_id(cg, 'trace=cache', 'client.second:2')
    t.assert_equals(hit.headers['x-request-id'], 'client.second:2')
    t.assert_equals(hit.headers['x-cache'], 'HIT')
    t.assert_equals(cg.requests, 1)
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        t.assert_equals(server.net_box:eval([[
            for _, tuple in box.space.forecast_cache:pairs() do
                for name in pairs(tuple[5]) do
                    if name:lower() == 'x-request-id' then return false end
                end
            end
            return true
        ]]), true)
    end
end

g.test_observability_correlates_stages_and_counts_retries = function(cg)
    capture_events(cg)
    cg.auto_respond = true
    cg.actions = {{status = 503}, {status = 200}}
    local attempts = metric(cg, 'forecast_upstream_attempts_total')
    local retries = metric(cg, 'forecast_upstream_retries_total')
    local misses = metric(cg, 'forecast_cache_operations_total', {operation = 'get', outcome = 'miss'})
    local http_count = metric(cg, 'http_server_request_latency_count', {path = '/forecast', method = 'GET'})
    local response = request_with_id(cg, 'trace=retry', 'shared-trace')
    t.assert_equals(response.status, 200)
    t.assert_equals(cg.upstream_request_ids, {false, false})
    t.assert_equals(metric(cg, 'forecast_upstream_attempts_total') - attempts, 2)
    t.assert_equals(metric(cg, 'forecast_upstream_retries_total') - retries, 1)
    t.assert_equals(metric(cg, 'forecast_cache_operations_total',
        {operation = 'get', outcome = 'miss'}) - misses, 1)
    t.assert_equals(metric(cg, 'http_server_request_latency_count',
        {path = '/forecast', method = 'GET'}) - http_count, 1)
    t.assert_equals(metric(cg, 'forecast_active_fetches'), 0)
    local seen, summaries, storage_events = {}, 0, 0
    for _, server in ipairs(cg.cluster.servers) do
        local events = server.net_box:eval('return observability_events')
        for _, event in ipairs(events) do
            t.assert_equals(event.request_id, 'shared-trace')
            t.assert_equals(event.instance_uuid, server.instance_uuid)
            t.assert(event.duration_seconds == nil or event.duration_seconds >= 0)
            if event.event == 'request_completed' then
                summaries = summaries + 1
                t.assert_equals(event.level, 'info')
                t.assert_equals(event.status, 200)
            elseif event.event == 'upstream_attempt' or event.event == 'cache_get' then
                t.assert_equals(event.level, event.status and event.status >= 400 and 'warn' or 'debug')
            end
            if event.event:match('^storage_cache') then storage_events = storage_events + 1 end
            seen[event.event] = true
        end
    end
    t.assert_equals(summaries, 1)
    t.assert(storage_events >= 2)
    t.assert(seen.cache_get and seen.cache_put and seen.upstream_attempt and seen.upstream_retry)
    t.assert(metric(cg, 'forecast_upstream_attempt_duration_seconds_sum') > 0)
    t.assert_str_contains(cg.router:http_request('get', '/metrics').body,
        'forecast_upstream_attempts_total')
end

g.test_observability_concurrent_requests_keep_distinct_context = function(cg)
    capture_events(cg)
    local router = cg.router
    local futures = {}
    for i = 1, 2 do
        futures[i] = router.net_box:eval([[
            local i, port = ...
            return require('http.client').get('http://127.0.0.1:' .. port .. '/forecast?trace=parallel-' .. i,
                {headers = {['X-Request-ID'] = 'parallel-' .. i}, timeout = 10, proxy = ''})
        ]], {i, router.http_port}, {is_async = true, timeout = 35})
    end
    for _ = 1, 2 do t.assert_equals(cg.entered:get(5), true) end
    t.assert_equals(metric(cg, 'forecast_active_fetches'), 2)
    for _ = 1, 2 do cg.gate:put(true) end
    for _, future in ipairs(futures) do t.assert_equals(assert(future:wait_result(5))[1].status, 200) end
    t.assert_equals(cg.upstream_request_ids, {false, false})
    t.assert_equals(metric(cg, 'forecast_active_fetches'), 0)
    -- Check each request's actual events while its own upstream attempt runs.
    for _, server in ipairs(cg.cluster.servers) do
        local events = server.net_box:eval('return observability_events')
        for _, event in ipairs(events) do
            t.assert(event.request_id == 'parallel-1' or event.request_id == 'parallel-2')
        end
    end
    local events = router.net_box:eval('return observability_events')
    for i = 1, 2 do
        local attempts, reads, writes, completions = 0, 0, 0, 0
        for _, event in ipairs(events) do
            if event.request_id == 'parallel-' .. i then
                if event.event == 'upstream_attempt' then attempts = attempts + 1 end
                if event.event == 'cache_get' then reads = reads + 1 end
                if event.event == 'cache_put' then writes = writes + 1 end
                if event.event == 'request_completed' then completions = completions + 1 end
            end
        end
        t.assert_equals({attempts, reads, writes, completions}, {1, 1, 1, 1})
    end
end

g.test_observability_internal_errors_are_generic_with_request_id = function(cg)
    local router = cg.router
    router.net_box:eval([[
        local vshard = require('vshard')
        rawset(_G, 'observability_saved_bucket', vshard.router.bucket_id_strcrc32)
        vshard.router.bucket_id_strcrc32 = function() error('private application detail') end
    ]])
    local ok, response = pcall(request_with_id, cg, 'trace=internal', 'internal-trace')
    router.net_box:eval([[
        require('vshard').router.bucket_id_strcrc32 = observability_saved_bucket
        rawset(_G, 'observability_saved_bucket', nil)
    ]])
    t.assert(ok, tostring(response))
    t.assert_equals(response.status, 500)
    t.assert_equals(response.body, 'Internal server error')
    t.assert_equals(response.headers['x-request-id'], 'internal-trace')
end

g.test_observability_instrumentation_failures_do_not_replace_result = function(cg)
    cg.auto_respond = true
    local router = cg.router
    router.net_box:eval([[
        local metrics = require('metrics')
        rawset(_G, 'observability_saved_collectors', {})
        for _, collector in pairs(metrics.collectors()) do
            if collector.name:match('^forecast_') or collector.name == 'http_server_request_latency' then
                local originals = {}
                observability_saved_collectors[collector] = originals
                for _, method in ipairs({'inc', 'set', 'observe'}) do
                    if collector[method] then
                        originals[method] = collector[method]
                        collector[method] = function() error('injected metrics failure') end
                    end
                end
            end
        end
    ]])
    local ok, response = pcall(request_with_id, cg, 'trace=broken-metrics', 'metrics-trace')
    router.net_box:eval([[
        for collector, methods in pairs(observability_saved_collectors) do
            for method, fn in pairs(methods) do collector[method] = fn end
        end
        rawset(_G, 'observability_saved_collectors', nil)
    ]])
    t.assert(ok, tostring(response))
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'trace=broken-metrics')
    t.assert_equals(response.headers['x-request-id'], 'metrics-trace')
end

g.test_observability_repeated_incoming_ids_generate_uuid = function(cg)
    cg.auto_respond = true
    local response = raw_http_response(cg.router, '/forecast?trace=duplicate',
        'X-Request-ID: first\r\nX-Request-ID: second\r\n')
    t.assert_equals(response.status, 200)
    local id = response.headers['x-request-id']
    t.assert_equals(#id, 36)
    t.assert(require('uuid').fromstr(id) ~= nil)
    t.assert_equals(cg.upstream_request_ids, {false})
end

g.test_observability_cancelled_parent_retains_permit_and_context = function(cg)
    configure(cg, cg.endpoint, '30', '1')
    local rejected = metric(cg, 'forecast_rejected_requests_total', {reason = 'concurrency_limit'})
    local router = cg.router
    router.net_box:eval([[
        local fiber = require('fiber')
        rawset(_G, 'observability_parent_done', false)
        rawset(_G, 'observability_parent', fiber.new(function()
            pcall(require('app.roles.custom').fetch, 'trace=cancelled', 'cancelled-trace')
            rawset(_G, 'observability_parent_done', true)
        end))
    ]])
    t.assert_equals(cg.entered:get(5), true)
    t.assert_equals(metric(cg, 'forecast_active_fetches'), 1)
    router.net_box:eval("require('fiber').create(function() observability_parent:cancel() end)")
    fiber.sleep(0.05)
    local busy = request_with_id(cg, 'trace=busy', 'busy-trace')
    t.assert_equals(busy.status, 503)
    t.assert_equals(busy.headers['x-request-id'], 'busy-trace')
    t.assert_equals(metric(cg, 'forecast_rejected_requests_total', {reason = 'concurrency_limit'}) - rejected, 1)
    t.assert_equals(metric(cg, 'forecast_active_fetches'), 1)
    t.assert_equals(cg.upstream_request_ids, {false})
    cg.gate:put(true)
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(metric(cg, 'forecast_active_fetches'), 0)
        t.assert_equals(router.net_box:eval('return observability_parent_done'), true)
    end)
    cg.auto_respond = true
    t.assert_equals(request_with_id(cg, 'trace=after-cancelled', 'after-cancelled').status, 200)
end

g.test_observability_storage_warning_keeps_cached_response = function(cg)
    cg.auto_respond = true
    request_with_id(cg, 'trace=touch', 'initial-trace')
    capture_events(cg)
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        server.net_box:eval([[
            rawset(_G, 'cache_failure_trigger', function() error('injected touch error') end)
            box.space.forecast_cache:before_replace(cache_failure_trigger)
        ]])
    end
    local response = request_with_id(cg, 'trace=touch', 'touch-trace')
    t.assert_equals(response.status, 200)
    t.assert_equals(response.headers['x-cache'], 'HIT')
    local warnings = 0
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        for _, event in ipairs(server.net_box:eval('return observability_events')) do
            if event.event == 'cache_touch_failure' then
                warnings = warnings + 1
                t.assert_equals(event.request_id, 'touch-trace')
                t.assert_equals(event.level, 'warn')
                t.assert_equals(event.error_kind, 'storage')
            end
        end
    end
    t.assert_equals(warnings, 1)
end

g.test_observability_metric_labels_stay_bounded_and_config_is_idempotent = function(cg)
    cg.auto_respond = true
    local router = cg.router
    local before = metric(cg, 'http_server_request_latency_count', {path = '/forecast', method = 'GET'})
    request_with_id(cg, 'trace=labels', 'unique-label-test')
    router.net_box:eval([[
        local role = require('app.roles.custom')
        for _ = 1, 3 do role.apply_config(nil, {is_master = true}) end
        for _, sample in ipairs(require('metrics').collect()) do
            if sample.metric_name:match('^forecast_') then
                for name, value in pairs(sample.label_pairs) do
                    assert(name ~= 'request_id' and name ~= 'query' and name ~= 'bucket_id')
                    assert(value ~= 'unique-label-test' and value ~= 'trace=labels')
                end
            end
        end
    ]])
    request_with_id(cg, 'trace=labels', 'other-label-test')
    t.assert_equals(metric(cg, 'http_server_request_latency_count',
        {path = '/forecast', method = 'GET'}) - before, 2)
end
