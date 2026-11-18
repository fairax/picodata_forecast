local t = require('luatest')
local fiber = require('fiber')
local g = t.group('integration_router')

local fixture = require('test.helpers.forecast')
fixture.bind(g, {name = 'router', port = 48080, transport = false})

local observations = require('test.helpers.observability')
local upstream = require('test.helpers.upstream')
local fetch = fixture.fetch
local finish = fixture.finish
local configure = fixture.configure
local set_endpoint = fixture.set_endpoint
local set_router_role = fixture.set_router_role
local fault_upstream = upstream.fault_upstream
local fault_state = upstream.fault_state
local raw_http_response = observations.raw_http_response
local function set_timeout(cg, value) configure(cg, cg.endpoint, value) end

local function location(cg, query)
    local result = cg.router.net_box:eval([[
        local router = require('vshard').router
        local bucket = router.bucket_id_strcrc32(...)
        return {bucket = bucket, uuid = assert(router.route(bucket)).uuid}
    ]], {query})
    local source, destination
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        if server.replicaset_uuid == result.uuid then source = server else destination = server end
    end
    return result.bucket, source, destination
end

g.test_fetch_yields_and_populates_cache = function(cg)
    local future = fetch(cg, 'b=2&a=1', true)
    t.assert_equals(cg.entered:get(5), true)
    -- The HTTP response remains blocked, but unrelated server fibers must run.
    t.assert_equals(cg.router.net_box:eval('return 42', {}, {timeout = 1}), 42)
    t.assert_equals(future:is_ready(), false)
    cg.gate:put(true)
    local response = finish(future)
    t.assert_equals(response.ok, true)
    t.assert_equals(response.response.status, 200)
    t.assert_equals(response.response.body, 'a=1&b=2')
    local ttl
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        ttl = server.net_box:eval([[
            local tuple = box.space.forecast_cache:get('a=1&b=2')
            if tuple then return tuple[6] - tuple[7] end
        ]]) or ttl
    end
    t.assert_equals(ttl, 60000000)
    local cached = fetch(cg, 'a=1&b=2')
    t.assert_equals(cached.ok, true)
    t.assert_equals(cached.response.body, response.response.body)
    t.assert_equals(cached.response.headers['x-cache'], {'HIT'})
    t.assert_equals(cg.requests, 1)
end

g.test_http_failure_is_not_cached = function(cg)
    cg.status = 503
    cg.actions = {{gated = false}, {gated = false}, {gated = false}}
    local future = fetch(cg, 'error=1', true)
    t.assert_equals(cg.entered:get(5), true)
    cg.gate:put(true)
    local failure = finish(future)
    t.assert_equals(failure.ok, true)
    t.assert_equals(failure.response.status, 503)
    t.assert_equals(failure.response.body, 'error=1')
    t.assert_equals(cg.requests, 3)
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    cg.status = 200
    local retry = fetch(cg, 'error=1', true)
    t.assert_equals(cg.entered:get(5), true)
    cg.gate:put(true)
    local success = finish(retry)
    t.assert_equals(success.ok, true)
    t.assert_equals(success.response.status, 200)
    t.assert_equals(cg.requests, 4)
end

g.test_http_route_fetches_and_caches = function(cg)
    local router = cg.router
    cg.gate:put(true)
    local response = router:http_request('get', '/forecast?b=2&a=1')
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'a=1&b=2')
    t.assert_equals(response.headers['content-type'], 'text/plain')
    local cached = router:http_request('get', '/forecast?a=1&b=2')
    t.assert_equals(cached.status, 200)
    t.assert_equals(cached.body, response.body)
    t.assert_equals(cached.headers['x-cache'], 'HIT')
    t.assert_equals(cg.requests, 1)
    local bucket_id = router.net_box:eval([[
        return require('vshard').router.bucket_id_strcrc32('a=1&b=2')
    ]])
    local copies = 0
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        local stored = server.net_box:eval([[
            local tuple = box.space.forecast_cache:get('a=1&b=2')
            if tuple == nil then return nil end
            return {id = tuple[2], owned = box.space._bucket:get(tuple[2]) ~= nil}
        ]])
        if stored ~= nil then
            copies = copies + 1
            t.assert_equals(stored, {id = bucket_id, owned = true})
        end
    end
    t.assert_equals(copies, 1)
end

g.test_http_route_preserves_upstream_errors = function(cg)
    local router = cg.router
    for _, status in ipairs({400, 429, 503}) do
        cg.status = status
        cg.retry_after = '120' -- Too long to retry within the total budget.
        cg.body = '{"error":"upstream detail"}'
        for _ = 1, 2 do
            cg.gate:put(true)
            local response = router:http_request('get', '/forecast?error=' .. status, {raise = false})
            t.assert_equals(response.status, status)
            t.assert_equals(response.body, cg.body)
            t.assert_equals(response.headers['content-type'], 'text/plain')
            t.assert_equals(response.headers['retry-after'], '120')
            t.assert_equals(response.headers['x-cache'], nil)
        end
    end
    t.assert_equals(cg.requests, 6)
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        t.assert_equals(server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    end
end

g.test_connection_failure_is_generic_502 = function(cg)
    local socket = require('socket')
    local listener = assert(socket.tcp_server('127.0.0.1', 0, function() end))
    local port = listener:name().port
    listener:close()
    set_endpoint(cg, 'http://127.0.0.1:' .. port .. '/private-upstream')
    local response = cg.router:http_request('get', '/forecast?connection=1', {raise = false})
    t.assert_equals(response.status, 502)
    t.assert_equals(response.body, 'Forecast service unavailable')
end

g.test_tls_failure_is_generic_502 = function(cg)
    local socket = require('socket')
    -- Accept TCP and close without a TLS handshake.
    cg.tls_listener = assert(socket.tcp_server('127.0.0.1', 0, function() end))
    local port = cg.tls_listener:name().port
    set_endpoint(cg, 'https://127.0.0.1:' .. port .. '/private-tls-upstream')
    local response = cg.router:http_request('get', '/forecast?tls=1', {raise = false})
    t.assert_equals(response.status, 502)
    t.assert_equals(response.body, 'Forecast service unavailable')
end

g.test_cache_decode_failure_fetches_upstream = function(cg)
    local router = cg.router
    cg.gate:put(true)
    t.assert_equals(router:http_request('get', '/forecast?corrupt=1').status, 200)
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        server.net_box:eval([[
            local space = box.space.forecast_cache
            if space:get('corrupt=1') then
                -- The schema permits a map, but Response requires lists of strings.
                space:update('corrupt=1', {{'=', 5, {['private-header'] = 123}}})
            end
        ]])
    end
    cg.gate:put(true)
    local response = router:http_request('get', '/forecast?corrupt=1', {raise = false})
    t.assert_equals(response.status, 200)
    t.assert_equals(cg.requests, 2)
    t.assert_equals(router:http_request('get', '/forecast?corrupt=1').headers['x-cache'], 'HIT')
end

g.test_oversized_cache_insertion_preserves_response = function(cg)
    -- Exceed memtx_max_tuple_size without changing production configuration.
    cg.body = string.rep('x', 1048576)
    cg.gate:put(true)
    local response = cg.router:http_request('get', '/forecast?oversized=1', {raise = false})
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, cg.body)
    t.assert_equals(cg.requests, 1)
    cg.body = 'small again'
    cg.gate:put(true)
    local retried = cg.router:http_request('get', '/forecast?oversized=1')
    t.assert_equals(retried.body, cg.body)
    t.assert_equals(retried.headers['x-cache'], nil)
    t.assert_equals(cg.requests, 2)
    t.assert_equals(cg.router:http_request('get', '/forecast?oversized=1').headers['x-cache'], 'HIT')
end

g.test_queries_distribute_across_both_shards = function(cg)
    local router = cg.router
    local queries = router.net_box:eval([[
        local router = require('vshard').router
        local queries = {}
        for i = 1, 1000 do
            local query = 'location=' .. i
            local bucket = router.bucket_id_strcrc32(query)
            local owner = assert(router.route(bucket))
            queries[owner.uuid] = query
        end
        return queries
    ]])
    local shards = 0
    for uuid, query in pairs(queries) do
        shards = shards + 1
        cg.gate:put(true)
        t.assert_equals(router:http_request('get', '/forecast?' .. query).status, 200)
        local found = 0
        for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
            local location = server.net_box:eval([[
                local query = ...
                local tuple = box.space.forecast_cache:get(query)
                if tuple == nil then return nil end
                return {uuid = box.info.cluster.uuid, owned = box.space._bucket:get(tuple[2]) ~= nil}
            ]], {query})
            if location ~= nil then
                found = found + 1
                t.assert_equals(location, {uuid = uuid, owned = true})
            end
        end
        t.assert_equals(found, 1)
    end
    t.assert_equals(shards, 2)
    t.assert_equals(cg.requests, 2)
end

g.test_cached_response_survives_bucket_move = function(cg)
    local router = cg.router
    local query = 'migration=1'
    cg.gate:put(true)
    t.assert_equals(router:http_request('get', '/forecast?' .. query).status, 200)
    local source, destination
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        if server.net_box:eval('return box.space.forecast_cache:get(...) ~= nil', {query}) then
            source = server
        else
            destination = server
        end
    end
    t.assert(source ~= nil and destination ~= nil)
    local uuid = destination.net_box:eval('return box.info.cluster.uuid')
    source.net_box:eval([[
        local query, destination = ...
        local bucket = box.space.forecast_cache:get(query)[2]
        local ok, err = require('vshard').storage.bucket_send(bucket, destination)
        assert(ok, tostring(err))
    ]], {query, uuid})
    local cached = router:http_request('get', '/forecast?' .. query)
    t.assert_equals(cached.status, 200)
    t.assert_equals(cached.headers['x-cache'], 'HIT')
    t.assert_equals(cg.requests, 1)
end

g.test_cleanup_runs_safely_during_bucket_transfer = function(cg)
    local query = 'cleanup=migration'
    local location = cg.router.net_box:eval([[
        local router = require('vshard').router
        local bucket = router.bucket_id_strcrc32(...)
        return {bucket = bucket, uuid = assert(router.route(bucket)).uuid}
    ]], {query})
    local source, destination
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        if server.replicaset_uuid == location.uuid then source = server else destination = server end
    end
    source.net_box:eval([[
        local query, bucket, destination = ...
        local space = box.space.forecast_cache
        local binary = require('test.fixtures.binary')
        local headers = {['content-type'] = {'application/octet-stream'}}
        space:replace({query, bucket, 200, binary('fresh' .. string.char(0, 255, 128)),
            headers, 9007199254740991, 0})
        space:replace({'expired-migration', bucket, 200, binary('old'), headers, 0, 0})
        local ok, err = require('vshard').storage.bucket_send(bucket, destination)
        assert(ok, tostring(err))
    ]], {query, location.bucket, destination.replicaset_uuid})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(destination.net_box:eval([[
            return box.space.forecast_cache:get('expired-migration') == nil
        ]]), true)
    end)
    local response = cg.router:http_request('get', '/forecast?' .. query)
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'fresh' .. string.char(0, 255, 128))
    t.assert_equals(response.headers['content-type'], 'application/octet-stream')
    t.assert_equals(response.headers['x-cache'], 'HIT')
    t.assert_equals(cg.requests, 0)
end

g.test_transient_responses_retry_then_cache_success = function(cg)
    cg.auto_respond = true
    cg.actions = {{status = 503, body = 'first'}, {status = 502, body = 'second'}, {status = 200, body = 'final'}}
    local response = fetch(cg, 'retry=success')
    t.assert_equals(response.ok, true)
    t.assert_equals(response.response.status, 200)
    t.assert_equals(response.response.body, 'final')
    t.assert_equals(cg.requests, 3)
    local cached = fetch(cg, 'retry=success')
    t.assert_equals(cached.response.body, 'final')
    t.assert_equals(cached.response.headers['x-cache'], {'HIT'})
    t.assert_equals(cg.requests, 3)
end

g.test_truncated_body_retries_and_caches_only_complete_response = function(cg)
    fault_upstream(cg, 'truncated')
    local router = cg.router
    local response = router:http_request('get', '/forecast?truncated=recovery')
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'ok')
    t.assert_equals(response.headers['x-cache'], nil)
    t.assert_equals(fault_state(cg).count, 2)
    local cached = router:http_request('get', '/forecast?truncated=recovery')
    t.assert_equals(cached.body, 'ok')
    t.assert_equals(cached.headers['x-cache'], 'HIT')
    t.assert_equals(fault_state(cg).count, 2)
end

g.test_configured_timeout_bounds_a_slow_stage = function(cg)
    set_timeout(cg, '1')
    local started = fiber.clock()
    local response = cg.router:http_request('get', '/forecast?budget=short', {raise = false})
    t.assert_equals(response.status, 504)
    t.assert_equals(response.body, 'Forecast request timed out')
    t.assert_equals(cg.requests, 1)
    local elapsed = fiber.clock() - started
    t.assert(elapsed >= 0.9 and elapsed < 3)
end

g.test_large_response_bodies_are_forwarded = function(cg)
    cg.auto_respond = true
    local router = cg.router
    for _, size in ipairs({1048575, 1048576, 1048577, 2097152}) do
        cg.body = string.rep('x', size)
        local before = cg.requests
        local response = router:http_request('get', '/forecast?size=' .. size, {raise = false})
        t.assert_equals(response.status, 200)
        t.assert_equals(response.body, cg.body)
        t.assert_equals(response.headers['content-type'], 'text/plain')
        t.assert_equals(cg.requests, before + 1)
    end
end

g.test_large_errors_follow_retry_policy_and_smaller_responses_recover = function(cg)
    cg.auto_respond = true
    local router = cg.router
    for _, status in ipairs({400, 429, 502, 503, 504}) do
        cg.status = status
        cg.body = string.rep('x', 1048577)
        local before = cg.requests
        local response = router:http_request('get', '/forecast?overflow=' .. status, {raise = false})
        t.assert_equals(response.status, status)
        t.assert_equals(response.body, cg.body)
        t.assert_equals(response.headers['content-type'], 'text/plain')
        t.assert_equals(response.headers['retry-after'], '0')
        t.assert_equals(cg.requests, before + (status == 400 and 1 or 3))
    end
    cg.status, cg.body = 200, 'recovered'
    local response = router:http_request('get', '/forecast?overflow=503')
    t.assert_equals(response.body, 'recovered')
    t.assert_equals(response.headers['x-cache'], nil)
    t.assert_equals(router:http_request('get', '/forecast?overflow=503').headers['x-cache'], 'HIT')
end

g.test_response_body_preserves_original_bytes = function(cg)
    cg.auto_respond = true
    local router = cg.router
    local bytes = {}
    for i = 0, 255 do bytes[#bytes + 1] = string.char(i) end
    for i, body in ipairs({'', table.concat(bytes), string.rep(string.char(255), 1048577)}) do
        cg.body = body
        local query = '/forecast?binary=' .. i
        local before = cg.requests
        local response = router:http_request('get', query)
        t.assert_equals(response.status, 200)
        t.assert_equals(response.body or '', body)
        t.assert_equals(cg.requests, before + 1)
        -- Nonempty bodies below memtx_max_tuple_size must hit cache.
        if #body > 0 and #body < 1048576 then
            local cached = router:http_request('get', query)
            t.assert_equals(cached.body or '', body)
            t.assert_equals(cached.headers['x-cache'], 'HIT')
            t.assert_equals(cg.requests, before + 1)
        end
    end
end

g.test_empty_responses_are_forwarded_without_caching = function(cg)
    cg.auto_respond = true
    local router = cg.router
    for _, status in ipairs({200, 204}) do
        cg.status, cg.body = status, ''
        local query = '/forecast?empty=' .. status
        local before = cg.requests
        for i = 1, 2 do
            local response = router:http_request('get', query)
            t.assert_equals(response.status, status)
            t.assert_equals(response.body or '', '')
            t.assert_equals(response.headers['x-cache'], nil)
            t.assert_equals(cg.requests, before + i)
        end
        for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
            t.assert_equals(server.net_box:eval('return box.space.forecast_cache:get(...)',
                {'empty=' .. status}), nil)
        end
        cg.status, cg.body = 200, 'recovered'
        local response = router:http_request('get', query)
        t.assert_equals(response.body, 'recovered')
        t.assert_equals(response.headers['x-cache'], nil)
        local cached = router:http_request('get', query)
        t.assert_equals(cached.body, 'recovered')
        t.assert_equals(cached.headers['x-cache'], 'HIT')
        t.assert_equals(cg.requests, before + 3)
    end
end

g.test_compressed_body_is_forwarded_and_cached_without_conversion = function(cg)
    cg.auto_respond = true
    -- A complete gzip member representing an empty body.
    local body = string.char(31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    cg.actions = {{body = body, content_type = 'application/octet-stream', content_encoding = 'gzip'}}
    local router = cg.router
    for i = 1, 2 do
        local response = router:http_request('get', '/forecast?compressed=1')
        t.assert_equals(response.status, 200)
        t.assert_equals(response.body, body)
        t.assert_equals(response.headers['content-encoding'], 'gzip')
        t.assert_equals(response.headers['content-type'], 'application/octet-stream')
        t.assert_equals(response.headers['x-cache'], i == 2 and 'HIT' or nil)
    end
    t.assert_equals(cg.requests, 1)
end

g.test_binary_upstream_errors_preserve_bytes_and_retry_policy = function(cg)
    cg.auto_respond = true
    local router = cg.router
    local body = string.char(0, 255, 128) .. 'upstream error'
    for _, status in ipairs({400, 429, 503}) do
        cg.status, cg.body = status, body
        local before = cg.requests
        local response = router:http_request('get', '/forecast?binary-error=' .. status, {raise = false})
        t.assert_equals(response.status, status)
        t.assert_equals(response.body, body)
        t.assert_equals(response.headers['retry-after'], '0')
        t.assert_equals(cg.requests, before + (status == 400 and 1 or 3))
    end
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        t.assert_equals(server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    end
end

g.test_large_chunked_body_and_fiber_responsiveness = function(cg)
    fault_upstream(cg, 'chunked-large')
    local future = fetch(cg, 'chunked=1', true)
    -- The upstream sends headers then delays its body. Other TX fibers must run.
    fiber.sleep(0.1)
    t.assert_equals(cg.router.net_box:eval('return 42'), 42)
    local response = finish(future)
    t.assert_equals(response.ok, true)
    t.assert_equals(response.response.status, 200)
    t.assert_equals(response.response.body, string.rep('x', 1048577))
    t.assert_equals(fault_state(cg).count, 1)
end

g.test_unavailable_storage_still_fetches_upstream = function(cg)
    cg.auto_respond = true
    local _, source = location(cg, 'unavailable=1')
    cg.stopped_storage = source
    source:stop()
    local started = fiber.clock()
    local response = cg.router:http_request('get', '/forecast?unavailable=1')
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'unavailable=1')
    t.assert_equals(cg.requests, 1)
    t.assert(fiber.clock() - started < 4)
end

g.test_insertion_errors_preserve_upstream_response = function(cg)
    cg.auto_respond = true
    for _, code in ipairs({'MEMORY_ISSUE', 'ILLEGAL_PARAMS'}) do
        for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
            server.net_box:eval([[
                local code = ...
                local old = rawget(_G, 'cache_failure_trigger')
                if old then box.space.forecast_cache:before_replace(nil, old) end
                local trigger = function() box.error(box.error[code], 'injected cache insertion failure') end
                rawset(_G, 'cache_failure_trigger', trigger)
                box.space.forecast_cache:before_replace(trigger)
            ]], {code})
        end
        local query = 'insertion=' .. code
        local before = cg.requests
        for _ = 1, 2 do
            local response = cg.router:http_request('get', '/forecast?' .. query)
            t.assert_equals(response.status, 200)
            t.assert_equals(response.body, query)
            t.assert_equals(response.headers['content-type'], 'text/plain')
            t.assert_equals(response.headers['x-cache'], nil)
        end
        t.assert_equals(cg.requests - before, 2)
    end
end

g.test_bucket_can_move_while_upstream_fetch_is_pending = function(cg)
    local query = 'migration=pending'
    local bucket, source, destination = location(cg, query)
    local future = fetch(cg, query, true)
    t.assert_equals(cg.entered:get(5), true)
    -- The cache lookup must release its bucket reference before fetching.
    source.net_box:eval([[
        local bucket, destination = ...
        local ok, err = require('vshard').storage.bucket_send(bucket, destination)
        assert(ok, tostring(err))
    ]], {bucket, destination.replicaset_uuid}, {timeout = 5})
    cg.gate:put(true)
    t.assert_equals(finish(future).response.status, 200)
    local stored = destination.net_box:eval([[
        local tuple = box.space.forecast_cache:get(...)
        return tuple and {bucket = tuple[2], body = tuple[4]}
    ]], {query})
    t.assert_equals(stored, {bucket = bucket, body = query})
    t.assert_equals(fetch(cg, query).response.headers['x-cache'], {'HIT'})
    t.assert_equals(cg.requests, 1)
end

g.test_expired_response_is_refetched_by_router = function(cg)
    cg.auto_respond = true
    local query = 'expired=router'
    local bucket, source = location(cg, query)
    source.net_box:eval([[
        require('app.roles.storage').stop()
        local query, bucket = ...
        local binary = require('test.fixtures.binary')
        box.space.forecast_cache:replace({query, bucket, 200, binary('stale'), {type = {'text'}}, 0, 0})
    ]], {query, bucket})
    local response = fetch(cg, query).response
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, query)
    t.assert_equals(response.headers['x-cache'], nil)
    t.assert_equals(fetch(cg, query).response.headers['x-cache'], {'HIT'})
    t.assert_equals(cg.requests, 1)
end

g.test_native_fetch_on_router_does_not_access_cache = function(cg)
    cg.auto_respond = true
    local response = cg.router.net_box:eval([[
        assert(box.space.forecast_cache == nil)
        return box.lib.load('forecast'):load('fetch_forecast')('native=router', 'native-test')
    ]])
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'native=router')
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        t.assert_equals(server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    end
end

g.test_fetch_limit_rejects_misses_but_serves_hits = function(cg)
    configure(cg, cg.endpoint, '30', '2')
    cg.auto_respond = true
    t.assert_equals(fetch(cg, 'limit=cached').response.status, 200)
    cg.auto_respond = false
    cg.entered:get(1)
    local first = fetch(cg, 'limit=first', true)
    t.assert_equals(cg.entered:get(5), true)
    local second = fetch(cg, 'limit=second', true)
    t.assert_equals(cg.entered:get(5), true)
    cg.router.net_box:eval("os.setenv('OPEN_METEO_MAX_CONCURRENT_FETCHES', '10')")
    local before = cg.requests
    local response = cg.router:http_request('get', '/forecast?limit=third', {raise = false})
    t.assert_equals(response.status, 503)
    t.assert_equals(response.body, 'Forecast service busy')
    t.assert_equals(fetch(cg, 'limit=cached').response.headers['x-cache'], {'HIT'})
    t.assert_equals(cg.requests, before)
    cg.gate:put(true)
    t.assert_equals(finish(first).response.status, 200)
    cg.auto_respond = true
    t.assert_equals(fetch(cg, 'limit=released').response.status, 200)
    cg.gate:put(true)
    t.assert_equals(finish(second).response.status, 200)
end

g.test_fetch_limit_releases_after_timeout_and_errors = function(cg)
    configure(cg, cg.endpoint, '1', '1')
    t.assert_equals(fetch(cg, 'limit=timeout').response.status, 504)
    cg.auto_respond = true
    cg.status = 400
    t.assert_equals(fetch(cg, 'limit=error').response.status, 400)
    cg.status = 200
    t.assert_equals(fetch(cg, 'limit=after-error').response.status, 200)
end

g.test_fetch_limit_holds_permit_across_retries = function(cg)
    configure(cg, cg.endpoint, '30', '1')
    cg.actions = {{status = 503}, {status = 200}}
    local pending = fetch(cg, 'limit=retry', true)
    t.assert_equals(cg.entered:get(5), true)
    t.assert_equals(fetch(cg, 'limit=before-retry').response.status, 503)
    t.assert_equals(cg.requests, 1)
    cg.gate:put(true)
    t.assert_equals(cg.entered:get(5), true)
    t.assert_equals(fetch(cg, 'limit=during-retry').response.status, 503)
    t.assert_equals(cg.requests, 2)
    cg.gate:put(true)
    t.assert_equals(finish(pending).response.status, 200)
    cg.auto_respond = true
    t.assert_equals(fetch(cg, 'limit=after-retry').response.status, 200)
end

g.test_repeated_retry_after_preserves_last_value_policy = function(cg)
    cg.header_listener = assert(require('socket').tcp_server('127.0.0.1', 0, function(connection)
        if not connection:read({delimiter = '\r\n\r\n'}, 5) then return end
        cg.requests = cg.requests + 1
        connection:write('HTTP/1.1 503 Service Unavailable\r\nContent-Length: 5\r\n' ..
            'Retry-After: 120\r\nRetry-After: 0\r\nConnection: close\r\n\r\nerror', 5)
    end))
    configure(cg, 'http://127.0.0.1:' .. cg.header_listener:name().port .. '/forecast')
    local response = fetch(cg, 'headers=retry-after').response
    t.assert_equals(response.status, 503)
    t.assert_equals(response.body, 'error')
    t.assert_equals(response.headers['retry-after'], {'120', '0'})
    t.assert_equals(cg.requests, 3)
end

g.test_connection_headers_removed_from_upstream_and_cache_hits = function(cg)
    -- Use a raw server: http.server replaces Connection with its own value.
    cg.header_listener = assert(require('socket').tcp_server('127.0.0.1', 0, function(connection)
        if not connection:read({delimiter = '\r\n\r\n'}, 5) then return end
        cg.requests = cg.requests + 1
        local body = 'headers=upstream'
        connection:write(table.concat({
            'HTTP/1.1 200 OK',
            'Content-Length: ' .. #body,
            'Content-Type: text/plain',
            'Retry-After: 0',
            'Connection: X-Private-Hop',
            'Connection: close,  x-SECOND-Hop  ',
            'Keep-Alive: timeout=5',
            'Proxy-Connection: keep-alive',
            'TE: trailers',
            'Trailer: x-trailer',
            'Upgrade: example',
            'X-Private-Hop: private',
            'X-Second-Hop: also private',
            'X-End-To-End: preserved',
            'Cache-Control: no-store',
            'Cache-Control: max-age=60',
            'Set-Cookie: first=1; Expires=Wed, 21 Oct 2037 07:28:00 GMT',
            'Set-Cookie: second=2',
            '', body,
        }, '\r\n'))
    end))
    configure(cg, 'http://127.0.0.1:' .. cg.header_listener:name().port .. '/forecast')
    local router = cg.router
    for attempt = 1, 2 do
        local response = raw_http_response(router, '/forecast?headers=upstream')
        t.assert_equals(response.status, 200)
        t.assert_equals(response.body, 'headers=upstream')
        for _, name in ipairs({'keep-alive', 'proxy-connection', 'te', 'trailer', 'upgrade',
            'x-private-hop', 'x-second-hop'}) do
            t.assert_equals(response.headers[name], nil)
        end
        t.assert_equals(response.headers['content-type'], 'text/plain')
        t.assert_equals(response.headers['retry-after'], '0')
        t.assert_equals(response.headers['x-end-to-end'], 'preserved')
        t.assert_equals(response.headers['cache-control'], {'no-store', 'max-age=60'})
        t.assert_equals(response.headers['set-cookie'],
            {'first=1; Expires=Wed, 21 Oct 2037 07:28:00 GMT', 'second=2'})
        if attempt == 2 then t.assert_equals(response.headers['x-cache'], 'HIT') end
    end
    t.assert_equals(cg.requests, 1)
end

g.test_cached_headers_filtered_case_insensitively = function(cg)
    local query = 'headers=mixed'
    local bucket, source = location(cg, query)
    source.net_box:eval([[
        local query, bucket = ...
        box.lib.load('forecast'):load('cache_put')(query, bucket, {
            status = 200, body = 'cached', headers = {
                CoNnEcTiOn = {' X-First, ', ', x-SECOND  '}, connection = {'x-third'},
                ['X-FIRST'] = {'private'}, ['x-Second'] = {'private'},
                ['X-Third'] = {'private'},
                ['KeEp-AlIvE'] = {'timeout=5'}, ['PrOxY-CoNnEcTiOn'] = {'keep-alive'},
                Te = {'trailers'}, TrAiLeR = {'x-trailer'}, UpGrAdE = {'example'},
                ['TrAnSfEr-EnCoDiNg'] = {'chunked'}, ['CoNtEnT-LeNgTh'] = {'999'},
                ['Content-Type'] = {'text/plain'}, ['Retry-After'] = {'7'},
                ['X-Safe-Values'] = {'first', 'bad\r\ninjection', 'second'},
                ['X-Only-Unsafe'] = {'bad\nvalue'},
            },
        }, 'cache-test')
    ]], {query, bucket})
    local response = cg.router:http_request('get', '/forecast?' .. query)
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'cached')
    for _, name in ipairs({'x-first', 'x-second', 'x-third', 'keep-alive', 'proxy-connection',
        'te', 'trailer', 'upgrade', 'x-only-unsafe'}) do
        t.assert_equals(response.headers[name], nil)
    end
    t.assert(response.headers['content-length'] ~= '999')
    t.assert_equals(response.headers['content-type'], 'text/plain')
    t.assert_equals(response.headers['retry-after'], '7')
    t.assert_equals(response.headers['x-cache'], 'HIT')
    local raw = raw_http_response(cg.router, '/forecast?' .. query)
    t.assert_equals(raw.headers['x-safe-values'], {'first', 'second'})
    t.assert_equals(raw.headers['x-only-unsafe'], nil)
    t.assert_equals(cg.requests, 0)
end

g.test_router_role_disable_and_reenable = function(cg)
    configure(cg, cg.endpoint, '30', '1')
    local router = cg.router
    cg.auto_respond = true
    t.assert_equals(fetch(cg, 'lifecycle=cached').response.status, 200)
    cg.auto_respond = false
    cg.entered:get(1)
    local pending = fetch(cg, 'lifecycle=pending', true)
    t.assert_equals(cg.entered:get(5), true)
    local before = cg.requests
    cg.router_role_disabled = true
    set_router_role(cg, false)
    for _, query in ipairs({'lifecycle=cached', 'lifecycle=disabled'}) do
        local response = router:http_request('get', '/forecast?' .. query, {raise = false})
        t.assert_equals(response.status, 404)
    end
    t.assert_equals(cg.requests, before)
    router.net_box:eval([[
        local httpd = require('cartridge').service_get('httpd')
        assert(httpd.iroutes['app.roles.custom.forecast'] == nil)
        for i, route in ipairs(httpd.routes) do
            assert(route.path ~= '/forecast')
            if route.name then assert(httpd.iroutes[route.name] == i) end
        end
        require('app.roles.custom').stop() -- Stopping twice must be harmless.
    ]])
    set_router_role(cg, true)
    cg.router_role_disabled = nil
    -- Re-enabling must retain permits held by requests that are still running.
    t.assert_equals(fetch(cg, 'lifecycle=busy').response.body, 'Forecast service busy')
    t.assert_equals(fetch(cg, 'lifecycle=cached').response.headers['x-cache'], {'HIT'})
    cg.gate:put(true)
    t.assert_equals(finish(pending).response.status, 200)
    cg.auto_respond = true
    t.assert_equals(router:http_request('get', '/forecast?lifecycle=enabled').status, 200)
    t.assert_equals(router.net_box:eval([[
        local count = 0
        for _, route in pairs(require('cartridge').service_get('httpd').routes) do
            if route.path == '/forecast' then count = count + 1 end
        end
        return count
    ]]), 1)
end

g.test_fiber_http_fetches_run_concurrently = function(cg)
    local futures = {}
    for i = 1, 8 do futures[i] = fetch(cg, 'fibreq-parallel=' .. i, true) end
    for _ = 1, 8 do t.assert_equals(cg.entered:get(3), true) end
    for _, future in ipairs(futures) do t.assert_equals(future:is_ready(), false) end
    for _ = 1, 8 do cg.gate:put(true) end
    for _, future in ipairs(futures) do t.assert_equals(finish(future).response.status, 200) end
end

g.test_redirect_is_forwarded_without_following = function(cg)
    cg.auto_respond = true
    cg.actions = {{status = 302, location = '/forecast?redirected=1', body = 'redirect'}}
    local response = cg.router:http_request('get', '/forecast?redirect=start',
        {http = {follow_location = false}, raise = false})
    t.assert_equals(response.status, 302)
    t.assert_equals(response.body, 'redirect')
    t.assert_equals(response.headers.location, '/forecast?redirected=1')
    t.assert_equals(cg.requests, 1)
    for _, value in pairs(response.headers) do
        if type(value) == 'string' then t.assert_equals(value:find('[\r\n]'), nil) end
    end
end

g.test_redirect_statuses_are_not_followed_retried_or_cached = function(cg)
    cg.auto_respond = true
    for _, status in ipairs({301, 302, 303, 307, 308}) do
        local before = cg.requests
        local target = '/forecast?redirect=' .. status
        for i = 1, 2 do
            cg.actions[before + i] = {status = status, location = target, body = 'redirect body'}
            local response = fetch(cg, 'redirect=' .. status).response
            t.assert_equals(response.status, status)
            t.assert_equals(response.body, 'redirect body')
            t.assert_equals(response.headers.location, {target})
            t.assert_equals(response.headers['x-cache'], nil)
            t.assert_equals(cg.requests, before + i)
        end
    end
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        t.assert_equals(server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    end
end
