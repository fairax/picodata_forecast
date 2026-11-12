local t = require('luatest')
local fiber = require('fiber')
local fio = require('fio')
local http = require('http.server')
local cartridge_helpers = require('cartridge.test-helpers')
local helper = require('test.helper')
local g = t.group('integration_fetch_with_cache')

local function fetch(cg, query, async)
    return cg.cluster.main_server.net_box:eval([[
        local query = ...
        local lib = box.lib.load('forecast')
        local fetch = lib:load('fetch_with_cache')
        local bucket = box.space._bucket.index.status:select({'active'}, {limit = 1})[1]
        local ok, response = pcall(fetch, query, assert(bucket).id)
        return {ok = ok, response = ok and response or tostring(response)}
    ]], {query}, {is_async = async or false, timeout = 35})
end

local function finish(future)
    local values, err = future:wait_result(35)
    assert(values, tostring(err))
    return values[1]
end

local function configure(cg, endpoint, timeout, max_bytes)
    timeout = timeout or '30'
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        if server.env.OPEN_METEO_FORECAST_URL ~= endpoint or server.env.OPEN_METEO_TIMEOUT_SECS ~= timeout
            or server.env.OPEN_METEO_MAX_RESPONSE_BYTES ~= max_bytes then
            server:stop()
            server.env.OPEN_METEO_FORECAST_URL = endpoint
            server.env.OPEN_METEO_TIMEOUT_SECS = timeout
            server.env.OPEN_METEO_MAX_RESPONSE_BYTES = max_bytes
            server:start()
            cg.cluster:wait_until_healthy(server)
        end
    end
end

local function probe_configuration(endpoint, timeout, replica, max_bytes)
    local directory = fio.tempdir()
    local output = fio.pathjoin(directory, 'result.json')
    local env = table.copy(os.environ())
    env.OPEN_METEO_FORECAST_URL = endpoint
    env.OPEN_METEO_TIMEOUT_SECS = timeout
    env.OPEN_METEO_MAX_RESPONSE_BYTES = max_bytes
    local process = require('luatest.process'):start('/usr/local/bin/tarantool', {
        fio.pathjoin(helper.root, 'test', 'fixtures', 'forecast_config_probe.lua'),
        helper.root, output, directory, replica and 'replica' or 'master',
    }, env)
    local ok, result = pcall(function()
        local value
        t.helpers.retrying({timeout = 5}, function()
            local file = assert(io.open(output))
            local content = file:read('*a')
            file:close()
            value = require('json').decode(content)
        end)
        return value
    end)
    process:kill(nil, {quiet = true})
    fio.rmtree(directory)
    assert(ok, tostring(result))
    return result
end

g.before_all(function(cg)
    cg.mock = http.new('127.0.0.1', 0, {log_requests = false})
    cg.mock:route({method = 'GET', path = '/forecast'}, function(req)
        local gate, entered = cg.gate, cg.entered
        cg.requests = cg.requests + 1
        local action = cg.actions[cg.requests] or {}
        local status = action.status or cg.status
        local body = action.body or cg.body or req.query
        local retry_after = action.retry_after
        if retry_after == nil then retry_after = cg.retry_after end
        if retry_after == false then retry_after = nil end
        table.insert(cg.request_times, fiber.clock())
        table.insert(cg.request_wall_times, fiber.time())
        entered:put(true)
        if not cg.auto_respond and action.gated ~= false then gate:get(40) end
        return {status = status, body = body, headers = {
            ['content-type'] = 'text/plain', ['retry-after'] = retry_after,
        }}
    end)
    cg.mock:start()
    local port = cg.mock.tcp_server:name().port
    cg.endpoint = 'http://127.0.0.1:' .. port .. '/forecast'
    cg.cluster = cartridge_helpers.Cluster:new({
        server_command = helper.server_command,
        datadir = fio.pathjoin(helper.datadir, 'fetch_with_cache'),
        base_http_port = 48080,
        base_advertise_port = 48300,
        use_vshard = false, -- Bootstrap after router membership has propagated.
        env = {OPEN_METEO_FORECAST_URL = cg.endpoint, OPEN_METEO_TIMEOUT_SECS = '30'},
        replicasets = {{
            alias = 'storage',
            roles = {'app.roles.storage', 'vshard-storage'},
            servers = {{alias = 'storage'}},
        }, {
            alias = 'router',
            roles = {'app.roles.custom', 'vshard-router'},
            servers = {{alias = 'router'}},
        }, {
            alias = 'storage2',
            roles = {'app.roles.storage', 'vshard-storage'},
            servers = {{alias = 'storage2'}},
        }},
    })
    cg.cluster:start()
    t.helpers.retrying({timeout = 5}, function()
        local candidates = cg.cluster.servers[2].net_box:eval([[
            return require('cartridge.rpc').get_candidates('app.roles.storage', {leader_only = true})
        ]])
        t.assert_equals(#candidates, 2)
        local routers = cg.cluster.main_server.net_box:eval([[
            return require('cartridge.rpc').get_candidates('vshard-router')
        ]])
        t.assert_equals(#routers, 1)
    end)
    cg.cluster:bootstrap_vshard()
end)

g.after_all(function(cg)
    helper.stop_cluster(cg.cluster)
    cg.mock:stop()
end)

g.before_each(function(cg)
    configure(cg, cg.endpoint)
    helper.truncate_space_on_cluster(cg.cluster, 'forecast_cache')
    cg.gate = fiber.channel(10)
    cg.entered = fiber.channel(100)
    cg.requests = 0
    cg.request_times = {}
    cg.request_wall_times = {}
    cg.auto_respond = false
    cg.actions = {}
    cg.retry_after = '0'
    cg.status = 200
    cg.body = nil
end)

g.after_each(function(cg)
    cg.gate:close()
    cg.entered:close()
    if cg.tls_listener ~= nil then
        cg.tls_listener:close()
        cg.tls_listener = nil
    end
    if cg.fault_process ~= nil then
        cg.fault_process:kill(nil, {quiet = true})
        cg.fault_process = nil
        fio.rmtree(cg.fault_directory)
    end
    configure(cg, cg.endpoint)
end)

g.test_fetch_yields_and_populates_cache = function(cg)
    local future = fetch(cg, 'b=2&a=1', true)
    t.assert_equals(cg.entered:get(5), true)
    -- The HTTP response remains blocked, but unrelated server fibers must run.
    t.assert_equals(cg.cluster.main_server.net_box:eval('return 42', {}, {timeout = 1}), 42)
    t.assert_equals(future:is_ready(), false)
    cg.gate:put(true)
    local response = finish(future)
    t.assert_equals(response.ok, true)
    t.assert_equals(response.response.status, 200)
    t.assert_equals(response.response.body, 'a=1&b=2')
    local ttl = cg.cluster.main_server.net_box:eval([[
        local tuple = box.space.forecast_cache:get('a=1&b=2')
        return tuple[6] - tuple[7]
    ]])
    t.assert_equals(ttl, 60000000)
    local cached = fetch(cg, 'a=1&b=2')
    t.assert_equals(cached.ok, true)
    t.assert_equals(cached.response.body, response.response.body)
    t.assert_equals(cached.response.headers['x-cache'], 'HIT')
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
    local router = cg.cluster.servers[2]
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
    local router = cg.cluster.servers[2]
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

local function set_endpoint(cg, endpoint)
    configure(cg, endpoint)
end

local function fault_state(cg)
    local file = assert(io.open(fio.pathjoin(cg.fault_directory, 'state.json')))
    local data = file:read('*a')
    file:close()
    return require('json').decode(data)
end

local function fault_upstream(cg, mode)
    cg.fault_directory = fio.tempdir()
    cg.fault_process = require('luatest.process'):start('/usr/local/bin/tarantool', {
        fio.pathjoin(helper.root, 'test', 'fixtures', 'fault_upstream.lua'), mode, cg.fault_directory,
    }, os.environ())
    local state
    t.helpers.retrying({timeout = 5}, function()
        state = fault_state(cg)
    end)
    local scheme = mode == 'certificate' and 'https' or 'http'
    set_endpoint(cg, scheme .. '://127.0.0.1:' .. state.port .. '/forecast')
end

g.test_connection_failure_is_generic_502 = function(cg)
    local socket = require('socket')
    local listener = assert(socket.tcp_server('127.0.0.1', 0, function() end))
    local port = listener:name().port
    listener:close()
    set_endpoint(cg, 'http://127.0.0.1:' .. port .. '/private-upstream')
    local response = cg.cluster.servers[2]:http_request('get', '/forecast?connection=1', {raise = false})
    t.assert_equals(response.status, 502)
    t.assert_equals(response.body, 'Forecast service unavailable')
end

g.test_tls_failure_is_generic_502 = function(cg)
    local socket = require('socket')
    -- Accept TCP and close without a TLS handshake.
    cg.tls_listener = assert(socket.tcp_server('127.0.0.1', 0, function() end))
    local port = cg.tls_listener:name().port
    set_endpoint(cg, 'https://127.0.0.1:' .. port .. '/private-tls-upstream')
    local response = cg.cluster.servers[2]:http_request('get', '/forecast?tls=1', {raise = false})
    t.assert_equals(response.status, 502)
    t.assert_equals(response.body, 'Forecast service unavailable')
end

g.test_timeout_is_generic_504 = function(cg)
    local router = cg.cluster.servers[2]
    -- The mock intentionally withholds its response beyond the real HTTP deadline.
    local started = fiber.clock()
    local response = router:http_request('get', '/forecast?timeout=1', {raise = false, timeout = 35})
    t.assert_equals(response.status, 504)
    t.assert_equals(response.body, 'Forecast request timed out')
    t.assert_equals(cg.requests, 3)
    local elapsed = fiber.clock() - started
    t.assert(elapsed >= 29 and elapsed < 33)
end

g.test_invalid_configuration_fails_role_startup = function(cg)
    for _, endpoint in ipairs({'invalid-private-endpoint', 'file:///tmp/forecast', ''}) do
        local result = probe_configuration(endpoint, '30')
        t.assert_equals(result.ok, false)
        t.assert_str_contains(result.error, 'Invalid forecast configuration')
    end
    t.assert_equals(cg.requests, 0)
end

g.test_cache_decode_failure_fetches_upstream = function(cg)
    local router = cg.cluster.servers[2]
    cg.gate:put(true)
    t.assert_equals(router:http_request('get', '/forecast?corrupt=1').status, 200)
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        server.net_box:eval([[
            local space = box.space.forecast_cache
            if space:get('corrupt=1') then
                -- The schema permits a map, but Response requires string values.
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
    local response = cg.cluster.servers[2]:http_request('get', '/forecast?oversized=1', {raise = false})
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, cg.body)
    t.assert_equals(cg.requests, 1)
    cg.body = 'small again'
    cg.gate:put(true)
    local retried = cg.cluster.servers[2]:http_request('get', '/forecast?oversized=1')
    t.assert_equals(retried.body, cg.body)
    t.assert_equals(retried.headers['x-cache'], nil)
    t.assert_equals(cg.requests, 2)
    t.assert_equals(cg.cluster.servers[2]:http_request('get', '/forecast?oversized=1').headers['x-cache'], 'HIT')
end

g.test_queries_distribute_across_both_shards = function(cg)
    local router = cg.cluster.servers[2]
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
    local router = cg.cluster.servers[2]
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
    local location = cg.cluster.servers[2].net_box:eval([[
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
        space:replace({query, bucket, 200, 'fresh', {['content-type'] = 'text/plain'}, 9007199254740991, 0})
        space:replace({'expired-migration', bucket, 200, 'old', {['content-type'] = 'text/plain'}, 0, 0})
        local ok, err = require('vshard').storage.bucket_send(bucket, destination)
        assert(ok, tostring(err))
    ]], {query, location.bucket, destination.replicaset_uuid})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(destination.net_box:eval([[
            return box.space.forecast_cache:get('expired-migration') == nil
        ]]), true)
    end)
    local response = cg.cluster.servers[2]:http_request('get', '/forecast?' .. query)
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'fresh')
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
    t.assert_equals(cached.response.headers['x-cache'], 'HIT')
    t.assert_equals(cg.requests, 3)
end

g.test_persistent_transient_statuses_stop_after_three_attempts = function(cg)
    cg.auto_respond = true
    for _, status in ipairs({429, 502, 503, 504}) do
        local before = cg.requests
        cg.status = status
        cg.body = 'final upstream error'
        local response = fetch(cg, 'persistent=' .. status)
        t.assert_equals(response.response.status, status)
        t.assert_equals(response.response.body, cg.body)
        t.assert_equals(response.response.headers['retry-after'], '0')
        t.assert_equals(cg.requests - before, 3)
    end
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 0)
end

g.test_permanent_http_errors_are_not_retried = function(cg)
    cg.auto_respond = true
    for _, status in ipairs({400, 401, 403, 404}) do
        local before = cg.requests
        cg.status = status
        local response = fetch(cg, 'permanent=' .. status)
        t.assert_equals(response.response.status, status)
        t.assert_equals(cg.requests - before, 1)
    end
end

g.test_retry_after_seconds_and_fiber_responsiveness = function(cg)
    cg.auto_respond = true
    cg.actions = {{status = 429, retry_after = '1'}, {status = 200}}
    local future = fetch(cg, 'retry=seconds', true)
    t.assert_equals(cg.entered:get(5), true)
    t.assert_equals(cg.cluster.main_server.net_box:eval('return 42', {}, {timeout = 0.5}), 42)
    t.assert_equals(future:is_ready(), false)
    t.assert_equals(finish(future).response.status, 200)
    t.assert_equals(cg.requests, 2)
    t.assert(cg.request_times[2] - cg.request_times[1] >= 1)
end

g.test_retry_after_http_date = function(cg)
    cg.auto_respond = true
    local deadline = os.time() + 2
    local date = os.date('!%a, %d %b %Y %H:%M:%S GMT', deadline)
    cg.actions = {{status = 503, retry_after = date}, {status = 200}}
    t.assert_equals(fetch(cg, 'retry=date').response.status, 200)
    t.assert_equals(cg.requests, 2)
    t.assert(cg.request_wall_times[2] >= deadline)
end

g.test_malformed_and_absent_retry_after_are_ignored = function(cg)
    cg.auto_respond = true
    cg.actions = {{status = 503, retry_after = 'not-a-date'}, {status = 503, retry_after = false}, {status = 200}}
    t.assert_equals(fetch(cg, 'retry=malformed').response.status, 200)
    t.assert_equals(cg.requests, 3)
    t.assert(cg.request_times[3] - cg.request_times[1] < 2)
end

g.test_connection_reset_is_retried = function(cg)
    fault_upstream(cg, 'reset')
    local response = fetch(cg, 'transport=reset')
    t.assert_equals(response.response.status, 200)
    t.assert_equals(response.response.body, 'ok')
    t.assert_equals(fault_state(cg).count, 2)
end

g.test_persistent_connection_reset_stops_after_three_attempts = function(cg)
    fault_upstream(cg, 'reset-always')
    local response = fetch(cg, 'transport=persistent')
    t.assert_equals(response.response.status, 502)
    t.assert_equals(fault_state(cg).count, 3)
end

g.test_certificate_validation_failure_is_not_retried = function(cg)
    fault_upstream(cg, 'certificate')
    local response = fetch(cg, 'tls=untrusted-certificate')
    t.assert_equals(response.response.status, 502)
    t.assert_equals(fault_state(cg).count, 1)
end

g.test_retry_after_beyond_budget_returns_upstream_response = function(cg)
    cg.auto_respond = true
    cg.status = 503
    for _, delay in ipairs({
        '120',
        '99999999999999999999999999999999',
        os.date('!%a, %d %b %Y %H:%M:%S GMT', os.time() + 3600),
    }) do
        cg.retry_after = delay
        local before = cg.requests
        local response = fetch(cg, 'retry=overbudget-' .. before)
        t.assert_equals(response.response.status, 503)
        t.assert_equals(response.response.headers['retry-after'], delay)
        t.assert_equals(cg.requests - before, 1)
    end
end

local function set_timeout(cg, value)
    configure(cg, cg.endpoint, value)
end

g.test_configured_timeout_limits_total_budget = function(cg)
    set_timeout(cg, '1')
    local started = fiber.clock()
    local response = cg.cluster.servers[2]:http_request('get', '/forecast?budget=short', {raise = false})
    t.assert_equals(response.status, 504)
    t.assert_equals(response.body, 'Forecast request timed out')
    t.assert_equals(cg.requests, 1)
    local elapsed = fiber.clock() - started
    t.assert(elapsed >= 0.9 and elapsed < 3)
end

g.test_valid_timeout_allows_success = function(cg)
    cg.auto_respond = true
    for index, value in ipairs({'15', '+5'}) do
        set_timeout(cg, value)
        t.assert_equals(fetch(cg, 'budget=valid-' .. index).response.status, 200)
    end
    t.assert_equals(cg.requests, 2)
end

g.test_invalid_timeout_fails_role_startup = function(cg)
    for _, value in ipairs({
        '', '0', '-1', 'abc', '1.5', ' 5', string.char(255),
        '18446744073709551616', '18446744073709551615',
    }) do
        local result = probe_configuration(cg.endpoint, value)
        t.assert_equals(result.ok, false)
        t.assert_str_contains(result.error, 'OPEN_METEO_TIMEOUT_SECS')
    end
    t.assert_equals(cg.requests, 0)
end

g.test_defaults_validate_on_master_and_replica = function(cg)
    t.assert_equals(probe_configuration(nil, nil).ok, true)
    t.assert_equals(probe_configuration(nil, nil, true).ok, true)
    local invalid = probe_configuration(cg.endpoint, '0', true)
    t.assert_equals(invalid.ok, false)
    t.assert_str_contains(invalid.error, 'OPEN_METEO_TIMEOUT_SECS')
end

g.test_initialized_configuration_is_stable = function(cg)
    cg.auto_respond = true
    cg.cluster.main_server.net_box:eval([[
        os.setenv('OPEN_METEO_FORECAST_URL', 'invalid-after-initialization')
        os.setenv('OPEN_METEO_TIMEOUT_SECS', '0')
        local lib = box.lib.load('forecast')
        lib:load('init_forecast')() -- Explicit initialization is idempotent.
    ]])
    cg.cluster.main_server.net_box:eval("collectgarbage('collect')")
    local response = fetch(cg, 'configuration=stable')
    t.assert_equals(response.response.status, 200)
    t.assert_equals(cg.requests, 1)
    cg.cluster.main_server.net_box:eval([[
        os.setenv('OPEN_METEO_FORECAST_URL', ...)
        os.setenv('OPEN_METEO_TIMEOUT_SECS', '30')
    ]], {cg.endpoint})
end

g.test_response_body_limit_boundaries = function(cg)
    cg.auto_respond = true
    local router = cg.cluster.servers[2]
    for _, size in ipairs({1048575, 1048576, 1048577}) do
        cg.body = string.rep('x', size)
        local before = cg.requests
        local response = router:http_request('get', '/forecast?size=' .. size, {raise = false})
        if size <= 1048576 then
            t.assert_equals(response.status, 200)
            t.assert_equals(response.body, cg.body)
        else
            t.assert_equals(response.status, 502)
            t.assert_equals(response.body, 'Forecast response too large')
        end
        t.assert_equals(cg.requests, before + 1)
    end
end

g.test_oversized_errors_are_not_retried_and_smaller_responses_recover = function(cg)
    cg.auto_respond = true
    local router = cg.cluster.servers[2]
    for _, status in ipairs({400, 429, 503}) do
        cg.status = status
        cg.body = string.rep('x', 1048577)
        local before = cg.requests
        local response = router:http_request('get', '/forecast?overflow=' .. status, {raise = false})
        t.assert_equals(response.status, 502)
        t.assert_equals(response.body, 'Forecast response too large')
        t.assert_equals(cg.requests, before + 1)
    end
    cg.status, cg.body = 200, 'recovered'
    local response = router:http_request('get', '/forecast?overflow=503')
    t.assert_equals(response.body, 'recovered')
    t.assert_equals(response.headers['x-cache'], nil)
    t.assert_equals(router:http_request('get', '/forecast?overflow=503').headers['x-cache'], 'HIT')
end

g.test_custom_body_limit_and_utf8_expansion = function(cg)
    configure(cg, cg.endpoint, '30', '8')
    cg.auto_respond = true
    local router = cg.cluster.servers[2]
    for i, body in ipairs({'12345678', '123456789', string.char(255, 255, 255)}) do
        cg.body = body
        local response = router:http_request('get', '/forecast?custom-size=' .. i, {raise = false})
        t.assert_equals(response.status, i == 1 and 200 or 502)
    end
    t.assert_equals(cg.requests, 3)
end

g.test_invalid_body_limits_fail_initialization = function(cg)
    for _, value in ipairs({'', '0', '-1', 'abc', '1.5', string.char(255),
        '18446744073709551615', '18446744073709551616'}) do
        local result = probe_configuration(cg.endpoint, '30', false, value)
        t.assert_equals(result.ok, false)
        t.assert_str_contains(result.error, 'OPEN_METEO_MAX_RESPONSE_BYTES')
    end
    t.assert_equals(probe_configuration(cg.endpoint, '30', true, '8').ok, true)
end

g.test_chunked_body_limit_and_fiber_responsiveness = function(cg)
    fault_upstream(cg, 'chunked-oversized')
    local future = fetch(cg, 'chunked=1', true)
    -- The upstream sends headers then delays its body. Other TX fibers must run.
    fiber.sleep(0.1)
    t.assert_equals(cg.cluster.main_server.net_box:eval('return 42'), 42)
    local response = finish(future)
    t.assert_equals(response.response.status, 502)
    t.assert_equals(response.response.body, 'Forecast response too large')
    t.assert_equals(fault_state(cg).count, 1)
end

g.test_body_read_timeout_is_504 = function(cg)
    fault_upstream(cg, 'slow-body')
    configure(cg, cg.cluster.main_server.env.OPEN_METEO_FORECAST_URL, '1')
    local response = fetch(cg, 'body-timeout=1')
    t.assert_equals(response.response.status, 504)
    t.assert_equals(response.response.body, 'Forecast request timed out')
    t.assert_equals(fault_state(cg).count, 1)
end
