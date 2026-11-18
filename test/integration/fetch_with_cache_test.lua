local t = require('luatest')
local fiber = require('fiber')
local clock = require('clock')
local fio = require('fio')
local http = require('http.server')
local cartridge_helpers = require('cartridge.test-helpers')
local helper = require('test.helper')
local g = t.group('integration_fetch_with_cache')

local function fetch(cg, query, async)
    return cg.cluster.servers[2].net_box:eval([[
        local ok, response = pcall(require('app.roles.custom').fetch, ...)
        return {ok = ok, response = ok and response or tostring(response)}
    ]], {query}, {is_async = async or false, timeout = 35})
end

local function finish(future)
    local values, err = future:wait_result(35)
    assert(values, tostring(err))
    return values[1]
end

local function configure(cg, endpoint, timeout, concurrency, proxy)
    timeout = timeout or '30'
    proxy = proxy or ''
    for _, server in ipairs({cg.cluster.servers[2]}) do
        if server.env.OPEN_METEO_FORECAST_URL ~= endpoint or server.env.OPEN_METEO_TIMEOUT_SECS ~= timeout
            or server.env.OPEN_METEO_MAX_CONCURRENT_FETCHES ~= concurrency
            or server.env.OPEN_METEO_HTTP_PROXY ~= proxy then
            server:stop()
            server.env.OPEN_METEO_FORECAST_URL = endpoint
            server.env.OPEN_METEO_TIMEOUT_SECS = timeout
            server.env.OPEN_METEO_MAX_CONCURRENT_FETCHES = concurrency
            server.env.OPEN_METEO_HTTP_PROXY = proxy
            server:start()
            cg.cluster:wait_until_healthy(server)
        end
    end
end

local function probe_configuration(endpoint, timeout, replica, concurrency, proxy)
    local directory = fio.tempdir()
    local output = fio.pathjoin(directory, 'result.json')
    local env = table.copy(os.environ())
    env.OPEN_METEO_FORECAST_URL = endpoint
    env.OPEN_METEO_TIMEOUT_SECS = timeout
    env.OPEN_METEO_MAX_CONCURRENT_FETCHES = concurrency
    env.OPEN_METEO_HTTP_PROXY = proxy or ''
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
        table.insert(cg.upstream_request_ids, req.headers['x-request-id'] or false)
        table.insert(cg.request_times, fiber.clock())
        table.insert(cg.request_wall_times, clock.realtime())
        entered:put(true)
        if not cg.auto_respond and action.gated ~= false then gate:get(40) end
        return {status = status, body = body, headers = {
            ['content-type'] = action.content_type or 'text/plain', ['retry-after'] = retry_after,
            ['location'] = action.location, ['content-encoding'] = action.content_encoding,
            ['x-request-id'] = action.response_request_id,
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
        env = {OPEN_METEO_FORECAST_URL = cg.endpoint, OPEN_METEO_TIMEOUT_SECS = '30', OPEN_METEO_HTTP_PROXY = ''},
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
    cg.upstream_request_ids = {}
    cg.request_times = {}
    cg.request_wall_times = {}
    cg.auto_respond = false
    cg.actions = {}
    cg.retry_after = '0'
    cg.status = 200
    cg.body = nil
end)

local function set_router_role(cg, enabled)
    local roles = {'vshard-router'}
    if enabled then table.insert(roles, 'app.roles.custom') end
    cg.cluster.servers[2]:graphql({
        query = [[mutation($rs: String!, $roles: [String!]!) {
            cluster { edit_topology(replicasets: [{uuid: $rs, roles: $roles}]) { servers { uri } } }
        }]],
        variables = {rs = cg.cluster.servers[2].replicaset_uuid, roles = roles},
    })
end

g.after_each(function(cg)
    for _, server in ipairs(cg.cluster.servers) do
        if server.net_box ~= nil then
            server.net_box:eval([[
            local saved = rawget(_G, 'observability_saved_log')
            if saved then
                for level, fn in pairs(saved) do require('log').new('app.observability')[level] = fn end
                rawset(_G, 'observability_saved_log', nil)
                rawset(_G, 'observability_events', nil)
            end
            ]])
        end
    end
    cg.cluster.servers[2].env.SSL_CERT_FILE = nil
    if cg.router_role_disabled then
        set_router_role(cg, true)
        cg.router_role_disabled = nil
    end
    if cg.stopped_storage then
        cg.stopped_storage:start()
        cg.cluster:wait_until_healthy(cg.stopped_storage)
        cg.stopped_storage = nil
    end
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        server.net_box:eval([[
            local trigger = rawget(_G, 'cache_failure_trigger')
            if trigger then
                box.space.forecast_cache:before_replace(nil, trigger)
                rawset(_G, 'cache_failure_trigger', nil)
            end
        ]])
    end
    cg.gate:close()
    cg.entered:close()
    if cg.proxy_listener then cg.proxy_listener:close(); cg.proxy_listener = nil end
    if cg.header_listener ~= nil then
        cg.header_listener:close()
        cg.header_listener = nil
    end
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
    t.assert_equals(cg.cluster.servers[2].net_box:eval('return 42', {}, {timeout = 1}), 42)
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
    local response = cg.cluster.servers[2]:http_request('get', '/forecast?' .. query)
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

g.test_persistent_transient_statuses_stop_after_three_attempts = function(cg)
    cg.auto_respond = true
    for _, status in ipairs({429, 502, 503, 504}) do
        local before = cg.requests
        cg.status = status
        cg.body = 'final upstream error'
        local response = fetch(cg, 'persistent=' .. status)
        t.assert_equals(response.response.status, status)
        t.assert_equals(response.response.body, cg.body)
        t.assert_equals(response.response.headers['retry-after'], {'0'})
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
    t.assert_equals(cg.cluster.servers[2].net_box:eval('return 42', {}, {timeout = 0.5}), 42)
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
    t.assert(cg.request_wall_times[2] >= deadline, tostring(cg.request_wall_times[2] - deadline))
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

g.test_truncated_body_retries_and_caches_only_complete_response = function(cg)
    fault_upstream(cg, 'truncated')
    local router = cg.cluster.servers[2]
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

local function assert_truncated_failure(cg, mode)
    fault_upstream(cg, mode)
    local response = cg.cluster.servers[2]:http_request('get', '/forecast?truncated=' .. mode, {raise = false})
    t.assert_equals(response.status, 502)
    t.assert_equals(response.body, 'Forecast service unavailable')
    t.assert_equals(response.headers['x-cache'], nil)
    t.assert_equals(fault_state(cg).count, 3)
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        t.assert_equals(server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    end
end

g.test_truncated_body_is_not_forwarded_or_cached = function(cg)
    assert_truncated_failure(cg, 'truncated-always')
end

g.test_truncated_empty_body_is_not_accepted = function(cg)
    assert_truncated_failure(cg, 'truncated-empty')
end

g.test_truncated_error_response_is_transport_failure = function(cg)
    assert_truncated_failure(cg, 'truncated-error')
end

g.test_truncated_chunked_body_is_not_accepted = function(cg)
    assert_truncated_failure(cg, 'truncated-chunked')
end

g.test_not_modified_content_length_is_metadata = function(cg)
    fault_upstream(cg, 'not-modified')
    local response = fetch(cg, 'bodyless=304').response
    t.assert_equals(response.status, 304)
    t.assert_equals(response.body, '')
    t.assert_equals(response.headers['content-length'], {'20'})
    t.assert_equals(fault_state(cg).count, 1)
end

g.test_no_content_response_is_not_retried = function(cg)
    fault_upstream(cg, 'no-content')
    local response = fetch(cg, 'bodyless=204').response
    t.assert_equals(response.status, 204)
    t.assert_equals(response.body, '')
    t.assert_equals(fault_state(cg).count, 1)
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
        t.assert_equals(response.response.headers['retry-after'], {delay})
        t.assert_equals(cg.requests - before, 1)
    end
end

local function set_timeout(cg, value)
    configure(cg, cg.endpoint, value)
end

g.test_backoff_deadline_preserves_last_response = function(cg)
    configure(cg, cg.endpoint, '2')
    cg.auto_respond, cg.status, cg.body, cg.retry_after = true, 503, 'upstream detail', '1'
    local router = cg.cluster.servers[2]
    local pending = fetch(cg, 'retry=late-wakeup', true)
    t.assert_equals(cg.entered:get(5), true)
    fiber.sleep(0.1) -- Let the response arrive and the retry backoff begin.
    router.net_box:eval([[
        local fiber = require('fiber')
        fiber.set_max_slice({warn = 5, err = 5})
        local clock = require('clock')
        local deadline = clock.monotonic() + 2.2
        -- Simulate a busy scheduler resuming the fetch after its deadline.
        while clock.monotonic() < deadline do end
    ]], {}, {timeout = 5})
    local response = finish(pending).response
    t.assert_equals(response.status, 503)
    t.assert_equals(response.body, 'upstream detail')
    t.assert_equals(response.headers['retry-after'], {'1'})
    t.assert_equals(cg.requests, 1)
end

g.test_valid_delay_does_not_require_a_second_backoff_budget = function(cg)
    configure(cg, cg.endpoint, '3')
    cg.auto_respond = true
    cg.actions = {{status = 503, retry_after = '2'}, {status = 200, body = 'recovered'}}
    local response = fetch(cg, 'retry=valid-delay').response
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'recovered')
    t.assert_equals(cg.requests, 2)
end

g.test_configured_timeout_bounds_a_slow_stage = function(cg)
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

g.test_defaults_validate_on_router_instances = function(cg)
    t.assert_equals(probe_configuration(nil, nil).ok, true)
    t.assert_equals(probe_configuration(nil, nil, true).ok, true)
    local invalid = probe_configuration(cg.endpoint, '0', true)
    t.assert_equals(invalid.ok, false)
    t.assert_str_contains(invalid.error, 'OPEN_METEO_TIMEOUT_SECS')
end

g.test_initialized_configuration_is_stable = function(cg)
    cg.auto_respond = true
    cg.cluster.servers[2].net_box:eval([[
        os.setenv('OPEN_METEO_FORECAST_URL', 'invalid-after-initialization')
        os.setenv('OPEN_METEO_TIMEOUT_SECS', '0')
        local lib = box.lib.load('forecast')
        lib:load('init_forecast')() -- Explicit initialization is idempotent.
    ]])
    cg.cluster.servers[2].net_box:eval("collectgarbage('collect')")
    local response = fetch(cg, 'configuration=stable')
    t.assert_equals(response.response.status, 200)
    t.assert_equals(cg.requests, 1)
    cg.cluster.servers[2].net_box:eval([[
        os.setenv('OPEN_METEO_FORECAST_URL', ...)
        os.setenv('OPEN_METEO_TIMEOUT_SECS', '30')
    ]], {cg.endpoint})
end

g.test_large_response_bodies_are_forwarded = function(cg)
    cg.auto_respond = true
    local router = cg.cluster.servers[2]
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
    local router = cg.cluster.servers[2]
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
    local router = cg.cluster.servers[2]
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
    local router = cg.cluster.servers[2]
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
    local router = cg.cluster.servers[2]
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
    local router = cg.cluster.servers[2]
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
    t.assert_equals(cg.cluster.servers[2].net_box:eval('return 42'), 42)
    local response = finish(future)
    t.assert_equals(response.ok, true)
    t.assert_equals(response.response.status, 200)
    t.assert_equals(response.response.body, string.rep('x', 1048577))
    t.assert_equals(fault_state(cg).count, 1)
end

g.test_body_read_timeout_is_504 = function(cg)
    fault_upstream(cg, 'slow-body')
    configure(cg, cg.cluster.servers[2].env.OPEN_METEO_FORECAST_URL, '1')
    local response = fetch(cg, 'body-timeout=1')
    t.assert_equals(response.response.status, 504)
    t.assert_equals(response.response.body, 'Forecast request timed out')
    t.assert_equals(fault_state(cg).count, 1)
end

local function location(cg, query)
    local result = cg.cluster.servers[2].net_box:eval([[
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

g.test_unavailable_storage_still_fetches_upstream = function(cg)
    cg.auto_respond = true
    local _, source = location(cg, 'unavailable=1')
    cg.stopped_storage = source
    source:stop()
    local started = fiber.clock()
    local response = cg.cluster.servers[2]:http_request('get', '/forecast?unavailable=1')
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
            local response = cg.cluster.servers[2]:http_request('get', '/forecast?' .. query)
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
    local response = cg.cluster.servers[2].net_box:eval([[
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
    cg.cluster.servers[2].net_box:eval("os.setenv('OPEN_METEO_MAX_CONCURRENT_FETCHES', '10')")
    local before = cg.requests
    local response = cg.cluster.servers[2]:http_request('get', '/forecast?limit=third', {raise = false})
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

g.test_fetch_limit_configuration = function(cg)
    t.assert_equals(probe_configuration(cg.endpoint, '30', false, '2').ok, true)
    for _, value in ipairs({'', '0', '-1', '1.5', 'abc', ' 2', '1e2', '9007199254740992'}) do
        local result = probe_configuration(cg.endpoint, '30', false, value)
        t.assert_equals(result.ok, false)
        t.assert_str_contains(result.error, 'OPEN_METEO_MAX_CONCURRENT_FETCHES')
    end
end

g.test_fetch_limit_retains_permit_during_cancellation = function(cg)
    configure(cg, cg.endpoint, '30', '1')
    local router = cg.cluster.servers[2]
    router.net_box:eval([[
        local fiber = require('fiber')
        rawset(_G, 'limited_fetch_done', false)
        rawset(_G, 'limited_fetch', fiber.new(function()
            pcall(require('app.roles.custom').fetch, 'limit=cancelled')
            rawset(_G, 'limited_fetch_done', true)
        end))
    ]])
    t.assert_equals(cg.entered:get(5), true)
    router.net_box:eval([[
        require('fiber').create(function() limited_fetch:cancel() end)
    ]])
    fiber.sleep(0.05)
    t.assert_equals(fetch(cg, 'limit=while-cancelled').response.status, 503)
    t.assert_equals(cg.requests, 1)
    cg.gate:put(true)
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(router.net_box:eval('return limited_fetch_done'), true)
    end)
    cg.auto_respond = true
    t.assert_equals(fetch(cg, 'limit=after-cancelled').response.status, 200)
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

local function raw_http_response(router, path, extra_headers)
    local socket = require('socket')
    local conn = assert(socket.tcp_connect('127.0.0.1', router.http_port, 5))
    assert(conn:write('GET ' .. path .. ' HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n' ..
        (extra_headers or '') .. '\r\n', 5))
    local raw = assert(conn:read({delimiter = '\r\n\r\n'}, 5))
    local headers = {}
    for name, value in raw:gmatch('([^\r\n:]+): ([^\r\n]*)\r\n') do
        name = name:lower()
        local previous = headers[name]
        if previous == nil then headers[name] = value
        elseif type(previous) == 'table' then previous[#previous + 1] = value
        else headers[name] = {previous, value} end
    end
    local body = assert(conn:read(tonumber(headers['content-length']), 5))
    conn:close()
    return {status = tonumber(raw:match('^HTTP/1%.1 (%d+)')), headers = headers, body = body}
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
    local router = cg.cluster.servers[2]
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
    local response = cg.cluster.servers[2]:http_request('get', '/forecast?' .. query)
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
    local raw = raw_http_response(cg.cluster.servers[2], '/forecast?' .. query)
    t.assert_equals(raw.headers['x-safe-values'], {'first', 'second'})
    t.assert_equals(raw.headers['x-only-unsafe'], nil)
    t.assert_equals(cg.requests, 0)
end

g.test_router_role_disable_and_reenable = function(cg)
    configure(cg, cg.endpoint, '30', '1')
    local router = cg.cluster.servers[2]
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
    local response = cg.cluster.servers[2]:http_request('get', '/forecast?redirect=start',
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

g.test_fibreq_https_with_trusted_test_certificate = function(cg)
    fault_upstream(cg, 'certificate')
    cg.cluster.servers[2].env.SSL_CERT_FILE = fio.pathjoin(cg.fault_directory, 'cert.pem')
    configure(cg, 'https://localhost:' .. fault_state(cg).port .. '/forecast')
    local response = fetch(cg, 'tls=trusted').response
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'ok')
    t.assert_equals(fault_state(cg).count, 1)
end

-- A local-only proxy fixture; HTTPS bytes are relayed unchanged to the TLS fixture.
local function proxy_server(cg, mode, tls_port)
    local socket = require('socket')
    cg.proxy_requests = {}
    cg.proxy_listener = assert(socket.tcp_server('127.0.0.1', 0, function(connection)
        local head = connection:read({delimiter = '\r\n\r\n'}, 5)
        if not head then return end
        cg.proxy_requests[#cg.proxy_requests + 1] = head
        if mode == 'slow' then fiber.sleep(2) end
        if mode == 'reject' then
            connection:write('HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 6\r\n\r\nsecret', 1)
            return
        end
        if mode == 'malformed' then connection:write('invalid proxy head\r\n\r\n', 1); return end
        if mode == 'oversized' then
            connection:write('HTTP/1.1 200 OK\r\nLong: ' .. string.rep('x', 17000) .. '\r\n\r\n', 1)
            return
        end
        if head:match('^CONNECT ') then
            local upstream = assert(socket.tcp_connect('127.0.0.1', tls_port, 3))
            connection:write('HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 Connection Established\r\n\r\n', 3)
            local function relay(from, to)
                while from:readable(5) do
                    local data = from:sysread(8192)
                    if not data or data == '' then break end
                    if not to:write(data, 5) then break end
                end
            end
            local reader = fiber.create(function() relay(upstream, connection); connection:shutdown() end)
            reader:set_joinable(true)
            relay(connection, upstream)
            upstream:shutdown()
            reader:join()
            upstream:close()
        else
            local status = mode == 'retry' and #cg.proxy_requests == 1 and 503 or 200
            local body = mode == 'binary' and string.char(0, 255, 128) or 'proxied'
            local headers = 'Content-Type: application/octet-stream\r\nX-Repeat: first\r\nX-Repeat: second\r\n'
            connection:write('HTTP/1.1 ' .. status .. ' Result\r\nContent-Length: ' .. #body .. '\r\n' ..
                headers .. 'Retry-After: 0\r\nConnection: close\r\n\r\n' .. body, 1)
        end
    end))
    return 'http://127.0.0.1:' .. cg.proxy_listener:name().port
end

g.test_proxy_http_absolute_target_auth_and_cache = function(cg)
    local proxy = proxy_server(cg, 'binary'):gsub('http://', 'http://user:p%%40ss@')
    configure(cg, 'http://unresolvable.invalid:1234/forecast', nil, nil, proxy)
    for _ = 1, 2 do
        local response = fetch(cg, 'b=2&a=1').response
        t.assert_equals(response.status, 200)
        t.assert_equals(response.body, string.char(0, 255, 128))
        t.assert_equals(response.headers['x-repeat'], {'first', 'second'})
    end
    t.assert_equals(#cg.proxy_requests, 1)
    t.assert_str_contains(cg.proxy_requests[1], 'GET http://unresolvable.invalid:1234/forecast?a=1&b=2 HTTP/1.1')
    t.assert_str_contains(cg.proxy_requests[1]:lower(), 'proxy-authorization: basic dxnlcjpwqhnz')
    t.assert_equals(cg.proxy_requests[1]:lower():find('x-request-id', 1, true), nil)
end

g.test_proxy_https_connect_and_certificate_validation = function(cg)
    fault_upstream(cg, 'certificate')
    local port = fault_state(cg).port
    local proxy = proxy_server(cg, 'tunnel', port):gsub('http://', 'http://user:p%%40ss@')
    local endpoint = 'https://localhost:' .. port .. '/forecast'
    configure(cg, endpoint, nil, nil, proxy)
    t.assert_equals(fetch(cg, 'proxy=untrusted').response.status, 502)
    t.assert_equals(#cg.proxy_requests, 1) -- Certificate failures are not retried.
    cg.cluster.servers[2].env.SSL_CERT_FILE = fio.pathjoin(cg.fault_directory, 'cert.pem')
    configure(cg, endpoint, '29', nil, proxy) -- Restart with the trusted test certificate.
    local response = fetch(cg, 'proxy=trusted').response
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'ok')
    t.assert_str_contains(cg.proxy_requests[2], 'CONNECT localhost:' .. port .. ' HTTP/1.1')
    t.assert_str_contains(cg.proxy_requests[2]:lower(), 'proxy-authorization: basic dxnlcjpwqhnz')
    t.assert_equals(fault_state(cg).proxy_authorization_seen, false)
end

g.test_proxy_rejection_is_generic_without_direct_fallback = function(cg)
    local proxy = proxy_server(cg, 'reject')
    configure(cg, cg.endpoint, nil, nil, proxy)
    for _ = 1, 2 do
        local response = fetch(cg, 'proxy=rejected').response
        t.assert_equals(response.status, 502)
        t.assert_equals(response.body, 'Forecast service unavailable')
    end
    t.assert_equals(#cg.proxy_requests, 2)
    t.assert_equals(cg.requests, 0)
end

g.test_proxy_timeout_yields_and_retries_transient_responses = function(cg)
    local proxy = proxy_server(cg, 'slow')
    configure(cg, cg.endpoint, '1', nil, proxy)
    local future = fetch(cg, 'proxy=timeout', true)
    t.helpers.retrying({timeout = 3}, function() t.assert_equals(#cg.proxy_requests, 1) end)
    t.assert_equals(cg.cluster.servers[2].net_box:eval('return 42', {}, {timeout = 0.5}), 42)
    t.assert_equals(finish(future).response.status, 504)
    t.assert_equals(#cg.proxy_requests, 1)
    cg.proxy_listener:close()
    proxy = proxy_server(cg, 'retry')
    configure(cg, cg.endpoint, nil, nil, proxy)
    for _ = 1, 2 do t.assert_equals(fetch(cg, 'proxy=retry').response.status, 200) end
    t.assert_equals(#cg.proxy_requests, 2) -- Retry then success; subsequent call hits cache.
end

g.test_proxy_connect_malformed_and_large_heads = function(cg)
    for _, mode in ipairs({'malformed', 'oversized'}) do
        local proxy = proxy_server(cg, mode)
        configure(cg, 'https://unresolvable.invalid/forecast', nil, nil, proxy)
        t.assert_equals(fetch(cg, 'proxy=' .. mode).response.status, 502)
        t.assert_equals(#cg.proxy_requests, 1)
        cg.proxy_listener:close()
        cg.proxy_listener = nil
    end
end

g.test_proxy_invalid_configuration_fails_without_exposing_credentials = function()
    for _, value in ipairs({'invalid', 'https://proxy:443', 'socks5://proxy:1080',
        'http://user:secret@proxy/path', 'http://proxy?q=1', 'http://proxy#fragment', string.char(255)}) do
        local result = probe_configuration('http://localhost/forecast', '30', false, nil, value)
        t.assert_equals(result.ok, false)
        t.assert_str_contains(result.error, 'OPEN_METEO_HTTP_PROXY')
        t.assert_equals(result.error:find('secret', 1, true), nil)
    end
end

local function request_with_id(cg, query, request_id)
    return cg.cluster.servers[2]:http_request('get', '/forecast?' .. query,
        {raise = false, http = {headers = {['X-Request-ID'] = request_id}}})
end

local function capture_events(cg)
    for _, server in ipairs(cg.cluster.servers) do
        server.net_box:eval([[
            local log = require('log').new('app.observability')
            local saved, events = {}, {}
            rawset(_G, 'observability_saved_log', saved)
            rawset(_G, 'observability_events', events)
            for _, level in ipairs({'debug', 'info', 'warn', 'error'}) do
                saved[level] = log[level]
                log[level] = function(record, ...)
                    if type(record) == 'table' and record.request_id then
                        local copy = table.copy(record)
                        copy.level = level
                        events[#events + 1] = copy
                    end
                    return saved[level](record, ...)
                end
            end
        ]])
    end
end

local function metric(cg, name, labels)
    return cg.cluster.servers[2].net_box:eval([[
        local name, labels = ...
        local total = 0
        for _, sample in ipairs(require('metrics').collect()) do
            if sample.metric_name == name then
                local matches = true
                for k, v in pairs(labels or {}) do
                    if sample.label_pairs[k] ~= v then matches = false end
                end
                if matches then total = total + sample.value end
            end
        end
        return total
    ]], {name, labels or {}})
end

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
    local previous
    for i, value in ipairs({'', 'contains spaces', 'first, second', string.rep('x', 129)}) do
        local response = request_with_id(cg, 'trace=invalid-' .. i, value)
        local id = response.headers['x-request-id']
        t.assert_equals(#id, 36)
        t.assert(require('uuid').fromstr(id) ~= nil)
        t.assert(id ~= previous)
        previous = id
    end
    local generated = cg.cluster.servers[2]:http_request('get', '/forecast?trace=generated')
    t.assert_equals(#generated.headers['x-request-id'], 36)
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
    t.assert_str_contains(cg.cluster.servers[2]:http_request('get', '/metrics').body,
        'forecast_upstream_attempts_total')
end

g.test_observability_concurrent_requests_keep_distinct_context = function(cg)
    capture_events(cg)
    local router = cg.cluster.servers[2]
    local futures = {}
    for i = 1, 2 do
        futures[i] = router.net_box:eval([[
            local i = ...
            return require('app.roles.custom').fetch('trace=parallel-' .. i, 'parallel-' .. i)
        ]], {i}, {is_async = true, timeout = 35})
    end
    for _ = 1, 2 do t.assert_equals(cg.entered:get(5), true) end
    t.assert_equals(metric(cg, 'forecast_active_fetches'), 2)
    for _ = 1, 2 do cg.gate:put(true) end
    for _, future in ipairs(futures) do t.assert_equals(assert(future:wait_result(5))[1].status, 200) end
    t.assert_equals(cg.upstream_request_ids, {false, false})
    t.assert_equals(metric(cg, 'forecast_active_fetches'), 0)
end

g.test_observability_internal_errors_are_generic_with_request_id = function(cg)
    local router = cg.cluster.servers[2]
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
    local router = cg.cluster.servers[2]
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

g.test_observability_wrapped_cancellation_preserves_error_and_counts_once = function(cg)
    local result = cg.cluster.servers[2].net_box:eval([[
        local fiber = require('fiber')
        local obs = require('app.observability')
        local original, received, calls
        calls = 0
        local wrapped = obs.wrap_http(function()
            calls = calls + 1
            local _, err = pcall(fiber.sleep, 100)
            original = err
            error(err, 0)
        end)
        local task = fiber.create(function()
            local ok, err = pcall(wrapped, {endpoint = {path = '/wrapped-cancellation', method = 'GET'}})
            received = {ok = ok, same = rawequal(err, original), kind = err.type}
        end)
        task:set_joinable(true)
        task:cancel()
        task:join()
        received.calls = calls
        return received
    ]])
    t.assert_equals(result, {ok = false, same = true, kind = 'FiberIsCancelled', calls = 1})
    t.assert_equals(metric(cg, 'http_server_request_latency_count',
        {path = '/wrapped-cancellation', method = 'GET', status = 'cancelled'}), 1)
end

g.test_observability_wrapped_outcomes_survive_metric_errors = function(cg)
    cg.cluster.servers[2].net_box:eval([[
        local t = require('luatest')
        local obs = require('app.observability')
        local collector
        for _, candidate in pairs(require('metrics').collectors()) do
            if candidate.name == 'http_server_request_latency' then collector = candidate end
        end
        local request = {endpoint = {path = '/wrapped-outcome', method = 'GET'}}
        local response, original, calls = {status = 202, body = 'accepted'}, {}, 0
        local function check()
            local returned = obs.wrap_http(function() calls = calls + 1; return response end)(request)
            t.assert(rawequal(returned, response))
            local ok, err = pcall(obs.wrap_http(function() calls = calls + 1; error(original, 0) end), request)
            t.assert_not(ok)
            t.assert(rawequal(err, original))
        end
        check()
        local observe = collector.observe
        collector.observe = function() error('injected observation failure') end
        local ok, err = pcall(check)
        collector.observe = observe
        t.assert(ok, tostring(err))
        t.assert_equals(calls, 4)
    ]])
    for _, status in ipairs({202, 500}) do
        t.assert_equals(metric(cg, 'http_server_request_latency_count',
            {path = '/wrapped-outcome', method = 'GET', status = status}), 1)
    end
end

g.test_observability_wrapped_default_status_and_string_error = function(cg)
    cg.cluster.servers[2].net_box:eval([[
        local t = require('luatest')
        local obs = require('app.observability')
        local request = {endpoint = {path = '/wrapped-default', method = 'GET'}}
        local response, calls = {body = 'ok'}, 0
        local returned = obs.wrap_http(function() calls = calls + 1; return response end)(request)
        t.assert(rawequal(returned, response))
        local ok, err = pcall(obs.wrap_http(function()
            calls = calls + 1
            error('original handler error', 0)
        end), request)
        t.assert_not(ok)
        -- The library may prefix string errors with its source location.
        t.assert_str_contains(err, 'original handler error')
        t.assert_equals(calls, 2)
    ]])
    for _, status in ipairs({200, 500}) do
        t.assert_equals(metric(cg, 'http_server_request_latency_count',
            {path = '/wrapped-default', method = 'GET', status = status}), 1)
    end
end

g.test_observability_repeated_incoming_ids_generate_uuid = function(cg)
    cg.auto_respond = true
    local response = raw_http_response(cg.cluster.servers[2], '/forecast?trace=duplicate',
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
    local router = cg.cluster.servers[2]
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
    local router = cg.cluster.servers[2]
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
