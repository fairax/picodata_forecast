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

g.before_all(function(cg)
    cg.mock = http.new('127.0.0.1', 0, {log_requests = false})
    cg.mock:route({method = 'GET', path = '/forecast'}, function(req)
        local gate, entered, status, body = cg.gate, cg.entered, cg.status, cg.body
        cg.requests = cg.requests + 1
        entered:put(true)
        gate:get(40)
        return {status = status, body = body or req.query, headers = {
            ['content-type'] = 'text/plain', ['retry-after'] = '7',
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
        use_vshard = true,
        env = {OPEN_METEO_FORECAST_URL = cg.endpoint},
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
    end)
end)

g.after_all(function(cg)
    helper.stop_cluster(cg.cluster)
    cg.mock:stop()
end)

g.before_each(function(cg)
    helper.truncate_space_on_cluster(cg.cluster, 'forecast_cache')
    cg.gate = fiber.channel(10)
    cg.entered = fiber.channel(10)
    cg.requests = 0
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
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        server.net_box:eval('os.setenv("OPEN_METEO_FORECAST_URL", ...)', {cg.endpoint})
    end
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
    local future = fetch(cg, 'error=1', true)
    t.assert_equals(cg.entered:get(5), true)
    cg.gate:put(true)
    local failure = finish(future)
    t.assert_equals(failure.ok, true)
    t.assert_equals(failure.response.status, 503)
    t.assert_equals(failure.response.body, 'error=1')
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    cg.status = 200
    local retry = fetch(cg, 'error=1', true)
    t.assert_equals(cg.entered:get(5), true)
    cg.gate:put(true)
    local success = finish(retry)
    t.assert_equals(success.ok, true)
    t.assert_equals(success.response.status, 200)
    t.assert_equals(cg.requests, 2)
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
        cg.body = '{"error":"upstream detail"}'
        for _ = 1, 2 do
            cg.gate:put(true)
            local response = router:http_request('get', '/forecast?error=' .. status, {raise = false})
            t.assert_equals(response.status, status)
            t.assert_equals(response.body, cg.body)
            t.assert_equals(response.headers['content-type'], 'text/plain')
            t.assert_equals(response.headers['retry-after'], '7')
            t.assert_equals(response.headers['x-cache'], nil)
        end
    end
    t.assert_equals(cg.requests, 6)
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        t.assert_equals(server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    end
end

local function set_endpoint(cg, endpoint)
    for _, server in ipairs({cg.cluster.servers[1], cg.cluster.servers[3]}) do
        server.net_box:eval('os.setenv("OPEN_METEO_FORECAST_URL", ...)', {endpoint})
    end
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
    local response = router:http_request('get', '/forecast?timeout=1', {raise = false, timeout = 35})
    t.assert_equals(response.status, 504)
    t.assert_equals(response.body, 'Forecast request timed out')
    cg.gate:put(true)
    t.assert_equals(cg.requests, 1)
end

g.test_invalid_configuration_is_generic_500 = function(cg)
    set_endpoint(cg, 'invalid-private-endpoint')
    local response = cg.cluster.servers[2]:http_request('get', '/forecast?internal=1', {raise = false})
    t.assert_equals(response.status, 500)
    t.assert_equals(response.body, 'Internal server error')
end

g.test_cache_lookup_failure_is_generic_500 = function(cg)
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
    local response = router:http_request('get', '/forecast?corrupt=1', {raise = false})
    t.assert_equals(response.status, 500)
    t.assert_equals(response.body, 'Internal server error')
    t.assert_equals(cg.requests, 1)
end

g.test_cache_insertion_failure_is_generic_500 = function(cg)
    -- Exceed memtx_max_tuple_size without changing production configuration.
    cg.body = string.rep('private-upstream-body', 100000)
    cg.gate:put(true)
    local response = cg.cluster.servers[2]:http_request('get', '/forecast?oversized=1', {raise = false})
    t.assert_equals(response.status, 500)
    t.assert_equals(response.body, 'Internal server error')
    t.assert_equals(cg.requests, 1)
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
