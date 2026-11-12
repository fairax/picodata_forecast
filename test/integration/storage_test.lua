local t = require('luatest')
local cartridge_helpers = require('cartridge.test-helpers')
local fio = require('fio')
local http = require('http.server')
local helper = require('test.helper')
local g = t.group('integration_storage')

local function call(cg, name, ...)
    return cg.cluster.main_server.net_box:eval([[
        local name, args = ...
        if name == 'fetch_with_cache' then
            table.insert(args, 1) -- Standalone storage tests do not bootstrap vshard.
        end
        local lib = box.lib.load('forecast')
        local func = lib:load(name)
        return func(unpack(args))
    ]], {name, {...}})
end

local function seed_expired(cg, key)
    cg.cluster.main_server.net_box:eval([[
        local key = ...
        box.space.forecast_cache:replace({key, 1, 200, 'old', {['content-type'] = 'application/json'}, 0, 0})
    ]], {key})
end

g.before_all(function(cg)
    cg.mock = http.new('127.0.0.1', 0, {log_requests = false})
    cg.mock:route({method = 'GET', path = '/forecast'}, function(req)
        cg.requests = cg.requests + 1
        cg.last_query = req.query
        return {status = cg.status, body = cg.body, headers = {['content-type'] = 'application/json'}}
    end)
    cg.mock:start()
    local port = cg.mock.tcp_server:name().port
    cg.cluster = cartridge_helpers.Cluster:new({
        server_command = helper.server_command,
        datadir = fio.pathjoin(helper.datadir, 'storage'),
        base_http_port = 18080,
        base_advertise_port = 18300,
        use_vshard = false,
        env = {OPEN_METEO_FORECAST_URL = 'http://127.0.0.1:' .. port .. '/forecast'},
        replicasets = {{
            alias = 'storage',
            uuid = cartridge_helpers.uuid('b'),
            roles = {'app.roles.storage'},
            servers = {{
                instance_uuid = cartridge_helpers.uuid('b', 1),
                alias = 'storage',
            }},
        }},
    })
    cg.cluster:start()
end)

g.after_all(function(cg)
    helper.stop_cluster(cg.cluster)
    cg.mock:stop()
end)

g.before_each(function(cg)
    helper.truncate_space_on_cluster(cg.cluster, 'forecast_cache')
    cg.requests = 0
    cg.last_query = nil
    cg.status = 200
    cg.body = '{"temperature":15}'
end)

g.after_each(function(cg)
    cg.cluster.main_server.net_box:eval("require('app.roles.storage').apply_config(nil, {is_master = true})")
end)

g.test_schema = function(cg)
    local schema = cg.cluster.main_server.net_box:eval([[
        local space = assert(box.space.forecast_cache)
        local indexes = {}
        for _, name in ipairs({'primary', 'bucket_id', 'expires_at', 'lru'}) do
            indexes[name] = space.index[name].parts
        end
        return {format = space:format(), indexes = indexes, temporary = space.temporary}
    ]])
    t.assert_equals(schema.temporary, true)
    t.assert_equals(schema.format, {
        {name = 'key', type = 'string', is_nullable = false},
        {name = 'bucket_id', type = 'unsigned', is_nullable = false},
        {name = 'status', type = 'unsigned', is_nullable = false},
        {name = 'body', type = 'string', is_nullable = false},
        {name = 'headers', type = 'map', is_nullable = false},
        {name = 'expires_at', type = 'unsigned', is_nullable = false},
        {name = 'last_access', type = 'unsigned', is_nullable = false},
    })
    t.assert_equals(schema.indexes.primary[1].fieldno, 1)
    t.assert_equals(schema.indexes.bucket_id[1].fieldno, 2)
    t.assert_equals(schema.indexes.expires_at[1].fieldno, 6)
    t.assert_equals(schema.indexes.lru[1].fieldno, 7)
    call(cg, 'create_schema') -- Reinitialization must preserve an existing schema.
end

g.test_put_and_get = function(cg)
    local fetched = call(cg, 'fetch_with_cache', 'forecast=1')
    t.assert_equals(fetched.status, 200)
    t.assert_equals(fetched.body, cg.body)
    local expected = fetched
    expected.headers['x-cache'] = 'HIT'
    t.assert_equals(call(cg, 'fetch_with_cache', 'forecast=1'), expected)
    t.assert_equals(cg.requests, 1)
end

g.test_missing_key_fetches_and_stores = function(cg)
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    t.assert_equals(call(cg, 'fetch_with_cache', 'missing=1').body, cg.body)
    t.assert_equals(cg.requests, 1)
    t.assert_equals(cg.last_query, 'missing=1')
    local tuple = cg.cluster.main_server.net_box:eval("return box.space.forecast_cache:get('missing=1')")
    t.assert_equals(tuple[4], cg.body)
end

g.test_refresh_expired_entry = function(cg)
    seed_expired(cg, 'forecast=1')
    cg.body = 'new'
    t.assert_equals(call(cg, 'fetch_with_cache', 'forecast=1').body, 'new')
    t.assert_equals(cg.requests, 1)
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 1)
    t.assert_equals(call(cg, 'fetch_with_cache', 'forecast=1').body, 'new')
    t.assert_equals(cg.requests, 1)
end

g.test_expired_read_deletes_entry = function(cg)
    seed_expired(cg, 'expired=1')
    cg.status = 503
    local response = call(cg, 'fetch_with_cache', 'expired=1')
    t.assert_equals(response.status, 503)
    t.assert_equals(response.body, cg.body)
    t.assert_equals(cg.requests, 3)
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 0)
end

g.test_background_cleanup_removes_untouched_expired_entries = function(cg)
    seed_expired(cg, 'expired=1')
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    end)
    t.assert_equals(cg.requests, 0)
end

g.test_insert_does_not_scan_other_expired_entries = function(cg)
    cg.cluster.main_server.net_box:eval("require('app.roles.storage').stop()")
    seed_expired(cg, 'expired=1')
    call(cg, 'fetch_with_cache', 'fresh=1')
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 2)
    cg.cluster.main_server.net_box:eval("require('app.roles.storage').apply_config(nil, {is_master = true})")
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 1)
    end)
end

g.test_restart_discards_cache_contents = function(cg)
    call(cg, 'fetch_with_cache', 'restart=1')
    local server = cg.cluster.main_server
    server.net_box:eval('box.snapshot()')
    server:stop()
    server:start()
    cg.cluster:wait_until_healthy(server)
    t.assert_equals(server.net_box:eval('return box.space.forecast_cache.temporary'), true)
    t.assert_equals(server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    t.assert_equals(call(cg, 'fetch_with_cache', 'restart=1').status, 200)
    t.assert_equals(cg.requests, 2)
end

g.test_schema_migrates_persistent_cache_and_preserves_temporary_cache = function(cg)
    cg.cluster.main_server.net_box:eval([[
        require('app.roles.storage').stop()
        box.space.forecast_cache:drop()
        local space = box.schema.space.create('forecast_cache')
        space:create_index('primary')
        space:insert({1, 'legacy'})
    ]])
    call(cg, 'create_schema')
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache.temporary'), true)
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    call(cg, 'fetch_with_cache', 'preserve=1')
    call(cg, 'create_schema')
    t.assert_equals(call(cg, 'fetch_with_cache', 'preserve=1').headers['x-cache'], 'HIT')
    t.assert_equals(cg.requests, 1)
end
