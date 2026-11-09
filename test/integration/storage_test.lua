local t = require('luatest')
local cartridge_helpers = require('cartridge.test-helpers')
local fio = require('fio')
local helper = require('test.helper')
local g = t.group('integration_storage')

local function call(cg, name, ...)
    return cg.cluster.main_server.net_box:eval([[
        local name, args = ...
        local lib = box.lib.load('forecast')
        local func = lib:load(name)
        return func(unpack(args))
    ]], {name, {...}})
end

local function response(body)
    return {
        status = 200,
        body = body,
        headers = {['content-type'] = 'application/json'},
    }
end

g.before_all(function(cg)
    cg.cluster = cartridge_helpers.Cluster:new({
        server_command = helper.server_command,
        datadir = fio.pathjoin(helper.datadir, 'storage'),
        base_http_port = 18080,
        base_advertise_port = 18300,
        use_vshard = false,
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
end)

g.before_each(function(cg)
    helper.truncate_space_on_cluster(cg.cluster, 'forecast_cache')
end)

g.test_schema = function(cg)
    local schema = cg.cluster.main_server.net_box:eval([[
        local space = assert(box.space.forecast_cache)
        local indexes = {}
        for _, name in ipairs({'primary', 'bucket_id', 'expires_at', 'lru'}) do
            indexes[name] = space.index[name].parts
        end
        return {format = space:format(), indexes = indexes}
    ]])
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
    call(cg, 'add_cache_entry', 'forecast', 1, response('{"temperature":15}'), {secs = 60, nanos = 0})
    local expected = response('{"temperature":15}')
    expected.headers['x-cache'] = 'HIT'
    t.assert_equals(call(cg, 'get_cache_entry', 'forecast'), expected)
end

g.test_missing_key = function(cg)
    t.assert_equals(call(cg, 'get_cache_entry', 'missing'), nil)
end

g.test_overwrite = function(cg)
    call(cg, 'add_cache_entry', 'forecast', 1, response('old'), {secs = 60, nanos = 0})
    call(cg, 'add_cache_entry', 'forecast', 1, response('new'), {secs = 60, nanos = 0})
    t.assert_equals(call(cg, 'get_cache_entry', 'forecast').body, 'new')
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 1)
end

g.test_expired_read_deletes_entry = function(cg)
    call(cg, 'add_cache_entry', 'expired', 1, response('old'), {secs = 0, nanos = 0})
    t.assert_equals(call(cg, 'get_cache_entry', 'expired'), nil)
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 0)
end

g.test_insert_removes_expired_entries = function(cg)
    call(cg, 'add_cache_entry', 'expired', 1, response('old'), {secs = 0, nanos = 0})
    call(cg, 'add_cache_entry', 'fresh', 1, response('new'), {secs = 60, nanos = 0})
    local keys = cg.cluster.main_server.net_box:eval([[
        local keys = {}
        for _, tuple in box.space.forecast_cache:pairs() do
            table.insert(keys, tuple[1])
        end
        return keys
    ]])
    t.assert_equals(keys, {'fresh'})
end
