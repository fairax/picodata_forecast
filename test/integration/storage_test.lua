local t = require('luatest')
local cartridge_helpers = require('cartridge.test-helpers')
local fio = require('fio')
local helper = require('test.helper')
local binary = require('test.fixtures.binary')
local g = t.group('integration_storage')

local function call(cg, name, ...)
    return cg.cluster.main_server.net_box:eval([[
        local name, args = ...
        local lib = box.lib.load('forecast')
        local func = lib:load(name)
        if name == 'cache_get' or name == 'cache_put' then args[#args + 1] = require('uuid').str() end
        return func(unpack(args))
    ]], {name, {...}})
end

local function seed_expired(cg, key)
    cg.cluster.main_server.net_box:eval([[
        local key = ...
        local binary = require('test.fixtures.binary')
        box.space.forecast_cache:replace({key, 1, 200, binary('old'), {['content-type'] = {'application/json'}}, 0, 0})
    ]], {key})
end

g.before_all(function(cg)
    cg.cluster = cartridge_helpers.Cluster:new({
        server_command = helper.server_command,
        datadir = fio.pathjoin(helper.datadir, 'storage'),
        base_http_port = 18080,
        base_advertise_port = 18300,
        use_vshard = false,
        env = {OPEN_METEO_FORECAST_URL = 'invalid-unused-on-storage',
            OPEN_METEO_TIMEOUT_SECS = '0'},
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

g.after_each(function(cg)
    cg.cluster.main_server.net_box:eval([[
        if rawget(_G, 'cache_failure_trigger') then
            box.space.forecast_cache:before_replace(nil, cache_failure_trigger)
            rawset(_G, 'cache_failure_trigger', nil)
        end
    ]])
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
        {name = 'body', type = 'varbinary', is_nullable = false},
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

local function put(cg, query, body)
    return call(cg, 'cache_put', query, 1, {
        status = 200, body = body or 'new', headers = {['content-type'] = {'application/json'}},
    })
end

g.test_put_and_get = function(cg)
    t.assert_equals(put(cg, 'forecast=1'), true)
    local response = call(cg, 'cache_get', 'forecast=1')
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'new')
    t.assert_equals(response.headers['x-cache'], {'HIT'})
    local timing = cg.cluster.main_server.net_box:eval([[
        local tuple = box.space.forecast_cache:get('forecast=1')
        return {ttl = tuple[6] - tuple[7], bucket = tuple[2]}
    ]])
    t.assert(timing.ttl > 59000000 and timing.ttl <= 60000000)
    t.assert_equals(timing.bucket, 1)
end

g.test_repeated_headers_round_trip = function(cg)
    local headers = {['content-type'] = {'text/plain'},
        ['set-cookie'] = {'first=1; Expires=Wed, 21 Oct 2037 07:28:00 GMT', 'second=2'},
        ['cache-control'] = {'no-store', 'max-age=60'}}
    t.assert_equals(call(cg, 'cache_put', 'headers=multiple', 1,
        {status = 200, body = 'body', headers = headers}), true)
    local response = call(cg, 'cache_get', 'headers=multiple')
    headers['x-cache'] = {'HIT'}
    t.assert_equals(response.headers, headers)
end

g.test_missing_key_returns_miss = function(cg)
    t.assert_equals(call(cg, 'cache_get', 'missing=1'), nil)
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 0)
end

g.test_binary_body_round_trip = function(cg)
    local bytes = {}
    for i = 0, 255 do bytes[#bytes + 1] = string.char(i) end
    for i, body in ipairs({' ', string.char(0), table.concat(bytes)}) do
        -- Both Lua byte strings and MessagePack binary procedure inputs are accepted.
        for j, input in ipairs({body, binary(body)}) do
            local key = 'binary=' .. i .. '-' .. j
            t.assert_equals(put(cg, key, input), true)
            local response = call(cg, 'cache_get', key)
            t.assert_equals(response.body, body)
            t.assert_equals(response.headers['x-cache'], {'HIT'})
            local marker = cg.cluster.main_server.net_box:eval([[
                local msgpack = require('msgpack')
                local encoded = msgpack.encode(box.space.forecast_cache:get(...))
                local start = require('ffi').cast('const char *', encoded)
                local _, after_header = msgpack.decode_array_header(start, #encoded)
                local offset = tonumber(after_header - start) + 1
                for _ = 1, 3 do _, offset = msgpack.decode(encoded, offset) end
                return encoded:byte(offset)
            ]], {key})
            t.assert(marker == 0xc4 or marker == 0xc5 or marker == 0xc6)
        end
    end
end

g.test_empty_bodies_skip_insertion_and_preserve_existing_entries = function(cg)
    for i, body in ipairs({'', binary('')}) do
        local key = 'empty=' .. i
        t.assert_equals(put(cg, key, body), true)
        t.assert_equals(call(cg, 'cache_get', key), nil)
        t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), i - 1)
        t.assert_equals(put(cg, key, 'existing'), true)
        t.assert_equals(put(cg, key, body), true)
        t.assert_equals(call(cg, 'cache_get', key).body, 'existing')
    end
end

g.test_expired_read_deletes_entry = function(cg)
    seed_expired(cg, 'expired=1')
    t.assert_equals(call(cg, 'cache_get', 'expired=1'), nil)
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 0)
end

g.test_non_successful_responses_are_not_cached = function(cg)
    call(cg, 'cache_put', 'error=1', 1, {status = 503, body = 'error', headers = {type = {'text'}}})
    t.assert_equals(call(cg, 'cache_get', 'error=1'), nil)
end

g.test_background_cleanup_removes_untouched_expired_entries = function(cg)
    seed_expired(cg, 'expired=1')
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    end)
end

g.test_insert_does_not_scan_other_expired_entries = function(cg)
    cg.cluster.main_server.net_box:eval("require('app.roles.storage').stop()")
    seed_expired(cg, 'expired=1')
    put(cg, 'fresh=1')
    t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 2)
    cg.cluster.main_server.net_box:eval("require('app.roles.storage').apply_config(nil, {is_master = true})")
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 1)
    end)
end

g.test_restart_discards_cache_contents = function(cg)
    put(cg, 'restart=1')
    local server = cg.cluster.main_server
    server.net_box:eval('box.snapshot()')
    server:stop()
    server:start()
    cg.cluster:wait_until_healthy(server)
    t.assert_equals(server.net_box:eval('return box.space.forecast_cache.temporary'), true)
    t.assert_equals(server.net_box:eval('return box.space.forecast_cache:count()'), 0)
    t.assert_equals(put(cg, 'restart=1'), true)
end

g.test_schema_initialization_preserves_existing_cache = function(cg)
    put(cg, 'preserve=1')
    call(cg, 'create_schema')
    t.assert_equals(call(cg, 'cache_get', 'preserve=1').headers['x-cache'], {'HIT'})
end

g.test_insertion_errors_are_reported_and_recover = function(cg)
    for _, code in ipairs({'MEMORY_ISSUE', 'ILLEGAL_PARAMS'}) do
        cg.cluster.main_server.net_box:eval([[
            local code = ...
            rawset(_G, 'cache_failure_trigger', function() box.error(box.error[code], 'injected cache failure') end)
            box.space.forecast_cache:before_replace(cache_failure_trigger)
        ]], {code})
        t.assert_error(function() put(cg, 'insertion=' .. code) end)
        t.assert_equals(cg.cluster.main_server.net_box:eval('return box.space.forecast_cache:count()'), 0)
        cg.cluster.main_server.net_box:eval([[
            box.space.forecast_cache:before_replace(nil, cache_failure_trigger)
            rawset(_G, 'cache_failure_trigger', nil)
        ]])
    end
    put(cg, 'recovered=1')
    t.assert_equals(call(cg, 'cache_get', 'recovered=1').headers['x-cache'], {'HIT'})
end

g.test_access_time_failure_serves_cached_response = function(cg)
    put(cg, 'touch=1')
    cg.cluster.main_server.net_box:eval([[
        rawset(_G, 'cache_failure_trigger', function() error('injected touch failure') end)
        box.space.forecast_cache:before_replace(cache_failure_trigger)
    ]])
    local response = call(cg, 'cache_get', 'touch=1')
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'new')
    t.assert_equals(response.headers['x-cache'], {'HIT'})
end

g.test_failed_expired_deletion_returns_miss = function(cg)
    cg.cluster.main_server.net_box:eval([[
        require('app.roles.storage').stop()
        local binary = require('test.fixtures.binary')
        box.space.forecast_cache:replace({'expired-failure=1', 1, 200, binary('stale'), {type = {'text'}}, 0, 0})
        rawset(_G, 'cache_failure_trigger', function() error('injected cache write failure') end)
        box.space.forecast_cache:before_replace(cache_failure_trigger)
    ]])
    t.assert_equals(call(cg, 'cache_get', 'expired-failure=1'), nil)
end

g.test_missing_cache_space_reports_error = function(cg)
    cg.cluster.main_server.net_box:eval([[
        require('app.roles.storage').stop()
        box.space.forecast_cache:drop()
    ]])
    local ok = pcall(call, cg, 'cache_get', 'missing-space=1')
    call(cg, 'create_schema')
    t.assert_equals(ok, false)
end

g.test_cache_hit_updates_access_time = function(cg)
    put(cg, 'touch=1')
    cg.cluster.main_server.net_box:eval([[
        box.space.forecast_cache:update('touch=1', {{'=', 7, 1}})
    ]])
    t.assert_equals(call(cg, 'cache_get', 'touch=1').headers['x-cache'], {'HIT'})
    t.assert(cg.cluster.main_server.net_box:eval([[
        return box.space.forecast_cache:get('touch=1')[7]
    ]]) > 1)
end
