local t = require('luatest')
local fio = require('fio')
local cartridge_helpers = require('cartridge.test-helpers')
local helper = require('test.helper')
local g = t.group('integration_cleanup')

local function eval(cg, code, args)
    return cg.cluster.main_server.net_box:eval(code, args)
end

g.before_all(function(cg)
    cg.mock = require('http.server').new('127.0.0.1', 0, {log_requests = false})
    cg.mock:route({method = 'GET', path = '/forecast'}, function()
        return {status = 200, body = 'promoted'}
    end)
    cg.mock:start()
    cg.cluster = cartridge_helpers.Cluster:new({
        server_command = helper.server_command,
        datadir = fio.pathjoin(helper.datadir, 'cleanup'),
        base_http_port = 58080,
        base_advertise_port = 58300,
        use_vshard = false,
        env = {OPEN_METEO_FORECAST_URL = 'http://127.0.0.1:' .. cg.mock.tcp_server:name().port .. '/forecast'},
        replicasets = {{alias = 'cleanup', roles = {'app.roles.storage'}, servers = {
            {alias = 'cleanup'}, {alias = 'cleanup-replica'},
        }}},
    })
    cg.cluster:start()
end)

g.after_all(function(cg)
    helper.stop_cluster(cg.cluster)
    cg.mock:stop()
end)

g.before_each(function(cg)
    cg.cluster.servers[2].net_box:eval("require('app.roles.storage').stop()")
    eval(cg, [[
        require('app.roles.storage').stop()
        box.space.forecast_cache:truncate()
        local storage = require('vshard').storage
        rawset(_G, 'cleanup_test_state', {ref = storage.bucket_refrw, unref = storage.bucket_unrefrw})
    ]])
end)

g.after_each(function(cg)
    if cg.promoted then
        cg.cluster.main_server:graphql({
            query = [[mutation($rs: String!, $leader: String!) {
                cluster { edit_topology(replicasets: [{uuid: $rs, failover_priority: [$leader]}]) { servers { uri } } }
            }]],
            variables = {rs = cg.cluster.servers[1].replicaset_uuid, leader = cg.cluster.servers[1].instance_uuid},
        })
        cg.promoted = false
    end
    eval(cg, [[
        box.cfg({read_only = false})
        local role = require('app.roles.storage')
        role.stop()
        local state = cleanup_test_state
        local storage = require('vshard').storage
        storage.bucket_refrw, storage.bucket_unrefrw = state.ref, state.unref
        if state.trigger then box.space.forecast_cache:before_replace(nil, state.trigger) end
        if box.space._bucket then box.space._bucket:drop() end
        box.space.forecast_cache:truncate()
        role.apply_config(nil, {is_master = true})
    ]])
end)

g.test_batch_bound_and_continuation = function(cg)
    local result = eval(cg, [[
        local space = box.space.forecast_cache
        for i = 1, 250 do
            space:replace({string.format('%03d', i), 1, 200, 'old', {type = 'text'}, 0, 0})
        end
        space:replace({'fresh', 1, 200, 'new', {type = 'text'}, 9007199254740991, 0})
        local lib = box.lib.load('forecast')
        local cleanup = lib:load('cleanup_expired')
        local first = cleanup(nil)
        local second = cleanup(first.next_cursor)
        local third = cleanup(second.next_cursor)
        return {first = first, second = second, third = third, count = space:count(), fresh = space:get('fresh')[4]}
    ]])
    t.assert_equals(result.first.scanned, 100)
    t.assert_equals(result.first.deleted, 100)
    t.assert_equals(result.second.scanned, 100)
    t.assert_equals(result.second.deleted, 100)
    t.assert_equals(result.third.scanned, 50)
    t.assert_equals(result.third.deleted, 50)
    t.assert_equals(result.third.next_cursor, box.NULL)
    t.assert_equals(result.count, 1)
    t.assert_equals(result.fresh, 'new')
end

g.test_read_only_pauses_cleanup = function(cg)
    local result = eval(cg, [[
        local role = require('app.roles.storage')
        local space = box.space.forecast_cache
        space:replace({'readonly', 1, 200, 'old', {type = 'text'}, 0, 0})
        box.cfg({read_only = true})
        role.apply_config(nil, {is_master = true})
        require('fiber').sleep(0.1)
        local count = space:count()
        local lib = box.lib.load('forecast')
        local batch = lib:load('cleanup_expired')(nil)
        box.cfg({read_only = false})
        return {count = count, deleted = batch.deleted}
    ]])
    t.assert_equals(result, {count = 1, deleted = 0})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(eval(cg, 'return box.space.forecast_cache:count()'), 0)
    end)
end

g.test_background_recovers_after_cleanup_error = function(cg)
    eval(cg, [[
        local space = box.space.forecast_cache
        space:replace({'retry', 1, 200, 'old', {type = 'text'}, 0, 0})
        cleanup_test_state.trigger = function(_, new)
            if new == nil then error('temporary cleanup error') end
        end
        space:before_replace(cleanup_test_state.trigger)
        require('app.roles.storage').apply_config(nil, {is_master = true})
        require('fiber').sleep(0.1)
        assert(space:count() == 1)
        space:before_replace(nil, cleanup_test_state.trigger)
        cleanup_test_state.trigger = nil
    ]])
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(eval(cg, 'return box.space.forecast_cache:count()'), 0)
    end)
end

g.test_real_promotion_moves_cleanup_to_new_master = function(cg)
    eval(cg, [[
        box.space.forecast_cache:replace({'promotion', 1, 200, 'old', {type = 'text'}, 0, 0})
    ]])
    local replica = cg.cluster.servers[2]
    t.assert_equals(replica.net_box:eval("return box.space.forecast_cache:get('promotion')"), nil)
    cg.promoted = true
    cg.cluster.main_server:graphql({
        query = [[mutation($rs: String!, $leader: String!) {
            cluster { edit_topology(replicasets: [{uuid: $rs, failover_priority: [$leader]}]) { servers { uri } } }
        }]],
        variables = {rs = replica.replicaset_uuid, leader = replica.instance_uuid},
    })
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(eval(cg, 'return box.info.ro'), true)
        t.assert_equals(replica.net_box:eval('return box.info.ro'), false)
        t.assert_equals(replica.net_box:eval('return box.space.forecast_cache:count()'), 0)
        local count = [[
            local n = 0
            for _, info in pairs(require('fiber').info()) do
                if info.name == 'forecast.cleanup' then n = n + 1 end
            end
            return n
        ]]
        t.assert_equals(eval(cg, count), 0)
        t.assert_equals(replica.net_box:eval(count), 1)
    end)
    local response = replica.net_box:eval([[
        local lib = box.lib.load('forecast')
        local fetch = lib:load('fetch_with_cache')
        local response = fetch('promotion-hit', 1)
        assert(response.status == 200 and response.headers['x-cache'] == nil)
        return fetch('promotion-hit', 1)
    ]])
    t.assert_equals(response.body, 'promoted')
    t.assert_equals(response.headers['x-cache'], 'HIT')
end

-- Stub only the vshard reference calls to make otherwise hard-to-time races deterministic.
local function sharded_setup()
    return [[
        local space = box.space.forecast_cache
        box.schema.space.create('_bucket')
        local state = cleanup_test_state
        state.held, state.released = 0, 0
        local storage = require('vshard').storage
        storage.bucket_unrefrw = function()
            state.held = state.held - 1
            state.released = state.released + 1
            return true
        end
        local lib = box.lib.load('forecast')
        local cleanup = lib:load('cleanup_expired')
    ]]
end

g.test_skipped_buckets_do_not_starve_later_candidates = function(cg)
    local result = eval(cg, sharded_setup() .. [[
        for i = 1, 150 do
            space:replace({string.format('%03d', i), i <= 100 and 1 or 2, 200, 'old', {type = 'text'}, 0, 0})
        end
        storage.bucket_refrw = function(id)
            if id == 1 then return nil, 'moving' end
            state.held = state.held + 1
            return true
        end
        local first = cleanup(nil)
        local second = cleanup(first.next_cursor)
        return {first = first, second = second, count = space:count(), held = state.held, released = state.released}
    ]])
    t.assert_equals(result.first.scanned, 100)
    t.assert_equals(result.first.deleted, 0)
    t.assert_equals(result.second.deleted, 50)
    t.assert_equals(result.count, 100)
    t.assert_equals(result.held, 0)
    t.assert_equals(result.released, 50)
end

g.test_batch_does_not_yield = function(cg)
    local result = eval(cg, [[
        local space = box.space.forecast_cache
        for i = 1, 100 do
            space:replace({tostring(i), 1, 200, 'old', {type = 'text'}, 0, 0})
        end
        local fiber = require('fiber')
        local before = assert(fiber.info()[fiber.self():id()].csw)
        local lib = box.lib.load('forecast')
        local batch = lib:load('cleanup_expired')(nil)
        local after = assert(fiber.info()[fiber.self():id()].csw)
        return {deleted = batch.deleted, yielded = after ~= before}
    ]])
    t.assert_equals(result, {deleted = 100, yielded = false})
end

g.test_delete_error_releases_reference = function(cg)
    local result = eval(cg, sharded_setup() .. [[
        space:replace({'failure', 1, 200, 'old', {type = 'text'}, 0, 0})
        storage.bucket_refrw = function() state.held = state.held + 1; return true end
        state.trigger = function(_, new)
            if new == nil then error('injected delete failure') end
        end
        space:before_replace(state.trigger)
        local ok, err = pcall(cleanup, nil)
        return {ok = ok, error = tostring(err), held = state.held, released = state.released}
    ]])
    t.assert_equals(result.ok, false)
    t.assert_str_contains(result.error, 'injected delete failure')
    t.assert_equals(result.held, 0)
    t.assert_equals(result.released, 1)
end

g.test_demotion_during_cleanup_stops_batch_quietly = function(cg)
    local result = eval(cg, sharded_setup() .. [[
        for _, key in ipairs({'first', 'second'}) do
            space:replace({key, 1, 200, 'old', {type = 'text'}, 0, 0})
        end
        state.trigger = function(_, new)
            if new == nil then error('injected deletion failure after demotion') end
        end
        space:before_replace(state.trigger)
        state.acquired = 0
        storage.bucket_refrw = function()
            state.held = state.held + 1
            state.acquired = state.acquired + 1
            box.cfg({read_only = true})
            return true
        end
        local batch = cleanup(nil)
        return {deleted = batch.deleted, count = space:count(), acquired = state.acquired,
            held = state.held, released = state.released}
    ]])
    t.assert_equals(result, {deleted = 0, count = 2, acquired = 1, held = 0, released = 1})
end

g.test_release_error_is_not_hidden_by_demotion = function(cg)
    local result = eval(cg, sharded_setup() .. [[
        space:replace({'failure', 1, 200, 'old', {type = 'text'}, 0, 0})
        storage.bucket_refrw = function()
            state.held = state.held + 1
            box.cfg({read_only = true})
            return true
        end
        storage.bucket_unrefrw = function()
            state.held = state.held - 1
            state.released = state.released + 1
            return nil, 'injected release failure'
        end
        local ok, err = pcall(cleanup, nil)
        return {ok = ok, error = tostring(err), held = state.held, released = state.released}
    ]])
    t.assert_equals(result.ok, false)
    t.assert_str_contains(result.error, 'injected release failure')
    t.assert_equals(result.held, 0)
    t.assert_equals(result.released, 1)
end

g.test_lifecycle_and_read_only = function(cg)
    local result = eval(cg, [[
        local role = require('app.roles.storage')
        local fiber = require('fiber')
        local function count()
            local n = 0
            for _, info in pairs(fiber.info()) do
                if info.name == 'forecast.cleanup' then n = n + 1 end
            end
            return n
        end
        box.cfg({read_only = true})
        role.apply_config(nil, {is_master = true})
        role.apply_config(nil, {is_master = true})
        local active = count()
        role.apply_config(nil, {is_master = false})
        local demoted = count()
        box.cfg({read_only = false})
        role.apply_config(nil, {is_master = true})
        local promoted = count()
        role.stop()
        return {active = active, demoted = demoted, promoted = promoted, stopped = count()}
    ]])
    t.assert_equals(result, {active = 1, demoted = 0, promoted = 1, stopped = 0})
end

g.test_backlog_yields_and_drains_in_background = function(cg)
    local result = eval(cg, [[
        local role = require('app.roles.storage')
        local fiber = require('fiber')
        local space = box.space.forecast_cache
        for i = 1, 350 do space:replace({tostring(i), 1, 200, 'old', {type = 'text'}, 0, 0}) end
        local ticks = 0
        local running = true
        local heartbeat = fiber.new(function() while running do ticks = ticks + 1; fiber.sleep(0) end end)
        heartbeat:set_joinable(true)
        role.apply_config(nil, {is_master = true})
        local deadline = fiber.clock() + 5
        while space:count() > 0 and fiber.clock() < deadline do fiber.sleep(0.01) end
        role.stop()
        running = false
        heartbeat:join()
        return {count = space:count(), ticks = ticks}
    ]])
    t.assert_equals(result.count, 0)
    t.assert(result.ticks > 1)
end
