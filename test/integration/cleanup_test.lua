local t = require('luatest')
local fio = require('fio')
local cartridge_helpers = require('cartridge.test-helpers')
local helper = require('test.helper')
local g = t.group('integration_cleanup')

local function eval(cg, code, args)
    return cg.cluster.main_server.net_box:eval("local binary = require('test.fixtures.binary'); " .. code, args)
end

g.before_all(function(cg)
    cg.cluster = cartridge_helpers.Cluster:new({
        server_command = helper.server_command,
        datadir = fio.pathjoin(helper.datadir, 'cleanup'),
        base_http_port = 58080,
        base_advertise_port = 58300,
        use_vshard = false,
        replicasets = {{alias = 'cleanup', roles = {'app.roles.storage'}, servers = {
            {alias = 'cleanup'}, {alias = 'cleanup-replica'},
        }}},
    })
    cg.cluster:start()
end)

g.after_all(function(cg)
    helper.stop_cluster(cg.cluster)
end)

g.before_each(function(cg)
    cg.cluster.servers[2].net_box:eval("require('app.roles.storage').stop()")
    eval(cg, [[
        require('app.roles.storage').stop()
        box.space.forecast_cache:truncate()
        local storage = require('vshard').storage
        rawset(_G, 'cleanup_test_state', {ref = storage.bucket_refrw, unref = storage.bucket_unrefrw,
            memory = box.info.memory, slab = box.slab.info})
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
        box.info.memory = state.memory
        box.slab.info = state.slab
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
            space:replace({string.format('%03d', i), 1, 200, binary('old'), {type = {'text'}}, 0, 0})
        end
        space:replace({'fresh', 1, 200, binary('new'), {type = {'text'}}, 9007199254740991, 0})
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
        space:replace({'readonly', 1, 200, binary('old'), {type = {'text'}}, 0, 0})
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
        space:replace({'retry', 1, 200, binary('old'), {type = {'text'}}, 0, 0})
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
        box.space.forecast_cache:replace({'promotion', 1, 200, binary('old'), {type = {'text'}}, 0, 0})
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
        lib:load('cache_put')('promotion-hit', 1, {status = 200, body = 'promoted', headers = {type = {'text'}}})
        return lib:load('cache_get')('promotion-hit')
    ]])
    t.assert_equals(response.body, 'promoted')
    t.assert_equals(response.headers['x-cache'], {'HIT'})
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
            space:replace({string.format('%03d', i), i <= 100 and 1 or 2, 200, binary('old'), {type = {'text'}}, 0, 0})
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
            space:replace({tostring(i), 1, 200, binary('old'), {type = {'text'}}, 0, 0})
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
        space:replace({'failure', 1, 200, binary('old'), {type = {'text'}}, 0, 0})
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
            space:replace({key, 1, 200, binary('old'), {type = {'text'}}, 0, 0})
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
        space:replace({'failure', 1, 200, binary('old'), {type = {'text'}}, 0, 0})
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
        for i = 1, 350 do space:replace({tostring(i), 1, 200, binary('old'), {type = {'text'}}, 0, 0}) end
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

g.test_eviction_batches_and_lru_order = function(cg)
    local result = eval(cg, [[
        local space = box.space.forecast_cache
        for i = 1, 250 do
            space:replace({string.format('%03d=', i), 1, 200, binary('fresh'), {type = {'text'}}, 9007199254740991, i})
        end
        local lib = box.lib.load('forecast')
        -- Accessing the oldest entry moves it to the end of the LRU index.
        lib:load('cache_get')('001')
        local evict = lib:load('evict_cache')
        local fiber = require('fiber')
        local before = fiber.info()[fiber.self():id()].csw
        local first = evict(nil, 0)
        local after = fiber.info()[fiber.self():id()].csw
        local retained = space:get('001=') ~= nil and space:get('102=') ~= nil
        local second = evict(first.next_cursor, 0)
        local third = evict(second.next_cursor, 0)
        return {first = first, second = second, third = third,
            retained = retained, count = space:count(), yielded = before ~= after}
    ]])
    t.assert_equals(result.first.scanned, 100)
    t.assert_equals(result.first.deleted, 100)
    t.assert_equals(result.first.next_cursor, {101, '101='})
    t.assert_equals(result.second.scanned, 100)
    t.assert_equals(result.third.scanned, 50)
    t.assert_equals(result.third.next_cursor, box.NULL)
    t.assert_equals(result.retained, true)
    t.assert_equals(result.count, 0)
    t.assert_equals(result.yielded, false)
end

g.test_eviction_releases_memory_and_stops_at_target = function(cg)
    local result = eval(cg, [[
        local space = box.space.forecast_cache
        local function used() local m = box.info.memory(); return m.data + m.index end
        local baseline = used()
        for i = 1, 40 do
            space:replace({tostring(i), 1, 200, binary(string.rep('x', 32768)), {type = {'text'}}, 9007199254740991, i})
        end
        collectgarbage('collect')
        local before = used()
        local slab = box.slab.info()
        assert(before == slab.arena_used)
        local lib = box.lib.load('forecast')
        local target = math.floor((baseline + before) / 2)
        local batch = lib:load('evict_cache')(nil, target)
        return {before = before, after = used(), target = target, deleted = batch.deleted, count = space:count()}
    ]])
    t.assert(result.after < result.before)
    t.assert(result.after <= result.target)
    t.assert(result.deleted > 0 and result.deleted < 40)
    t.assert(result.count > 0)
end

g.test_eviction_skips_buckets_and_retries_next_sweep = function(cg)
    local result = eval(cg, sharded_setup() .. [[
        for i = 1, 150 do
            space:replace({string.format('%03d', i), i <= 100 and 1 or 2, 200, binary('fresh'),
                {type = {'text'}}, 9007199254740991, i})
        end
        storage.bucket_refrw = function(id)
            if id == 1 then return nil, 'moving' end
            state.held = state.held + 1
            return true
        end
        local evict = lib:load('evict_cache')
        local first = evict(nil, 0)
        local second = evict(first.next_cursor, 0)
        storage.bucket_refrw = function() state.held = state.held + 1; return true end
        local retry = evict(nil, 0)
        return {first = first, second = second, retry = retry, held = state.held, released = state.released}
    ]])
    t.assert_equals(result.first.scanned, 100)
    t.assert_equals(result.first.deleted, 0)
    t.assert_equals(result.second.deleted, 50)
    t.assert_equals(result.retry.deleted, 100)
    t.assert_equals(result.held, 0)
    t.assert_equals(result.released, 150)
end

g.test_eviction_error_releases_reference = function(cg)
    local result = eval(cg, sharded_setup() .. [[
        space:replace({'evict-error', 1, 200, binary('fresh'), {type = {'text'}}, 9007199254740991, 0})
        storage.bucket_refrw = function() state.held = state.held + 1; return true end
        state.trigger = function() error('injected eviction error') end
        space:before_replace(state.trigger)
        local ok, err = pcall(lib:load('evict_cache'), nil, 0)
        return {ok = ok, error = tostring(err), held = state.held, released = state.released}
    ]])
    t.assert_equals(result.ok, false)
    t.assert_str_contains(result.error, 'injected eviction error')
    t.assert_equals(result.held, 0)
    t.assert_equals(result.released, 1)
end

g.test_eviction_hysteresis_and_fiber_progress = function(cg)
    local result = eval(cg, [[
        local role = require('app.roles.storage')
        local fiber = require('fiber')
        local space = box.space.forecast_cache
        for i = 1, 200 do
            space:replace({tostring(i), 1, 200, binary('fresh'), {type = {'text'}}, 9007199254740991, i})
        end
        local high = math.floor(box.cfg.memtx_memory * 0.8)
        local low = math.floor(box.cfg.memtx_memory * 0.7)
        local phase, middle = 1, false
        box.slab.info = function()
            local memory = box.info.memory()
            return {arena_used = memory.data + memory.index, items_used = 0}
        end
        box.info.memory = function()
            local count = space:count()
            if count == 100 then middle = true end
            local used = phase == 1 and high - 1 or low + math.ceil((count - 50) * (high - low) / 150)
            if phase == 3 then used = math.floor(box.cfg.memtx_memory * 0.75) end
            return {data = used, index = 0}
        end
        local ticks, running = 0, true
        local heartbeat = fiber.new(function() while running do ticks = ticks + 1; fiber.sleep(0) end end)
        heartbeat:set_joinable(true)
        role.apply_config(nil, {is_master = true})
        fiber.sleep(0.05)
        local below_high = space:count()
        phase = 2
        local deadline = fiber.clock() + 4
        while space:count() > 50 and fiber.clock() < deadline do fiber.sleep(0.01) end
        local at_low = space:count()
        phase = 3
        fiber.sleep(1.1)
        role.stop()
        running = false
        heartbeat:join()
        return {below_high = below_high, at_low = at_low, after = space:count(), ticks = ticks, middle = middle}
    ]])
    t.assert_equals(result.below_high, 200)
    t.assert_equals(result.at_low, 50)
    t.assert_equals(result.after, 50)
    t.assert_equals(result.middle, true)
    t.assert(result.ticks > 1)
end

g.test_unreclaimable_eviction_sleeps_and_recovers = function(cg)
    local result = eval(cg, sharded_setup() .. [[
        space:replace({'locked', 1, 200, binary('fresh'), {type = {'text'}}, 9007199254740991, 0})
        local attempts, available = 0, false
        storage.bucket_refrw = function()
            attempts = attempts + 1
            if not available then return nil, 'moving' end
            state.held = state.held + 1
            return true
        end
        box.slab.info = function()
            local memory = box.info.memory()
            return {arena_used = memory.data + memory.index, items_used = 0}
        end
        box.info.memory = function() return {data = box.cfg.memtx_memory, index = 0} end
        local role = require('app.roles.storage')
        local fiber = require('fiber')
        role.apply_config(nil, {is_master = true})
        fiber.sleep(0.1)
        local blocked = space:count()
        local initial = attempts
        available = true
        fiber.sleep(1.1)
        role.stop()
        return {initial = initial, attempts = attempts, blocked = blocked,
            count = space:count(), held = state.held, released = state.released}
    ]])
    t.assert_equals(result.initial, 1)
    t.assert_equals(result.attempts, 2)
    t.assert_equals(result.blocked, 1)
    t.assert_equals(result.count, 0)
    t.assert_equals(result.held, 0)
    t.assert_equals(result.released, 1)
end

g.test_eviction_read_only_and_demotion_reset_hysteresis = function(cg)
    local result = eval(cg, sharded_setup() .. [[
        local role = require('app.roles.storage')
        local fiber = require('fiber')
        space:replace({'retained', 1, 200, binary('fresh'), {type = {'text'}}, 9007199254740991, 0})
        box.slab.info = function()
            local memory = box.info.memory()
            return {arena_used = memory.data + memory.index, items_used = 0}
        end
        box.info.memory = function() return {data = box.cfg.memtx_memory, index = 0} end
        box.cfg({read_only = true})
        local lib = box.lib.load('forecast')
        local readonly = lib:load('evict_cache')(nil, 0)
        role.apply_config(nil, {is_master = true})
        fiber.sleep(0.05)
        local retained = space:count()
        role.stop()
        box.cfg({read_only = false})
        storage.bucket_refrw = function() return nil, 'moving' end
        role.apply_config(nil, {is_master = true})
        fiber.sleep(0.05) -- Enter eviction mode, but retain the unavailable bucket.
        role.apply_config(nil, {is_master = false})
        storage.bucket_refrw = function() state.held = state.held + 1; return true end
        box.info.memory = function() return {data = math.floor(box.cfg.memtx_memory * 0.75), index = 0} end
        role.apply_config(nil, {is_master = true})
        role.apply_config(nil, {is_master = true})
        fiber.sleep(0.05)
        role.stop()
        return {readonly = readonly.deleted, retained = retained, after = space:count()}
    ]])
    t.assert_equals(result, {readonly = 0, retained = 1, after = 1})
end

g.test_eviction_error_background_retry = function(cg)
    eval(cg, [[
        local space = box.space.forecast_cache
        space:replace({'retry-evict', 1, 200, binary('fresh'), {type = {'text'}}, 9007199254740991, 0})
        box.slab.info = function()
            local memory = box.info.memory()
            return {arena_used = memory.data + memory.index, items_used = 0}
        end
        box.info.memory = function() return {data = box.cfg.memtx_memory, index = 0} end
        cleanup_test_state.trigger = function() error('temporary eviction error') end
        space:before_replace(cleanup_test_state.trigger)
        require('app.roles.storage').apply_config(nil, {is_master = true})
        require('fiber').sleep(0.1)
        assert(space:count() == 1)
        space:before_replace(nil, cleanup_test_state.trigger)
        cleanup_test_state.trigger = nil
    ]])
    t.helpers.retrying({timeout = 4}, function()
        t.assert_equals(eval(cg, 'return box.space.forecast_cache:count()'), 0)
    end)
end

g.test_eviction_cursor_handles_equal_access_times = function(cg)
    local result = eval(cg, [[
        local space = box.space.forecast_cache
        for i = 1, 101 do
            space:replace({string.format('%03d', i), 1, 200, binary('fresh'), {type = {'text'}}, 9007199254740991, 7})
        end
        local lib = box.lib.load('forecast')
        local evict = lib:load('evict_cache')
        local first = evict(nil, 0)
        local second = evict(first.next_cursor, 0)
        return {cursor = first.next_cursor, deleted = first.deleted + second.deleted, count = space:count()}
    ]])
    t.assert_equals(result, {cursor = {7, '100'}, deleted = 101, count = 0})
end

g.test_expiration_sweep_relieves_pressure_before_evicting_fresh_entries = function(cg)
    local result = eval(cg, [[
        local space = box.space.forecast_cache
        for i = 1, 250 do
            space:replace({'expired-' .. i, 1, 200, binary('expired'), {type = {'text'}}, 0, 1000 + i})
        end
        for i = 1, 10 do
            space:replace({'fresh-' .. i, 1, 200, binary('fresh'), {type = {'text'}}, 9007199254740991, i})
        end
        local function expired() return space.index.expires_at:count({0}) end
        box.info.memory = function()
            local ratio = expired() > 0 and 0.8 or 0.7
            return {data = math.floor(box.cfg.memtx_memory * ratio), index = 0}
        end
        box.slab.info = function()
            return {arena_used = box.info.memory().data, items_used = 0}
        end
        local role = require('app.roles.storage')
        local fiber = require('fiber')
        role.apply_config(nil, {is_master = true})
        local deadline = fiber.clock() + 4
        while expired() > 0 and fiber.clock() < deadline do fiber.sleep(0.01) end
        role.stop()
        return {expired = expired(), count = space:count()}
    ]])
    t.assert_equals(result, {expired = 0, count = 10})
end

g.test_unavailable_expired_entries_do_not_prevent_eviction = function(cg)
    local result = eval(cg, sharded_setup() .. [[
        for i = 1, 100 do
            space:replace({'expired-' .. i, 1, 200, binary('expired'), {type = {'text'}}, 0, i + 100})
        end
        space:replace({'fresh', 2, 200, binary('fresh'), {type = {'text'}}, 9007199254740991, 0})
        storage.bucket_refrw = function(id)
            if id == 1 then return nil, 'moving' end
            state.held = state.held + 1
            return true
        end
        box.info.memory = function()
            local ratio = space:get('fresh') ~= nil and 0.8 or 0.7
            return {data = math.floor(box.cfg.memtx_memory * ratio), index = 0}
        end
        box.slab.info = function()
            return {arena_used = box.info.memory().data, items_used = 0}
        end
        local role = require('app.roles.storage')
        local fiber = require('fiber')
        role.apply_config(nil, {is_master = true})
        local deadline = fiber.clock() + 4
        while space:get('fresh') ~= nil and fiber.clock() < deadline do fiber.sleep(0.01) end
        role.stop()
        return {count = space:count(), fresh = space:get('fresh') ~= nil,
            held = state.held, released = state.released}
    ]])
    t.assert_equals(result, {count = 100, fresh = false, held = 0, released = 1})
end

g.test_eviction_phase_does_not_restart_expiration_between_batches = function(cg)
    local result = eval(cg, [[
        local space = box.space.forecast_cache
        for i = 1, 350 do
            space:replace({string.format('fresh-%03d', i), 1, 200, binary('fresh'),
                {type = {'text'}}, 9007199254740991, i})
        end
        local inserted = false
        cleanup_test_state.trigger = function(old, new)
            if old ~= nil and new == nil and not inserted then
                inserted = true
                -- Simulate an entry expiring after the initial expiration sweep.
                space:replace({'expired-during-eviction', 1, 200, binary('expired'), {type = {'text'}}, 0, 10000})
            end
        end
        space:before_replace(cleanup_test_state.trigger)
        local function fresh_count()
            return space:count() - (space:get('expired-during-eviction') ~= nil and 1 or 0)
        end
        local high = math.floor(box.cfg.memtx_memory * 0.8)
        local low = math.floor(box.cfg.memtx_memory * 0.7)
        box.info.memory = function()
            return {data = low + math.ceil((fresh_count() - 100) * (high - low) / 250), index = 0}
        end
        box.slab.info = function()
            return {arena_used = box.info.memory().data, items_used = 0}
        end
        local fiber = require('fiber')
        local role = require('app.roles.storage')
        role.apply_config(nil, {is_master = true})
        local deadline = fiber.clock() + 4
        while fresh_count() > 100 and fiber.clock() < deadline do fiber.sleep(0.01) end
        local between_phases = space:get('expired-during-eviction') ~= nil
        deadline = fiber.clock() + 3
        while space:get('expired-during-eviction') ~= nil and fiber.clock() < deadline do fiber.sleep(0.01) end
        role.stop()
        return {between_phases = between_phases, fresh = fresh_count(),
            expired = space:get('expired-during-eviction') ~= nil}
    ]])
    t.assert_equals(result, {between_phases = true, fresh = 100, expired = false})
end
