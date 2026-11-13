-- luacheck: globals box.lib.load

-- Keep the module loaded so its process-local configuration survives Lua GC.
local lib
local fiber = require('fiber')
local log = require('log')
local cleanup_task

local function stop_cleanup()
    local task = cleanup_task
    if task == nil then return end
    cleanup_task = nil
    task.running = false
    if task.sleeping then task.fiber:wakeup() end
    local ok, err = task.fiber:join()
    if not ok then log.error('Forecast cleanup stopped with an error: %s', tostring(err)) end
end

local function start_cleanup()
    local cleanup = lib:load('cleanup_expired')
    local evict = lib:load('evict_cache')
    local task = {running = true}
    task.fiber = fiber.new(function()
        local cursor, eviction_cursor, evicting
        local function reset_eviction()
            eviction_cursor, evicting = nil, false
        end
        local function live_bytes()
            local memory = box.info.memory()
            return memory.data + memory.index
        end
        local function expiration_step()
            local batch = cleanup(cursor)
            cursor = batch.next_cursor ~= box.NULL and batch.next_cursor or nil
            return cursor ~= nil
        end
        local function eviction_step()
            local low = math.floor(box.cfg.memtx_memory * 0.7)
            if live_bytes() <= low then
                reset_eviction()
                return false
            end
            local batch = evict(eviction_cursor, low)
            eviction_cursor = batch.next_cursor ~= box.NULL and batch.next_cursor or nil
            if live_bytes() <= low then
                reset_eviction()
            elseif eviction_cursor ~= nil then
                return true
            end
            return false
        end
        local function sleep(delay)
            if not task.running then return end
            task.sleeping = true
            fiber.sleep(delay)
            task.sleeping = false
        end
        while task.running do
            local ok, err = pcall(function()
                while task.running and not box.info.ro do
                    if not expiration_step() then break end
                    sleep(0)
                end
                if not task.running or box.info.ro then return end

                local high = math.floor(box.cfg.memtx_memory * 0.8)
                local low = math.floor(box.cfg.memtx_memory * 0.7)
                local used = live_bytes()
                if used <= low then
                    reset_eviction()
                elseif used >= high then
                    evicting = true
                end
                if evicting then
                    -- Yield between expiration and the first eviction batch.
                    sleep(0)
                    while task.running and not box.info.ro do
                        if not eviction_step() then break end
                        sleep(0)
                    end
                end
            end)
            if not ok then
                log.error('Forecast cleanup failed: %s', tostring(err))
            end
            if not ok or box.info.ro then
                cursor = nil
                reset_eviction()
            end
            -- A completed sweep or eviction cycle pauses before expiration.
            sleep(1)
        end
    end)
    task.fiber:name('forecast.cleanup')
    task.fiber:set_joinable(true)
    cleanup_task = task
end

local function apply_config(_, opts)
    if not opts.is_master then
        stop_cleanup()
        return true
    end
    if cleanup_task ~= nil and cleanup_task.fiber:status() ~= 'dead' then return true end
    stop_cleanup()
    start_cleanup()
    return true
end

local function stop()
    stop_cleanup()
    return true
end

local function init(opts) -- luacheck: no unused args
    lib = lib or box.lib.load('forecast')
    lib:load('init_forecast')()
    if not opts.is_master then return true end
    local create_schema = lib:load('create_schema')
    create_schema()
    box.schema.func.create('forecast.fetch_with_cache', {language = 'C', if_not_exists = true})

    return true
end

return {
    role_name = 'app.roles.storage',
    init = init,
    apply_config = apply_config,
    stop = stop,
    fetch_with_cache = function(query, bucket_id)
        local fetch = lib:load('fetch_with_cache')
        return fetch(query, bucket_id)
    end,
}
