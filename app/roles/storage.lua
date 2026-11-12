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

local function apply_config(_, opts)
    if not opts.is_master then
        stop_cleanup()
        return true
    end
    if cleanup_task ~= nil and cleanup_task.fiber:status() ~= 'dead' then return true end
    stop_cleanup()
    local cleanup = lib:load('cleanup_expired')
    local task = {running = true}
    task.fiber = fiber.new(function()
        local cursor
        while task.running do
            local delay = 1
            if not box.info.ro then
                local ok, batch = pcall(cleanup, cursor)
                if ok then
                    cursor = batch.next_cursor
                    if cursor == box.NULL then cursor = nil end
                    if cursor ~= nil then delay = 0 end
                else
                    cursor = nil
                    log.error('Forecast cleanup failed: %s', tostring(batch))
                end
            else
                cursor = nil
            end
            if task.running then
                task.sleeping = true
                fiber.sleep(delay)
                task.sleeping = false
            end
        end
    end)
    task.fiber:name('forecast.cleanup')
    task.fiber:set_joinable(true)
    cleanup_task = task
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
