-- luacheck: globals box.lib.load

-- Keep the module loaded so its process-local configuration survives Lua GC.
local lib

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
    fetch_with_cache = function(query, bucket_id)
        local fetch = lib:load('fetch_with_cache')
        return fetch(query, bucket_id)
    end,
}
