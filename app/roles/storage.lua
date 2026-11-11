-- luacheck: globals box.lib.load

local function init(opts) -- luacheck: no unused args
    if not opts.is_master then return true end
    local lib = box.lib.load('forecast')
    local create_schema = lib:load('create_schema')
    create_schema()
    box.schema.func.create('forecast.fetch_with_cache', {language = 'C', if_not_exists = true})

    return true
end

return {
    role_name = 'app.roles.storage',
    init = init,
    fetch_with_cache = function(query, bucket_id)
        local lib = box.lib.load('forecast')
        local fetch = lib:load('fetch_with_cache')
        return fetch(query, bucket_id)
    end,
}
