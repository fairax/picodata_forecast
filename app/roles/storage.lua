local cartridge = require('cartridge')

local function init(opts) -- luacheck: no unused args
    if not opts.is_master then return true end
    local lib = box.lib.load('forecast')
    local create_schema = lib:load('create_schema')
    create_schema()

    return true
end

return {
    role_name = 'app.roles.storage',
    init = init,
}
