local INIT_PROC = 'forecast.router_init'
local CONFIG_PROC = 'forecast.router_apply_config'

local function init(opts)
    if not opts.is_master then
        return true
    end
    box.schema.func.create(INIT_PROC, {language = 'C', if_not_exists = true})
    box.schema.func.create(CONFIG_PROC, {language = 'C', if_not_exists = true})
    return box.func[INIT_PROC]:call()
end

local function stop()
    return true
end

local function validate_config(conf_new, conf_old) -- luacheck: no unused args
    local ok, err = pcall(function()
        -- Cartridge can validate before box.cfg(), so load the validator without schema registration.
        local module = box.lib.load('forecast')
        return module:load('router_validate_config')(conf_new.forecast_router)
    end)
    if not ok then
        return nil, tostring(err)
    end
    return true
end

local function apply_config(conf, opts)
    if not opts.is_master then
        return true
    end
    return box.func[CONFIG_PROC]:call({
        conf.forecast_router == nil and box.NULL or conf.forecast_router,
    })
end

return {
    role_name = 'app.roles.router',
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    -- dependencies = {'cartridge.roles.vshard-router'},
}
