local CACHE_RPC_PROC = 'forecast.rpc_handler'
local CONFIG_PROC = 'forecast.storage_apply_config'

local function init(opts)
    if not opts.is_master then
        return true
    end

    local name = 'forecast.storage_init'
    box.schema.func.create(name, {language = 'C', if_not_exists = true})
    box.func[name]:call()
    box.schema.func.create(CACHE_RPC_PROC, {language = 'C', if_not_exists = true})
    box.schema.func.create(CONFIG_PROC, {language = 'C', if_not_exists = true})

    rawset(_G, 'rpc_handler', function(path, ctx, mp_request)
        return box.func[CACHE_RPC_PROC]:call({path, ctx, mp_request})
    end)
    return true
end

local function stop()
    return true
end

local function validate_config(conf_new, conf_old) -- luacheck: no unused args
    local ok, err = pcall(function()
        -- Cartridge can validate before box.cfg(), so load the validator without schema registration.
        local module = box.lib.load('forecast')
        return module:load('storage_validate_config')(conf_new.forecast_storage)
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
        conf.forecast_storage == nil and box.NULL or conf.forecast_storage,
    })
end

return {
    role_name = 'app.roles.storage',
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    -- dependencies = {'cartridge.roles.vshard-router'},
}
