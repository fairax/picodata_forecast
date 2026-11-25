local CACHE_RPC_PROC = 'forecast.rpc_handler'

local function init(opts)
    if not opts.is_master then
        return true
    end
    
    local name = 'forecast.storage_init'
    box.schema.func.create(name, {language = 'C', if_not_exists = true})
    box.func[name]:call()
    box.schema.func.create(CACHE_RPC_PROC, {language = 'C', if_not_exists = true})

    rawset(_G, 'rpc_handler', function(path, ctx, mp_request)
        return box.func[CACHE_RPC_PROC]:call({path, ctx, mp_request})
    end)
    return true
end

local function stop()
    return true
end

local function validate_config(conf_new, conf_old) -- luacheck: no unused args
    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    -- if opts.is_master then
    -- end

    return true
end

return {
    role_name = 'app.roles.storage',
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    -- dependencies = {'cartridge.roles.vshard-router'},
}
