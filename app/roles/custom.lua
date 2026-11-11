local cartridge = require('cartridge')

local function init(opts) -- luacheck: no unused args
    -- if opts.is_master then
    -- end

    local httpd = assert(cartridge.service_get('httpd'), "Failed to get httpd service")
    httpd:route({method = 'GET', path = '/forecast'}, function(request)
        local lib = box.lib.load('forecast') -- luacheck: ignore 143
        local normalize = lib:load('normalize_query')
        local query = normalize(request.query or '')
        local router = require('vshard').router
        local bucket_id = router.bucket_id_strcrc32(query)
        local response, err = router.callrw(
            bucket_id, 'forecast.fetch_with_cache', {query, bucket_id}, {timeout = 35}
        )
        if response == nil then
            require('log').error('Forecast request failed: %s', tostring(err))
            return {status = 502, body = 'Forecast service unavailable'}
        end

        -- The HTTP server supplies framing for the returned body.
        local headers = response.headers or {}
        headers['content-length'] = nil
        headers['transfer-encoding'] = nil
        headers.connection = nil
        return {status = response.status, body = response.body, headers = headers}
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
    role_name = 'app.roles.custom',
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    -- dependencies = {'cartridge.roles.vshard-router'},
}
