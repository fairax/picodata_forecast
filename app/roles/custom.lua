-- luacheck: globals box.lib.load

local cartridge = require('cartridge')
local log = require('log')

-- Retain the module so its process-local configuration survives Lua GC.
local lib

local function cache_get(query, bucket_id)
    local ok, response, err = pcall(require('vshard').router.callrw,
        bucket_id, 'forecast.cache_get', {query}, {timeout = 1})
    if not ok then
        log.error('Forecast cache lookup failed: %s', tostring(response))
        return nil
    end
    if err ~= nil then log.error('Forecast cache lookup failed: %s', tostring(err)) end
    return response
end

local function cache_put(query, bucket_id, response)
    local ok, result, err = pcall(require('vshard').router.callrw,
        bucket_id, 'forecast.cache_put', {query, bucket_id, response}, {timeout = 1})
    if not ok then
        log.error('Forecast cache insertion failed: %s', tostring(result))
        return nil
    end
    if err ~= nil then log.error('Forecast cache insertion failed: %s', tostring(err)) end
    return result
end

local function fetch(query)
    query = lib:load('normalize_query')(query)
    local bucket_id = require('vshard').router.bucket_id_strcrc32(query)
    local response = cache_get(query, bucket_id)
    if response ~= nil and response ~= box.NULL then return response end

    response = lib:load('fetch_forecast')(query)
    if response.status >= 200 and response.status < 300 then
        cache_put(query, bucket_id, response)
    end
    return response
end

local function init(opts) -- luacheck: no unused args
    lib = lib or box.lib.load('forecast')
    lib:load('init_forecast')()

    local httpd = assert(cartridge.service_get('httpd'), "Failed to get httpd service")
    httpd:route({method = 'GET', path = '/forecast'}, function(request)
        local response = fetch(request.query or '')

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
    fetch = fetch,
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    -- dependencies = {'cartridge.roles.vshard-router'},
}
