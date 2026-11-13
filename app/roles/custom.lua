-- luacheck: globals box.lib.load

local cartridge = require('cartridge')
local log = require('log')

-- Retain the module so its process-local configuration survives Lua GC.
local lib
local max_concurrent_fetches
local active_fetches = 0
local forecast_route_name = 'app.roles.custom.forecast'

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

    if active_fetches >= max_concurrent_fetches then
        return {status = 503, body = 'Forecast service busy', headers = {['content-type'] = 'text/plain'}}
    end
    -- No yield between checking the limit and reserving a permit.
    active_fetches = active_fetches + 1
    local ok, result = pcall(function()
        -- The native call waits for its worker even when this fiber is cancelled.
        local fetched = lib:load('fetch_forecast')(query)
        if fetched.status >= 200 and fetched.status < 300 then
            cache_put(query, bucket_id, fetched)
        end
        return fetched
    end)
    active_fetches = active_fetches - 1
    if not ok then error(result, 0) end
    return result
end

local function filter_headers(headers)
    headers = headers or {}
    local excluded = {
        connection = true,
        ['keep-alive'] = true,
        ['proxy-connection'] = true,
        te = true,
        trailer = true,
        ['transfer-encoding'] = true,
        upgrade = true,
        -- The HTTP server supplies framing for the returned body.
        ['content-length'] = true,
    }
    for name, value in pairs(headers) do
        if name:lower() == 'connection' then
            for token in value:gmatch('[^,]+') do
                excluded[token:match('^%s*(.-)%s*$'):lower()] = true
            end
        end
    end
    local filtered = {}
    for name, value in pairs(headers) do
        if not excluded[name:lower()] then filtered[name] = value end
    end
    return filtered
end

local function init(opts) -- luacheck: no unused args
    if max_concurrent_fetches == nil then
        local value = os.getenv('OPEN_METEO_MAX_CONCURRENT_FETCHES')
        local limit = value == nil and 32 or tonumber(value)
        if limit == nil or limit < 1 or limit > 9007199254740991 or limit ~= math.floor(limit)
            or (value ~= nil and not value:match('^%d+$')) then
            error('OPEN_METEO_MAX_CONCURRENT_FETCHES must be a positive integer')
        end
        max_concurrent_fetches = limit
    end
    lib = lib or box.lib.load('forecast')
    lib:load('init_forecast')()

    local httpd = assert(cartridge.service_get('httpd'), "Failed to get httpd service")
    httpd:route({name = forecast_route_name, method = 'GET', path = '/forecast'}, function(request)
        local response = fetch(request.query or '')

        return {status = response.status, body = response.body, headers = filter_headers(response.headers)}
    end)

    return true
end

local function stop()
    local httpd = cartridge.service_get('httpd')
    if httpd ~= nil then
        local index = httpd.iroutes[forecast_route_name]
        if index ~= nil then
            table.remove(httpd.routes, index)
            httpd.iroutes[forecast_route_name] = nil
            -- Removing a route shifts the indexes of subsequent named routes.
            for i, route in ipairs(httpd.routes) do
                if route.name then httpd.iroutes[route.name] = i end
            end
        end
    end
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
