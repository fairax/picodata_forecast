-- luacheck: globals box.lib.load

local cartridge = require('cartridge')
local obs = require('app.observability')
local clock = require('clock')
local fiber = require('fiber')

-- Retain the module so its process-local configuration survives Lua GC.
local lib
local max_concurrent_fetches
local active_fetches = 0
local forecast_route_name = 'app.roles.custom.forecast'

local function cache_get(query, bucket_id, request_id)
    local started = clock.monotonic()
    local ok, response, err = pcall(require('vshard').router.callrw,
        bucket_id, 'forecast.cache_get', {query, request_id}, {timeout = 1})
    if not ok then err, response = response, nil end
    local hit = response ~= nil and response ~= box.NULL
    local outcome = err ~= nil and 'error' or (hit and 'hit' or 'miss')
    obs.cache(request_id, 'get', outcome, started, bucket_id, err and tostring(err))
    return err == nil and response or nil
end

local function cache_put(query, bucket_id, response, request_id)
    local started = clock.monotonic()
    local ok, result, err = pcall(require('vshard').router.callrw,
        bucket_id, 'forecast.cache_put', {query, bucket_id, response, request_id}, {timeout = 1})
    if not ok then err, result = result, nil end
    obs.cache(request_id, 'put', err ~= nil and 'error' or 'success', started, bucket_id, err and tostring(err))
    return result
end

local function fetch(query, request_id)
    request_id = obs.request_id(request_id)
    query = lib:load('normalize_query')(query)
    local bucket_id = require('vshard').router.bucket_id_strcrc32(query)
    local response = cache_get(query, bucket_id, request_id)
    if response ~= nil and response ~= box.NULL then return response end

    if active_fetches >= max_concurrent_fetches then
        obs.reject(request_id)
        return {status = 503, body = 'Forecast service busy', headers = {['content-type'] = {'text/plain'}}}
    end
    local completion = fiber.channel(1)
    -- No yield between checking the limit and reserving a permit.
    active_fetches = active_fetches + 1
    obs.active(active_fetches)
    -- SDK 6's I/O executor cannot safely unwind a cancelled fiber. Keep native
    -- I/O on an independent fiber; caller cancellation must not free its slot.
    local started, err = pcall(fiber.new, function()
        local ok, result = pcall(function()
            local fetched = lib:load('fetch_forecast')(query, request_id)
            if fetched.status >= 200 and fetched.status < 300 and #fetched.body > 0 then
                cache_put(query, bucket_id, fetched, request_id)
            else
                obs.cache(request_id, 'put', 'skipped', clock.monotonic(), bucket_id)
            end
            return fetched
        end)
        active_fetches = active_fetches - 1
        obs.active(active_fetches)
        completion:put({ok = ok, result = result}, 0)
    end)
    if not started then
        active_fetches = active_fetches - 1
        obs.active(active_fetches)
        error(err, 0)
    end
    local outcome = completion:get()
    if not outcome.ok then error(outcome.result, 0) end
    return outcome.result
end

local function filter_headers(headers)
    headers = headers or {}
    local safe_headers = {}
    for name, values in pairs(headers) do
        local safe = {}
        for _, value in ipairs(values) do
            if not value:find('[\r\n]') then safe[#safe + 1] = value end
        end
        if #safe > 0 then
            safe_headers[name] = safe
        end
    end
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
    for name, values in pairs(safe_headers) do
        if name:lower() == 'connection' then
            for _, value in ipairs(values) do
                for token in value:gmatch('[^,]+') do
                    excluded[token:match('^%s*(.-)%s*$'):lower()] = true
                end
            end
        end
    end
    local filtered = {}
    for name, value in pairs(safe_headers) do
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
    obs.active(active_fetches)

    local httpd = assert(cartridge.service_get('httpd'), "Failed to get httpd service")
    local handler = function(request)
        local request_id = obs.request_id((request.headers or {})['x-request-id'])
        local started = clock.monotonic()
        local ok, response = pcall(fetch, request.query or '', request_id)
        if not ok then
            local cancelled = type(response) == 'cdata' and response.type == 'FiberIsCancelled'
            obs.event(cancelled and 'info' or 'error',
                cancelled and 'request_completed' or 'request_failed', request_id,
                {outcome = cancelled and 'cancelled' or 'error',
                    error_kind = cancelled and 'cancelled' or 'internal', status = not cancelled and 500 or nil,
                    duration_seconds = clock.monotonic() - started, detail = tostring(response)})
            if cancelled then error(response, 0) end
            response = {status = 500, body = 'Internal server error',
                headers = {['content-type'] = {'text/plain; charset=utf-8'}}}
        end
        obs.event('info', 'request_completed', request_id,
            {outcome = ok and 'response' or 'error', status = response.status,
                duration_seconds = clock.monotonic() - started})
        local headers = filter_headers(response.headers)
        for name in pairs(headers) do
            if name:lower() == 'x-request-id' then headers[name] = nil end
        end
        headers['x-request-id'] = {request_id}
        return {status = response.status, body = response.body, headers = headers}
    end
    httpd:route({name = forecast_route_name, method = 'GET', path = '/forecast'}, obs.wrap_http(handler))

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
