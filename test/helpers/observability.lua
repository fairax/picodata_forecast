local M = {}

function M.request_with_id(cg, query, request_id)
    return cg.router:http_request('get', '/forecast?' .. query,
        {raise = false, http = {headers = {['X-Request-ID'] = request_id}}})
end

function M.capture_events(cg)
    for _, server in ipairs(cg.cluster.servers) do
        server.net_box:eval([[
            -- Tarantool gives each module its own logger; hook the one used by event().
            local log
            for i = 1, math.huge do
                local name, value = debug.getupvalue(require('app.observability').event, i)
                if name == nil then break end
                if name == 'log' then log = value; break end
            end
            assert(log, 'observability logger not found')
            local saved, events = {}, {}
            rawset(_G, 'observability_saved_log', {logger = log, methods = saved})
            rawset(_G, 'observability_events', events)
            for _, level in ipairs({'debug', 'info', 'warn', 'error'}) do
                saved[level] = log[level]
                log[level] = function(record, ...)
                    if type(record) == 'table' and record.request_id then
                        local copy = table.copy(record)
                        copy.level = level
                        events[#events + 1] = copy
                    end
                    return saved[level](record, ...)
                end
            end
        ]])
    end
end

function M.metric(cg, name, labels)
    return cg.router.net_box:eval([[
        local name, labels = ...
        local total = 0
        for _, sample in ipairs(require('metrics').collect()) do
            if sample.metric_name == name then
                local matches = true
                for k, v in pairs(labels or {}) do
                    if sample.label_pairs[k] ~= v then matches = false end
                end
                if matches then total = total + sample.value end
            end
        end
        return total
    ]], {name, labels or {}})
end

function M.raw_http_response(router, path, extra_headers)
    local socket = require('socket')
    local conn = assert(socket.tcp_connect('127.0.0.1', router.http_port, 5))
    assert(conn:write('GET ' .. path .. ' HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n' ..
        (extra_headers or '') .. '\r\n', 5))
    local raw = assert(conn:read({delimiter = '\r\n\r\n'}, 5))
    local headers = {}
    for name, value in raw:gmatch('([^\r\n:]+): ([^\r\n]*)\r\n') do
        name = name:lower()
        local previous = headers[name]
        if previous == nil then headers[name] = value
        elseif type(previous) == 'table' then previous[#previous + 1] = value
        else headers[name] = {previous, value} end
    end
    local body = assert(conn:read(tonumber(headers['content-length']), 5))
    conn:close()
    return {status = tonumber(raw:match('^HTTP/1%.1 (%d+)')), headers = headers, body = body}
end

return M
