local t = require('luatest')
local fiber = require('fiber')
local clock = require('clock')
local fio = require('fio')
local http = require('http.server')
local helper = require('test.helper')
local M = {}

function M.start(cg)
    cg.mock = http.new('127.0.0.1', 0, {log_requests = false})
    cg.mock:route({method = 'GET', path = '/forecast'}, function(req)
        local gate, entered = cg.gate, cg.entered
        cg.requests = cg.requests + 1
        local action = cg.actions[cg.requests] or {}
        local status = action.status or cg.status
        local body = action.body or cg.body or req.query
        local retry_after = action.retry_after
        if retry_after == nil then retry_after = cg.retry_after end
        if retry_after == false then retry_after = nil end
        table.insert(cg.upstream_request_ids, req.headers['x-request-id'] or false)
        table.insert(cg.request_times, fiber.clock())
        table.insert(cg.request_wall_times, clock.realtime())
        entered:put(true)
        if not cg.auto_respond and action.gated ~= false then gate:get(40) end
        return {status = status, body = body, headers = {
            ['content-type'] = action.content_type or 'text/plain', ['retry-after'] = retry_after,
            ['location'] = action.location, ['content-encoding'] = action.content_encoding,
            ['x-request-id'] = action.response_request_id,
        }}
    end)
    cg.mock:start()
    local port = cg.mock.tcp_server:name().port
    cg.endpoint = 'http://127.0.0.1:' .. port .. '/forecast'
end

function M.reset(cg)
    cg.gate = fiber.channel(10)
    cg.entered = fiber.channel(100)
    cg.requests = 0
    cg.upstream_request_ids = {}
    cg.request_times = {}
    cg.request_wall_times = {}
    cg.auto_respond = false
    cg.actions = {}
    cg.retry_after = '0'
    cg.status = 200
    cg.body = nil
end

function M.fault_state(cg)
    local file = assert(io.open(fio.pathjoin(cg.fault_directory, 'state.json')))
    local data = file:read('*a')
    file:close()
    return require('json').decode(data)
end

function M.fault_upstream(cg, mode)
    cg.fault_directory = fio.tempdir()
    cg.fault_process = require('luatest.process'):start('/usr/local/bin/tarantool', {
        fio.pathjoin(helper.root, 'test', 'fixtures', 'fault_upstream.lua'), mode, cg.fault_directory,
    }, os.environ())
    local state
    t.helpers.retrying({timeout = 5}, function()
        state = M.fault_state(cg)
    end)
    local scheme = mode == 'certificate' and 'https' or 'http'
    cg.configure(cg, scheme .. '://127.0.0.1:' .. state.port .. '/forecast')
end

function M.proxy_server(cg, mode, tls_port)
    local socket = require('socket')
    cg.proxy_requests = {}
    cg.proxy_listener = assert(socket.tcp_server('127.0.0.1', 0, function(connection)
        local head = connection:read({delimiter = '\r\n\r\n'}, 5)
        if not head then return end
        cg.proxy_requests[#cg.proxy_requests + 1] = head
        if mode == 'slow' then fiber.sleep(2) end
        if mode == 'reject' then
            connection:write('HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 6\r\n\r\nsecret', 1)
            return
        end
        if mode == 'malformed' then connection:write('invalid proxy head\r\n\r\n', 1); return end
        if mode == 'oversized' then
            connection:write('HTTP/1.1 200 OK\r\nLong: ' .. string.rep('x', 17000) .. '\r\n\r\n', 1)
            return
        end
        if head:match('^CONNECT ') then
            local upstream = assert(socket.tcp_connect('127.0.0.1', tls_port, 3))
            connection:write('HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 Connection Established\r\n\r\n', 3)
            local function relay(from, to)
                while from:readable(5) do
                    local data = from:sysread(8192)
                    if not data or data == '' then break end
                    if not to:write(data, 5) then break end
                end
            end
            local reader = fiber.create(function() relay(upstream, connection); connection:shutdown() end)
            reader:set_joinable(true)
            relay(connection, upstream)
            upstream:shutdown()
            reader:join()
            upstream:close()
        else
            local status = mode == 'retry' and #cg.proxy_requests == 1 and 503 or 200
            local body = mode == 'binary' and string.char(0, 255, 128) or 'proxied'
            local headers = 'Content-Type: application/octet-stream\r\nX-Repeat: first\r\nX-Repeat: second\r\n'
            connection:write('HTTP/1.1 ' .. status .. ' Result\r\nContent-Length: ' .. #body .. '\r\n' ..
                headers .. 'Retry-After: 0\r\nConnection: close\r\n\r\n' .. body, 1)
        end
    end))
    return 'http://127.0.0.1:' .. cg.proxy_listener:name().port
end

function M.reset_resources(cg)
    if cg.gate then cg.gate:close() end
    if cg.entered then cg.entered:close() end
    for _, name in ipairs({'proxy_listener', 'header_listener', 'tls_listener'}) do
        if cg[name] then cg[name]:close(); cg[name] = nil end
    end
    if cg.fault_process then
        cg.fault_process:kill(nil, {quiet = true})
        cg.fault_process = nil
        fio.rmtree(cg.fault_directory)
    end
end

return M
