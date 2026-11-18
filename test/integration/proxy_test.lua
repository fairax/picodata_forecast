local t = require('luatest')
local fio = require('fio')
local g = t.group('integration_proxy')

local fixture = require('test.helpers.forecast')
fixture.bind(g, {name = 'proxy', port = 6800, transport = false})

local upstream = require('test.helpers.upstream')
local fetch = fixture.fetch
local finish = fixture.finish
local configure = fixture.configure
local fault_upstream = upstream.fault_upstream
local fault_state = upstream.fault_state
local proxy_server = upstream.proxy_server

g.test_proxy_http_absolute_target_auth_and_cache = function(cg)
    local proxy = proxy_server(cg, 'binary'):gsub('http://', 'http://user:p%%40ss@')
    configure(cg, 'http://unresolvable.invalid:1234/forecast', nil, nil, proxy)
    for _ = 1, 2 do
        local response = fetch(cg, 'b=2&a=1').response
        t.assert_equals(response.status, 200)
        t.assert_equals(response.body, string.char(0, 255, 128))
        t.assert_equals(response.headers['x-repeat'], {'first', 'second'})
    end
    t.assert_equals(#cg.proxy_requests, 1)
    t.assert_str_contains(cg.proxy_requests[1], 'GET http://unresolvable.invalid:1234/forecast?a=1&b=2 HTTP/1.1')
    t.assert_str_contains(cg.proxy_requests[1]:lower(), 'proxy-authorization: basic dxnlcjpwqhnz')
    t.assert_equals(cg.proxy_requests[1]:lower():find('x-request-id', 1, true), nil)
end

g.test_proxy_https_connect_and_certificate_validation = function(cg)
    fault_upstream(cg, 'certificate')
    local port = fault_state(cg).port
    local proxy = proxy_server(cg, 'tunnel', port):gsub('http://', 'http://user:p%%40ss@')
    local endpoint = 'https://localhost:' .. port .. '/forecast'
    configure(cg, endpoint, nil, nil, proxy)
    t.assert_equals(fetch(cg, 'proxy=untrusted').response.status, 502)
    t.assert_equals(#cg.proxy_requests, 1) -- Certificate failures are not retried.
    cg.router.env.SSL_CERT_FILE = fio.pathjoin(cg.fault_directory, 'cert.pem')
    configure(cg, endpoint, '29', nil, proxy) -- Restart with the trusted test certificate.
    local response = fetch(cg, 'proxy=trusted').response
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'ok')
    t.assert_str_contains(cg.proxy_requests[2], 'CONNECT localhost:' .. port .. ' HTTP/1.1')
    t.assert_str_contains(cg.proxy_requests[2]:lower(), 'proxy-authorization: basic dxnlcjpwqhnz')
    t.assert_equals(fault_state(cg).proxy_authorization_seen, false)
end

g.test_proxy_rejection_is_generic_without_direct_fallback = function(cg)
    local proxy = proxy_server(cg, 'reject')
    configure(cg, cg.endpoint, nil, nil, proxy)
    for _ = 1, 2 do
        local response = fetch(cg, 'proxy=rejected').response
        t.assert_equals(response.status, 502)
        t.assert_equals(response.body, 'Forecast service unavailable')
    end
    t.assert_equals(#cg.proxy_requests, 2)
    t.assert_equals(cg.requests, 0)
end

g.test_proxy_timeout_yields_and_retries_transient_responses = function(cg)
    local proxy = proxy_server(cg, 'slow')
    configure(cg, cg.endpoint, '1', nil, proxy)
    local future = fetch(cg, 'proxy=timeout', true)
    t.helpers.retrying({timeout = 3}, function() t.assert_equals(#cg.proxy_requests, 1) end)
    t.assert_equals(cg.router.net_box:eval('return 42', {}, {timeout = 0.5}), 42)
    t.assert_equals(finish(future).response.status, 504)
    t.assert_equals(#cg.proxy_requests, 1)
    cg.proxy_listener:close()
    proxy = proxy_server(cg, 'retry')
    configure(cg, cg.endpoint, nil, nil, proxy)
    for _ = 1, 2 do t.assert_equals(fetch(cg, 'proxy=retry').response.status, 200) end
    t.assert_equals(#cg.proxy_requests, 2) -- Retry then success; subsequent call hits cache.
end

g.test_proxy_connect_malformed_and_large_heads = function(cg)
    for _, mode in ipairs({'malformed', 'oversized'}) do
        local proxy = proxy_server(cg, mode)
        configure(cg, 'https://unresolvable.invalid/forecast', nil, nil, proxy)
        t.assert_equals(fetch(cg, 'proxy=' .. mode).response.status, 502)
        t.assert_equals(#cg.proxy_requests, 1)
        cg.proxy_listener:close()
        cg.proxy_listener = nil
    end
end
