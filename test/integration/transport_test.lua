local t = require('luatest')
local fio = require('fio')
local g = t.group('integration_transport')

local fixture = require('test.helpers.forecast')
fixture.bind(g, {name = 'transport', port = 38080, transport = true})

local upstream = require('test.helpers.upstream')
local fetch = fixture.fetch
local finish = fixture.finish
local configure = fixture.configure
local fault_upstream = upstream.fault_upstream
local fault_state = upstream.fault_state

g.test_persistent_transient_statuses_stop_after_three_attempts = function(cg)
    cg.auto_respond = true
    for _, status in ipairs({429, 502, 503, 504}) do
        local before = cg.requests
        cg.status = status
        cg.body = 'final upstream error'
        local response = fetch(cg, 'persistent=' .. status)
        t.assert_equals(response.response.status, status)
        t.assert_equals(response.response.body, cg.body)
        t.assert_equals(response.response.headers['retry-after'], {'0'})
        t.assert_equals(cg.requests - before, 3)
    end
end

g.test_permanent_http_errors_are_not_retried = function(cg)
    cg.auto_respond = true
    for _, status in ipairs({400, 401, 403, 404}) do
        local before = cg.requests
        cg.status = status
        local response = fetch(cg, 'permanent=' .. status)
        t.assert_equals(response.response.status, status)
        t.assert_equals(cg.requests - before, 1)
    end
end

g.test_retry_after_seconds_and_fiber_responsiveness = function(cg)
    cg.auto_respond = true
    cg.actions = {{status = 429, retry_after = '1'}, {status = 200}}
    local future = fetch(cg, 'retry=seconds', true)
    t.assert_equals(cg.entered:get(5), true)
    t.assert_equals(cg.router.net_box:eval('return 42', {}, {timeout = 0.5}), 42)
    t.assert_equals(future:is_ready(), false)
    t.assert_equals(finish(future).response.status, 200)
    t.assert_equals(cg.requests, 2)
    t.assert(cg.request_times[2] - cg.request_times[1] >= 1)
end

g.test_retry_after_http_date = function(cg)
    cg.auto_respond = true
    local deadline = os.time() + 2
    local date = os.date('!%a, %d %b %Y %H:%M:%S GMT', deadline)
    cg.actions = {{status = 503, retry_after = date}, {status = 200}}
    t.assert_equals(fetch(cg, 'retry=date').response.status, 200)
    t.assert_equals(cg.requests, 2)
    t.assert(cg.request_wall_times[2] >= deadline, tostring(cg.request_wall_times[2] - deadline))
end

g.test_malformed_and_absent_retry_after_are_ignored = function(cg)
    cg.auto_respond = true
    cg.actions = {{status = 503, retry_after = 'not-a-date'}, {status = 503, retry_after = false}, {status = 200}}
    t.assert_equals(fetch(cg, 'retry=malformed').response.status, 200)
    t.assert_equals(cg.requests, 3)
    t.assert(cg.request_times[3] - cg.request_times[1] < 2)
end

g.test_connection_reset_is_retried = function(cg)
    fault_upstream(cg, 'reset')
    local response = fetch(cg, 'transport=reset')
    t.assert_equals(response.response.status, 200)
    t.assert_equals(response.response.body, 'ok')
    t.assert_equals(fault_state(cg).count, 2)
end

g.test_persistent_connection_reset_stops_after_three_attempts = function(cg)
    fault_upstream(cg, 'reset-always')
    local response = fetch(cg, 'transport=persistent')
    t.assert_equals(response.response.status, 502)
    t.assert_equals(fault_state(cg).count, 3)
end

g.test_not_modified_content_length_is_metadata = function(cg)
    fault_upstream(cg, 'not-modified')
    local response = fetch(cg, 'bodyless=304').response
    t.assert_equals(response.status, 304)
    t.assert_equals(response.body, '')
    t.assert_equals(response.headers['content-length'], {'20'})
    t.assert_equals(fault_state(cg).count, 1)
end

g.test_no_content_response_is_not_retried = function(cg)
    fault_upstream(cg, 'no-content')
    local response = fetch(cg, 'bodyless=204').response
    t.assert_equals(response.status, 204)
    t.assert_equals(response.body, '')
    t.assert_equals(fault_state(cg).count, 1)
end

g.test_certificate_validation_failure_is_not_retried = function(cg)
    fault_upstream(cg, 'certificate')
    local response = fetch(cg, 'tls=untrusted-certificate')
    t.assert_equals(response.response.status, 502)
    t.assert_equals(fault_state(cg).count, 1)
end

g.test_retry_after_beyond_budget_returns_upstream_response = function(cg)
    cg.auto_respond = true
    cg.status = 503
    for _, delay in ipairs({
        '120',
        '99999999999999999999999999999999',
        os.date('!%a, %d %b %Y %H:%M:%S GMT', os.time() + 3600),
    }) do
        cg.retry_after = delay
        local before = cg.requests
        local response = fetch(cg, 'retry=overbudget-' .. before)
        t.assert_equals(response.response.status, 503)
        t.assert_equals(response.response.headers['retry-after'], {delay})
        t.assert_equals(cg.requests - before, 1)
    end
end

g.test_initialized_configuration_is_stable = function(cg)
    cg.auto_respond = true
    cg.router.net_box:eval([[
        os.setenv('OPEN_METEO_FORECAST_URL', 'invalid-after-initialization')
        os.setenv('OPEN_METEO_TIMEOUT_SECS', '0')
        local lib = box.lib.load('forecast')
        lib:load('init_forecast')() -- Explicit initialization is idempotent.
    ]])
    cg.router.net_box:eval("collectgarbage('collect')")
    local response = fetch(cg, 'configuration=stable')
    t.assert_equals(response.response.status, 200)
    t.assert_equals(cg.requests, 1)
    cg.router.net_box:eval([[
        os.setenv('OPEN_METEO_FORECAST_URL', ...)
        os.setenv('OPEN_METEO_TIMEOUT_SECS', '30')
    ]], {cg.endpoint})
end

g.test_body_read_timeout_is_504 = function(cg)
    fault_upstream(cg, 'slow-body')
    configure(cg, cg.router.env.OPEN_METEO_FORECAST_URL, '1')
    local response = fetch(cg, 'body-timeout=1')
    t.assert_equals(response.response.status, 504)
    t.assert_equals(response.response.body, 'Forecast request timed out')
    t.assert_equals(fault_state(cg).count, 1)
end

g.test_fibreq_https_with_trusted_test_certificate = function(cg)
    fault_upstream(cg, 'certificate')
    cg.router.env.SSL_CERT_FILE = fio.pathjoin(cg.fault_directory, 'cert.pem')
    configure(cg, 'https://localhost:' .. fault_state(cg).port .. '/forecast')
    local response = fetch(cg, 'tls=trusted').response
    t.assert_equals(response.status, 200)
    t.assert_equals(response.body, 'ok')
    t.assert_equals(fault_state(cg).count, 1)
end

g.test_incomplete_responses_are_rejected = function(cg)
    for _, mode in ipairs({'truncated-always', 'truncated-chunked'}) do
        fault_upstream(cg, mode)
        local response = fetch(cg, 'truncated=' .. mode).response
        t.assert_equals(response.status, 502)
        t.assert_equals(response.body, 'Forecast service unavailable')
        t.assert_equals(fault_state(cg).count, 3)
        cg.fault_process:kill(nil, {quiet = true})
        cg.fault_process = nil
        fio.rmtree(cg.fault_directory)
    end
end
