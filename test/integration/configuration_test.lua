local t = require('luatest')
local g = t.group('integration_configuration')

local fio = require('fio')
local helper = require('test.helper')

local function probe_configuration(endpoint, timeout, replica, concurrency, proxy)
    local directory = fio.tempdir()
    local output = fio.pathjoin(directory, 'result.json')
    local env = table.copy(os.environ())
    env.OPEN_METEO_FORECAST_URL = endpoint
    env.OPEN_METEO_TIMEOUT_SECS = timeout
    env.OPEN_METEO_MAX_CONCURRENT_FETCHES = concurrency
    env.OPEN_METEO_HTTP_PROXY = proxy or ''
    local process = require('luatest.process'):start('/usr/local/bin/tarantool', {
        fio.pathjoin(helper.root, 'test', 'fixtures', 'forecast_config_probe.lua'),
        helper.root, output, directory, replica and 'replica' or 'master',
    }, env)
    local ok, result = pcall(function()
        local value
        t.helpers.retrying({timeout = 5}, function()
            local file = assert(io.open(output))
            local content = file:read('*a')
            file:close()
            value = require('json').decode(content)
        end)
        return value
    end)
    process:kill(nil, {quiet = true})
    fio.rmtree(directory)
    assert(ok, tostring(result))
    return result
end

g.test_invalid_configuration_fails_role_startup = function()
    for _, endpoint in ipairs({'invalid-private-endpoint', 'file:///tmp/forecast', ''}) do
        local result = probe_configuration(endpoint, '30')
        t.assert_equals(result.ok, false)
        t.assert_str_contains(result.error, 'Invalid forecast configuration')
    end
end

g.test_valid_timeout_settings = function()
    for _, value in ipairs({'15', '+5'}) do
        t.assert_equals(probe_configuration('http://localhost/forecast', value).ok, true)
    end
end

g.test_invalid_timeout_fails_role_startup = function()
    for _, value in ipairs({
        '', '0', '-1', 'abc', '1.5', ' 5', string.char(255),
        '18446744073709551616', '18446744073709551615',
    }) do
        local result = probe_configuration('http://localhost/forecast', value)
        t.assert_equals(result.ok, false)
        t.assert_str_contains(result.error, 'OPEN_METEO_TIMEOUT_SECS')
    end
end

g.test_defaults_validate_on_router_instances = function()
    t.assert_equals(probe_configuration(nil, nil).ok, true)
    t.assert_equals(probe_configuration(nil, nil, true).ok, true)
    local invalid = probe_configuration('http://localhost/forecast', '0', true)
    t.assert_equals(invalid.ok, false)
    t.assert_str_contains(invalid.error, 'OPEN_METEO_TIMEOUT_SECS')
end

g.test_fetch_limit_configuration = function()
    t.assert_equals(probe_configuration('http://localhost/forecast', '30', false, '2').ok, true)
    for _, value in ipairs({'', '0', '-1', '1.5', 'abc', ' 2', '1e2', '9007199254740992'}) do
        local result = probe_configuration('http://localhost/forecast', '30', false, value)
        t.assert_equals(result.ok, false)
        t.assert_str_contains(result.error, 'OPEN_METEO_MAX_CONCURRENT_FETCHES')
    end
end

g.test_proxy_invalid_configuration_fails_without_exposing_credentials = function()
    for _, value in ipairs({'invalid', 'https://proxy:443', 'socks5://proxy:1080',
        'http://user:secret@proxy/path', 'http://proxy?q=1', 'http://proxy#fragment', string.char(255)}) do
        local result = probe_configuration('http://localhost/forecast', '30', false, nil, value)
        t.assert_equals(result.ok, false)
        t.assert_str_contains(result.error, 'OPEN_METEO_HTTP_PROXY')
        t.assert_equals(result.error:find('secret', 1, true), nil)
    end
end
