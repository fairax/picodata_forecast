local t = require('luatest')
local g = t.group('unit_observability')

local function metric(name, labels)
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
end

g.test_observability_wrapped_cancellation_preserves_error_and_counts_once = function()
    local fiber = require('fiber')
    local obs = require('app.observability')
    local original, received, calls
    calls = 0
    local wrapped = obs.wrap_http(function()
        calls = calls + 1
        local _, err = pcall(fiber.sleep, 100)
        original = err
        error(err, 0)
    end)
    local task = fiber.create(function()
        local ok, err = pcall(wrapped, {endpoint = {path = '/wrapped-cancellation', method = 'GET'}})
        received = {ok = ok, same = rawequal(err, original), kind = err.type}
    end)
    task:set_joinable(true)
    task:cancel()
    task:join()
    received.calls = calls
    t.assert_equals(received, {ok = false, same = true, kind = 'FiberIsCancelled', calls = 1})
    t.assert_equals(metric('http_server_request_latency_count',
        {path = '/wrapped-cancellation', method = 'GET', status = 'cancelled'}), 1)
end

g.test_observability_wrapped_outcomes_survive_metric_errors = function()
    local obs = require('app.observability')
    local collector
    for _, candidate in pairs(require('metrics').collectors()) do
        if candidate.name == 'http_server_request_latency' then collector = candidate end
    end
    local request = {endpoint = {path = '/wrapped-outcome', method = 'GET'}}
    local response, original, calls = {status = 202, body = 'accepted'}, {}, 0
    local function check()
        local returned = obs.wrap_http(function() calls = calls + 1; return response end)(request)
        t.assert(rawequal(returned, response))
        local ok, err = pcall(obs.wrap_http(function() calls = calls + 1; error(original, 0) end), request)
        t.assert_not(ok)
        t.assert(rawequal(err, original))
    end
    check()
    local observe = collector.observe
    collector.observe = function() error('injected observation failure') end
    local ok, err = pcall(check)
    collector.observe = observe
    t.assert(ok, tostring(err))
    t.assert_equals(calls, 4)
    for _, status in ipairs({202, 500}) do
        t.assert_equals(metric('http_server_request_latency_count',
            {path = '/wrapped-outcome', method = 'GET', status = status}), 1)
    end
end

g.test_observability_wrapped_default_status_and_string_error = function()
    local obs = require('app.observability')
    local request = {endpoint = {path = '/wrapped-default', method = 'GET'}}
    local response, calls = {body = 'ok'}, 0
    local returned = obs.wrap_http(function() calls = calls + 1; return response end)(request)
    t.assert(rawequal(returned, response))
    local ok, err = pcall(obs.wrap_http(function()
        calls = calls + 1
        error('original handler error', 0)
    end), request)
    t.assert_not(ok)
    -- The library may prefix string errors with its source location.
    t.assert_str_contains(err, 'original handler error')
    t.assert_equals(calls, 2)
    for _, status in ipairs({200, 500}) do
        t.assert_equals(metric('http_server_request_latency_count',
            {path = '/wrapped-default', method = 'GET', status = status}), 1)
    end
end

g.test_request_id_validation = function()
    local obs = require('app.observability')
    t.assert_equals(obs.request_id('client.first:1'), 'client.first:1')
    local previous = obs.request_id(nil)
    t.assert(require('uuid').fromstr(previous) ~= nil)
    for _, value in ipairs({'', 'contains spaces', 'first, second', string.rep('x', 129)}) do
        local id = obs.request_id(value)
        t.assert_equals(#id, 36)
        t.assert(require('uuid').fromstr(id) ~= nil)
        t.assert(id ~= previous)
        previous = id
    end
end
