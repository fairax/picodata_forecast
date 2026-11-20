#!/usr/bin/env tarantool
-- Test the provisioned queries and pending durations with the pinned promtool.
local fio = require('fio')
local json = require('json')
local popen = require('popen')

-- PromQL label strings do not accept JSON's optional escaped forward slash.
require('compat').json_escape_forward_slash = 'new'

local root = fio.dirname(fio.dirname(fio.abspath(arg[0])))

local function read_json(path)
    local file = assert(io.open(path))
    local content = file:read('*a')
    file:close()
    return json.decode(content)
end

local function write_json(path, value)
    local file = assert(io.open(path, 'w'))
    assert(file:write(json.encode(value)))
    assert(file:close())
end

local function run(args, capture)
    -- Pass arguments directly, without a shell; env resolves Docker on PATH.
    local child = assert(popen.new({'/usr/bin/env', unpack(args)}, {
        stdout = capture and popen.opts.PIPE or popen.opts.INHERIT,
        stderr = popen.opts.INHERIT,
    }))
    local ok, result = pcall(function()
        local chunks = {}
        if capture then
            while true do
                local chunk = assert(child:read())
                if chunk == '' then break end
                chunks[#chunks + 1] = chunk
            end
        end
        local status = assert(child:wait())
        assert(status.state == 'exited' and status.exit_code == 0,
            string.format('%s failed: %s (exit code %s)', args[1], status.state, tostring(status.exit_code)))
        return table.concat(chunks)
    end)
    child:close()
    if not ok then error(result, 0) end
    return result
end

local function main()
    local config = read_json(fio.pathjoin(root, 'monitoring/grafana/provisioning/alerting/alerts.json'))
    local rules, order, tests = {}, {}, {}
    for _, group in ipairs(config.groups) do
        assert(group.interval == '30s')
        for _, rule in ipairs(group.rules) do
            local uid = rule.uid:gsub('^om%-', '')
            rules[uid] = rule
            order[#order + 1] = uid
        end
    end

    local function series(name, values, labels)
        local merged = {job = 'open-meteo', instance = 'router'}
        for k, v in pairs(labels or {}) do merged[k] = v end
        local keys, selector = {}, {}
        for k in pairs(merged) do keys[#keys + 1] = k end
        table.sort(keys)
        for _, k in ipairs(keys) do selector[#selector + 1] = k .. '=' .. json.encode(merged[k]) end
        return {series = name .. '{' .. table.concat(selector, ',') .. '}', values = values}
    end

    local function case(name, uid, inputs, checks, dimensions)
        local rule = rules[uid]
        dimensions = table.copy(dimensions or {instance = 'router'})
        if uid == 'instance-unavailable' or uid == 'memtx-pressure' then dimensions.job = 'open-meteo' end
        local assertions = {}
        for _, check in ipairs(checks) do
            local labels = table.copy(rule.labels)
            for k, v in pairs(dimensions) do labels[k] = v end
            assertions[#assertions + 1] = {
                eval_time = check[1], alertname = rule.title,
                exp_alerts = check[2] and {{exp_labels = labels}} or {},
            }
        end
        tests[#tests + 1] = {name = name, interval = '1m', input_series = inputs, alert_rule_test = assertions}
    end

    case('healthy instance', 'instance-unavailable', {series('up', '1+0x15')}, {{'5m', false}})
    case('down instance waits then fires and recovers', 'instance-unavailable',
        {series('up', '0+0x3 1+0x10')}, {{'1m', false}, {'2m', true}, {'4m', false}})
    case('missing availability data', 'instance-unavailable', {}, {{'2m', true}}, {job = 'open-meteo'})

    local function requests(ok, errors, cancelled)
        local values = {
            series('http_server_request_latency_count', ok, {path = '/forecast', status = '200'}),
            series('http_server_request_latency_count', errors, {path = '/forecast', status = '503'}),
        }
        if cancelled then
            values[#values + 1] = series('http_server_request_latency_count', cancelled,
                {path = '/forecast', status = 'cancelled'})
        end
        return values
    end

    case('5xx fires after pending period; cancellations excluded', 'forecast-errors',
        requests('0+90x15', '0+10x15', '0+1000x15'), {{'5m', false}, {'6m', true}})
    case('exactly five percent is healthy', 'forecast-errors',
        requests('0+95x15', '0+5x15'), {{'10m', false}})
    case('too little traffic', 'forecast-errors', requests('0+1x15', '0+1x15'), {{'10m', false}})
    case('counter resets retain error ratio', 'forecast-errors',
        requests('0+90x6 0+90x10', '0+10x6 0+10x10'), {{'6m', true}, {'12m', true}})
    case('errors recover after the rolling window', 'forecast-errors',
        requests('0+90x6 640+100x10', '0+10x6 60+0x10'), {{'6m', true}, {'13m', false}})

    local function latency(bucket_one, count)
        count = count or '0+100x15'
        return {
            series('http_server_request_latency_bucket', bucket_one, {path = '/forecast', status = '200', le = '1'}),
            series('http_server_request_latency_bucket', count, {path = '/forecast', status = '200', le = '5'}),
            series('http_server_request_latency_bucket', count, {path = '/forecast', status = '200', le = '+Inf'}),
            series('http_server_request_latency_count', count, {path = '/forecast', status = '200'}),
        }
    end

    case('slow p95', 'forecast-latency', latency('0+0x15'), {{'5m', false}, {'6m', true}})
    case('fast p95', 'forecast-latency', latency('0+100x15'), {{'10m', false}})
    case('slow but insufficient traffic', 'forecast-latency', latency('0+0x15', '0+1x15'), {{'10m', false}})
    case('exactly two seconds p95 is healthy', 'forecast-latency', {
        series('http_server_request_latency_bucket', '0+95x15', {path = '/forecast', status = '200', le = '2'}),
        series('http_server_request_latency_bucket', '0+100x15', {path = '/forecast', status = '200', le = '4'}),
        series('http_server_request_latency_bucket', '0+100x15', {path = '/forecast', status = '200', le = '+Inf'}),
        series('http_server_request_latency_count', '0+100x15', {path = '/forecast', status = '200'}),
    }, {{'10m', false}})
    case('idle counters are healthy', 'forecast-errors', requests('0+0x15', '0+0x15'), {{'10m', false}})
    case('idle histogram is healthy', 'forecast-latency', latency('0+0x15', '0+0x15'), {{'10m', false}})

    local function rejection(values)
        return {series('forecast_rejected_requests_total', values, {reason = 'concurrency_limit'})}
    end
    case('rejections meet threshold and pending duration', 'fetch-capacity', rejection('0+1x15'),
        {{'5m', false}, {'7m', true}})
    case('rejections below threshold', 'fetch-capacity', rejection('0+0x15'), {{'10m', false}})
    case('cache operation labels preserved', 'cache-failures',
        {series('forecast_cache_operations_total', '0+2x15', {outcome = 'error', operation = 'get'})},
        {{'6m', false}, {'8m', true}}, {instance = 'router', operation = 'get'})

    local function upstream(success, failure, labels)
        return {
            series('forecast_upstream_attempts_total', success, {outcome = 'response', status = '200'}),
            series('forecast_upstream_attempts_total', failure, labels),
        }
    end
    for _, value in ipairs({{'transport', 'none'}, {'timeout', 'none'}, {'response', '429'}, {'response', '503'}}) do
        case('upstream failure ' .. value[1] .. '/' .. value[2], 'upstream-instability',
            upstream('0+7x15', '0+3x15', {outcome = value[1], status = value[2]}), {{'6m', false}, {'7m', true}})
    end
    case('upstream exact twenty percent', 'upstream-instability',
        upstream('0+8x15', '0+2x15', {outcome = 'response', status = '502'}), {{'10m', false}})
    case('upstream 400 is not instability', 'upstream-instability',
        upstream('0+7x15', '0+3x15', {outcome = 'response', status = '400'}), {{'10m', false}})
    case('upstream minimum attempt count', 'upstream-instability',
        upstream('0+1x15', '0+1x15', {outcome = 'timeout', status = 'none'}), {{'10m', false}})
    case('memory pressure pending and recovery', 'memtx-pressure',
        {series('tnt_slab_quota_used', '95+0x10 80+0x5'), series('tnt_slab_quota_size', '100+0x16')},
        {{'9m', false}, {'10m', true}, {'11m', false}})
    case('memory exact ninety percent', 'memtx-pressure',
        {series('tnt_slab_quota_used', '90+0x15'), series('tnt_slab_quota_size', '100+0x15')}, {{'15m', false}})
    for _, uid in ipairs(order) do
        if uid ~= 'instance-unavailable' then
            case(uid .. ' absent or idle is healthy', uid, {}, {{'15m', false}})
        end
    end

    -- Check minimum traffic and exact count boundaries independently of 'for'.
    local inputs = requests('0+3x15', '0+1x15')
    inputs[#inputs + 1] = rejection('0+1x15')[1]
    local expressions = {}
    for _, uid in ipairs({'forecast-errors', 'fetch-capacity'}) do
        for _, check in ipairs({{'4m', 0}, {'5m', 1}}) do
            expressions[#expressions + 1] = {
                expr = rules[uid].data[1].model.expr, eval_time = check[1],
                exp_samples = {{labels = '{instance="router"}', value = check[2]}},
            }
        end
    end
    tests[#tests + 1] = {name = 'minimum twenty requests and five rejections are inclusive',
        interval = '1m', input_series = inputs, promql_expr_test = expressions}

    local projected_rules = {}
    for _, uid in ipairs(order) do
        local rule = rules[uid]
        assert(rule.execErrState == 'Error' and rule.condition == 'C')
        local threshold = rule.data[2].model
        assert(threshold.type == 'threshold' and threshold.expression == 'A')
        local evaluator = threshold.conditions[1].evaluator
        assert(evaluator.type == 'gt' and #evaluator.params == 1 and evaluator.params[1] == 0)
        assert(rule.noDataState == (uid == 'instance-unavailable' and 'Alerting' or 'OK'))
        projected_rules[#projected_rules + 1] = {
            alert = rule.title, expr = '(' .. rule.data[1].model.expr .. ') > 0',
            ['for'] = rule['for'], labels = rule.labels,
        }
    end

    local compose = json.decode(run({'docker', 'compose', '-f', fio.pathjoin(root, 'compose.monitoring.yml'),
        'config', '--format', 'json'}, true))
    local directory = assert(fio.tempdir())
    local ok, err = pcall(function()
        assert(fio.chmod(directory, tonumber('755', 8))) -- promtool runs as an unprivileged user.
        write_json(fio.pathjoin(directory, 'rules.json'),
            {groups = {{name = 'provisioned-alerts', rules = projected_rules}}})
        write_json(fio.pathjoin(directory, 'tests.json'),
            {rule_files = {'rules.json'}, evaluation_interval = '1m', tests = tests})
        run({'docker', 'run', '--rm', '--volume', directory .. ':/tests:ro', '--workdir', '/tests',
            '--entrypoint', 'promtool', compose.services.prometheus.image, 'test', 'rules', 'tests.json'})
    end)
    assert(fio.rmtree(directory))
    if not ok then error(err, 0) end
    print(string.format('Validated %d provisioned rules across %d deterministic scenarios', #order, #tests))
end

local ok, err = pcall(main)
if not ok then io.stderr:write(tostring(err), '\n'); os.exit(1) end
os.exit(0)