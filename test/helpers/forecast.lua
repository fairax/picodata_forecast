local t = require('luatest')
local fio = require('fio')
local cartridge_helpers = require('cartridge.test-helpers')
local helper = require('test.helper')
local upstream = require('test.helpers.upstream')
local M = {}

function M.finish(future)
    local values, err = future:wait_result(35)
    assert(values, tostring(err))
    return values[1]
end

function M.configure(cg, endpoint, timeout, concurrency, proxy)
    timeout = timeout or '30'
    proxy = proxy or ''
    for _, server in ipairs({cg.router}) do
        if server.env.OPEN_METEO_FORECAST_URL ~= endpoint or server.env.OPEN_METEO_TIMEOUT_SECS ~= timeout
            or server.env.OPEN_METEO_MAX_CONCURRENT_FETCHES ~= concurrency
            or server.env.OPEN_METEO_HTTP_PROXY ~= proxy then
            server:stop()
            server.env.OPEN_METEO_FORECAST_URL = endpoint
            server.env.OPEN_METEO_TIMEOUT_SECS = timeout
            server.env.OPEN_METEO_MAX_CONCURRENT_FETCHES = concurrency
            server.env.OPEN_METEO_HTTP_PROXY = proxy
            server:start()
            cg.cluster:wait_until_healthy(server)
        end
    end
end

function M.set_router_role(cg, enabled)
    local roles = {'vshard-router'}
    if enabled then table.insert(roles, 'app.roles.custom') end
    cg.router:graphql({
        query = [[mutation($rs: String!, $roles: [String!]!) {
            cluster { edit_topology(replicasets: [{uuid: $rs, roles: $roles}]) { servers { uri } } }
        }]],
        variables = {rs = cg.router.replicaset_uuid, roles = roles},
    })
end

function M.set_endpoint(cg, endpoint)
    M.configure(cg, endpoint)
end

function M.fetch(cg, query, async)
    local code = [[
        local ok, response = pcall(require('app.roles.custom').fetch, ...)
        return {ok = ok, response = ok and response or tostring(response)}
    ]]
    -- Native transport calls need a request ID, without involving cache routing.
    if cg.transport then
        code = [[
            local query = ...
            local ok, response = pcall(box.lib.load('forecast'):load('fetch_forecast'), query, 'transport-test')
            return {ok = ok, response = ok and response or tostring(response)}
        ]]
    end
    return cg.router.net_box:eval(code, {query}, {is_async = async or false, timeout = 35})
end

function M.bind(g, options)
    g.before_all(function(cg)
        cg.transport = options.transport or false
        cg.configure = M.configure
        upstream.start(cg)
        local replicasets = cg.transport and {{alias = 'router', roles = {'app.roles.custom'},
            servers = {{alias = 'router'}}}} or {
            {alias = 'storage', roles = {'app.roles.storage', 'vshard-storage'}, servers = {{alias = 'storage'}}},
            {alias = 'router', roles = {'app.roles.custom', 'vshard-router'}, servers = {{alias = 'router'}}},
            {alias = 'storage2', roles = {'app.roles.storage', 'vshard-storage'}, servers = {{alias = 'storage2'}}},
        }
        cg.cluster = cartridge_helpers.Cluster:new({
            server_command = helper.server_command,
            datadir = fio.pathjoin(helper.datadir, options.name),
            base_http_port = options.port, base_advertise_port = options.port + 300,
            use_vshard = false,
            env = {OPEN_METEO_FORECAST_URL = cg.endpoint, OPEN_METEO_TIMEOUT_SECS = '30', OPEN_METEO_HTTP_PROXY = ''},
            replicasets = replicasets,
        })
        cg.cluster:start()
        cg.router = cg.transport and cg.cluster.main_server or cg.cluster.servers[2]
        if not cg.transport then
            t.helpers.retrying({timeout = 5}, function()
                local candidates = cg.router.net_box:eval([[
                    return require('cartridge.rpc').get_candidates('app.roles.storage', {leader_only = true})
                ]])
                t.assert_equals(#candidates, 2)
            end)
            cg.cluster:bootstrap_vshard()
        end
    end)
    g.before_each(function(cg)
        M.configure(cg, cg.endpoint)
        if not cg.transport then helper.truncate_space_on_cluster(cg.cluster, 'forecast_cache') end
        upstream.reset(cg)
    end)
    g.after_each(function(cg)
        if cg.router_role_disabled then M.set_router_role(cg, true); cg.router_role_disabled = nil end
        if cg.stopped_storage then
            cg.stopped_storage:start()
            cg.cluster:wait_until_healthy(cg.stopped_storage)
            cg.stopped_storage = nil
        end
        for _, server in ipairs(cg.cluster.servers) do
            if server.net_box ~= nil then
                server.net_box:eval([[
                    local saved = rawget(_G, 'observability_saved_log')
                    if saved then
                        for level, fn in pairs(saved.methods) do saved.logger[level] = fn end
                        rawset(_G, 'observability_saved_log', nil)
                        rawset(_G, 'observability_events', nil)
                    end
                    local collectors = rawget(_G, 'observability_saved_collectors')
                    if collectors then
                        for collector, methods in pairs(collectors) do
                            for method, fn in pairs(methods) do collector[method] = fn end
                        end
                        rawset(_G, 'observability_saved_collectors', nil)
                    end
                    local bucket = rawget(_G, 'observability_saved_bucket')
                    if bucket then
                        require('vshard').router.bucket_id_strcrc32 = bucket
                        rawset(_G, 'observability_saved_bucket', nil)
                    end
                    local trigger = rawget(_G, 'cache_failure_trigger')
                    if trigger and box.space.forecast_cache then
                        box.space.forecast_cache:before_replace(nil, trigger)
                        rawset(_G, 'cache_failure_trigger', nil)
                    end
                    if box.space.forecast_cache and not box.info.ro then
                        require('app.roles.storage').apply_config(nil, {is_master = true})
                    end
                ]])
            end
        end
        cg.router.env.SSL_CERT_FILE = nil
        upstream.reset_resources(cg)
        M.configure(cg, cg.endpoint)
        cg.router.net_box:eval([[
            local endpoint = ...
            os.setenv('OPEN_METEO_FORECAST_URL', endpoint)
            os.setenv('OPEN_METEO_TIMEOUT_SECS', '30')
        ]], {cg.endpoint})
    end)
    g.after_all(function(cg)
        helper.stop_cluster(cg.cluster)
        cg.mock:stop()
    end)
end

return M
