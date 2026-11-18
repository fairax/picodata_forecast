-- This file is required automatically by luatest.
-- Add common configuration here.

local fio = require('fio')
local t = require('luatest')

local helper = {}

helper.root = fio.dirname(fio.abspath(package.search('init')))
package.path = helper.root .. '/?.lua;' .. helper.root .. '/?/init.lua;' .. package.path
helper.datadir = assert(fio.tempdir())
helper.server_command = fio.pathjoin(helper.root, 'init.lua')

function helper.truncate_space_on_cluster(cluster, space_name)
    assert(cluster ~= nil)
    for _, server in ipairs(cluster.servers) do
        server.net_box:eval([[
            local space_name = ...
            local space = box.space[space_name]
            if space ~= nil and not box.cfg.read_only then
                space:truncate()
            end
        ]], {space_name})
    end
end

function helper.drop_space_on_cluster(cluster, space_name)
    assert(cluster ~= nil)
    for _, server in ipairs(cluster.servers) do
        server.net_box:eval([[
            local space_name = ...
            local space = box.space[space_name]
            if space ~= nil and not box.cfg.read_only then
                space:drop()
            end
        ]], {space_name})
    end
end

function helper.stop_cluster(cluster)
    assert(cluster ~= nil)
    cluster:stop()
    fio.rmtree(cluster.datadir)
end

t.before_suite(function()
    box.cfg({work_dir = helper.datadir})
end)

t.after_suite(function()
    fio.chdir(helper.root)
    fio.rmtree(helper.datadir)
end)

return helper
