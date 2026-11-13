-- Exercise the actual role initialization in a fresh process without cluster state.
local json = require('json')
local root, output, workdir, replica = unpack(arg)
package.path = root .. '/?.lua;' .. root .. '/.rocks/share/tarantool/?.lua;' ..
    root .. '/.rocks/share/tarantool/?/init.lua;' .. package.path
package.cpath = root .. '/?.so;' .. root .. '/.rocks/lib/tarantool/?.so;' .. package.cpath
box.cfg({work_dir = workdir, wal_mode = 'none', log = '/dev/null'})
local cartridge = require('cartridge')
-- No HTTP server is needed to validate role startup in this isolated process.
cartridge.service_get = function() return {route = function() end} end
local ok, err = pcall(require('app.roles.custom').init, {is_master = replica ~= 'replica'})
local file = assert(io.open(output, 'w'))
file:write(json.encode({ok = ok, error = ok and '' or tostring(err)}))
file:close()
os.exit(0)
