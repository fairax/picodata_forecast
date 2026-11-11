-- Exercise the actual role initialization in a fresh process without cluster state.
local json = require('json')
local root, output, workdir, replica = unpack(arg)
package.path = root .. '/?.lua;' .. package.path
package.cpath = root .. '/?.so;' .. package.cpath
box.cfg({work_dir = workdir, wal_mode = 'none', log = '/dev/null'})
local ok, err = pcall(require('app.roles.storage').init, {is_master = replica ~= 'replica'})
local file = assert(io.open(output, 'w'))
file:write(json.encode({ok = ok, error = ok and '' or tostring(err)}))
file:close()
os.exit(0)
