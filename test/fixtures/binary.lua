-- Preserve MessagePack binary when seeding tuples: Tarantool 2.11 otherwise
-- encodes Lua byte strings as MessagePack strings.
local msgpack = require('msgpack')

return function(bytes)
    local size = #bytes
    local header = string.char(0xc6, math.floor(size / 16777216) % 256,
        math.floor(size / 65536) % 256, math.floor(size / 256) % 256, size % 256)
    return msgpack.object_from_raw(header .. bytes)
end
