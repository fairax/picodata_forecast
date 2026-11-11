-- Local-only upstream faults, run in a separate Tarantool process.
local fiber = require('fiber')
local fio = require('fio')
local json = require('json')
local socket = require('socket')
local mode, directory = unpack(arg)
local ssl, context

local function quote(value)
    return "'" .. value:gsub("'", "'\\''") .. "'"
end

if mode == 'certificate' then
    local ffi = require('ffi')
    ffi.cdef([[
        typedef struct ssl_ctx_st SSL_CTX;
        typedef struct ssl_st SSL;
        typedef struct ssl_method_st SSL_METHOD;
        const SSL_METHOD *TLS_server_method(void);
        SSL_CTX *SSL_CTX_new(const SSL_METHOD *method);
        void SSL_CTX_free(SSL_CTX *ctx);
        int SSL_CTX_use_certificate_file(SSL_CTX *ctx, const char *file, int type);
        int SSL_CTX_use_PrivateKey_file(SSL_CTX *ctx, const char *file, int type);
        SSL *SSL_new(SSL_CTX *ctx);
        void SSL_free(SSL *ssl);
        int SSL_set_fd(SSL *ssl, int fd);
        int SSL_accept(SSL *ssl);
        int SSL_get_error(const SSL *ssl, int result);
        int SSL_write(SSL *ssl, const void *buffer, int size);
    ]])
    local key = fio.pathjoin(directory, 'key.pem')
    local cert = fio.pathjoin(directory, 'cert.pem')
    assert(os.execute('openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost' ..
        ' -keyout ' .. quote(key) .. ' -out ' .. quote(cert) .. ' >/dev/null 2>&1') == 0)
    -- Use Tarantool's exported OpenSSL symbols, avoiding a second incompatible library.
    ssl = ffi.C
    context = ffi.gc(assert(ssl.SSL_CTX_new(ssl.TLS_server_method())), ssl.SSL_CTX_free)
    assert(ssl.SSL_CTX_use_certificate_file(context, cert, 1) == 1)
    assert(ssl.SSL_CTX_use_PrivateKey_file(context, key, 1) == 1)
end

-- OpenSSL uses nonblocking sockets: yield only when the handshake/write needs I/O.
local function tls_call(connection, session, operation, ...)
    local deadline = fiber.clock() + 5
    while true do
        local result = operation(session, ...)
        if result > 0 then return result end
        local err = ssl.SSL_get_error(session, result)
        local remaining = deadline - fiber.clock()
        if remaining <= 0 then return nil end
        if err == 2 then -- SSL_ERROR_WANT_READ
            if not connection:readable(remaining) then return nil end
        elseif err == 3 then -- SSL_ERROR_WANT_WRITE
            if not connection:writable(remaining) then return nil end
        else
            return nil -- Includes the expected client certificate-validation alert.
        end
    end
end

local server
local count = 0
local function publish()
    local temporary = fio.pathjoin(directory, 'state.tmp')
    local file = assert(io.open(temporary, 'w'))
    file:write(json.encode({port = server:name().port, count = count}))
    file:close()
    assert(fio.rename(temporary, fio.pathjoin(directory, 'state.json')))
end

local ok_response = 'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok'
local function handle(connection)
    count = count + 1
    publish()
    if context then
        local ffi = require('ffi')
        local session = ffi.gc(assert(ssl.SSL_new(context)), ssl.SSL_free)
        assert(ssl.SSL_set_fd(session, connection:fd()) == 1)
        if tls_call(connection, session, ssl.SSL_accept) then
            tls_call(connection, session, ssl.SSL_write, ok_response, #ok_response)
        end
        ffi.gc(session, nil)
        ssl.SSL_free(session)
        return
    end
    if mode == 'reset-always' or (mode == 'reset' and count == 1) then
        connection:linger(true, 0) -- Abortive close sends TCP RST instead of FIN.
        return
    end
    if not connection:read({delimiter = '\r\n\r\n'}, 5) then return end
    if mode == 'slow-body' then
        if not connection:write('HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n', 5) then return end
        fiber.sleep(2)
        connection:write('ok', 5)
    elseif mode == 'chunked-oversized' then
        if not connection:write('HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n', 5) then
            return
        end
        fiber.sleep(0.5)
        local body = string.rep('x', 1048577)
        connection:write(string.format('%x\r\n', #body) .. body .. '\r\n0\r\n\r\n', 5)
    else
        connection:write(ok_response, 5)
    end
end

server = assert(socket.tcp_server('127.0.0.1', 0, function(connection)
    local ok, err = pcall(handle, connection)
    connection:close()
    if not ok then error(err) end
end))
publish()
fiber.sleep(math.huge)
