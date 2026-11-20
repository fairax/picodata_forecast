//! Local patch: explicit HTTP proxy support; proxied sockets are never pooled.
use crate::{error::Error, request, response};
use futures_lite::io::{self, AsyncReadExt, AsyncWriteExt};
use std::{fmt, time::Duration};
use tarantool::{
    fiber,
    fiber::r#async::timeout::{self, IntoTimeout},
    network::client::tcp,
};

#[derive(Clone)]
pub struct Proxy {
    host: String,
    port: u16,
    authorization: Option<String>,
}
impl fmt::Debug for Proxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Proxy")
            .field("host", &self.host)
            .field("port", &self.port)
            .finish()
    }
}
impl Proxy {
    /// HTTP proxy URL with optional percent-encoded Basic credentials.
    pub fn http(value: &str) -> Result<Self, Box<Error>> {
        let invalid = || Box::new(Error::ProxyConfig);
        let url = url::Url::parse(value).map_err(|_| invalid())?;
        if url.scheme() != "http"
            || url.host_str().is_none()
            || !matches!(url.path(), "" | "/")
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid());
        }
        let authorization = if !url.username().is_empty() || url.password().is_some() {
            let user = percent_encoding::percent_decode_str(url.username())
                .decode_utf8()
                .map_err(|_| invalid())?;
            let password = percent_encoding::percent_decode_str(url.password().unwrap_or(""))
                .decode_utf8()
                .map_err(|_| invalid())?;
            Some(
                crate::utils::basic_auth(user, Some(password))
                    .as_str()
                    .to_owned(),
            )
        } else {
            None
        };
        Ok(Self {
            host: url
                .host_str()
                .ok_or_else(invalid)?
                .trim_matches(['[', ']'])
                .to_owned(),
            port: url.port_or_known_default().ok_or_else(invalid)?,
            authorization,
        })
    }
}

// Read the bounded CONNECT head byte-by-byte to avoid consuming TLS tunnel bytes.
async fn tunnel(
    stream: &mut tcp::TcpStream,
    authority: &str,
    auth: Option<&str>,
) -> Result<(), Box<Error>> {
    let mut head = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some(auth) = auth {
        head.push_str(&format!("Proxy-Authorization: {auth}\r\n"));
    }
    head.push_str("\r\n");
    stream
        .write_all(head.as_bytes())
        .await
        .map_err(|e| Box::new(Error::IO(e)))?;
    for _ in 0..8 {
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            if head.len() >= 16384 {
                return Err(Box::new(Error::ProxyProtocol));
            }
            let mut byte = [0];
            stream
                .read_exact(&mut byte)
                .await
                .map_err(|e| Box::new(Error::IO(e)))?;
            head.push(byte[0]);
        }
        let text = std::str::from_utf8(&head).map_err(|_| Box::new(Error::ProxyProtocol))?;
        let mut parts = text.lines().next().unwrap_or("").split_whitespace();
        if !matches!(parts.next(), Some("HTTP/1.0" | "HTTP/1.1")) {
            return Err(Box::new(Error::ProxyProtocol));
        }
        let status = parts
            .next()
            .and_then(|s| s.parse::<u16>().ok())
            .ok_or_else(|| Box::new(Error::ProxyProtocol))?;
        if (200..300).contains(&status) {
            return Ok(());
        }
        if !(100..200).contains(&status) {
            return Err(Box::new(Error::ProxyStatus(status)));
        }
    }
    Err(Box::new(Error::ProxyProtocol))
}

pub(crate) fn execute(
    proxy: &Proxy,
    request: request::Request,
    connect_timeout: Duration,
) -> Result<response::Response, Box<Error>> {
    let (mut request, headers, body, tls_timeout, request_timeout, response_timeout) =
        request.pieces();
    for (name, value) in headers {
        request.insert_header(name, &value);
    }
    if let Some(body) = body {
        request.set_body(body);
    }
    let url = request.url().clone();
    if !matches!(url.scheme(), "http" | "https") {
        return Err(Box::new(Error::UnsupportedSchema));
    }
    let host = url
        .host_str()
        .ok_or(Error::EmptyHost)?
        .trim_matches(['[', ']']);
    let mut stream = tcp::TcpStream::connect_timeout(&proxy.host, proxy.port, connect_timeout)
        .map_err(|e| Box::new(Error::TCP(e)))?;
    // Never send proxy credentials to the TLS origin.
    request.remove_header("proxy-authorization");
    let exchange = async {
        if url.scheme() == "https" {
            let authority = match url.host() {
                Some(url::Host::Ipv6(ip)) => {
                    format!("[{ip}]:{}", url.port_or_known_default().unwrap_or(443))
                }
                _ => format!("{host}:{}", url.port_or_known_default().unwrap_or(443)),
            };
            tunnel(&mut stream, &authority, proxy.authorization.as_deref()).await?;
            let stream = async_native_tls::connect(host, stream)
                .timeout(tls_timeout)
                .await
                .map_err(|e| match e {
                    timeout::Error::Expired => Box::new(Error::Timeout),
                    timeout::Error::Failed(e) => Box::new(Error::TLS(e)),
                })?;
            async_h1::connect(stream, request)
                .await
                .map_err(|e| Box::new(Error::HTTP(e)))
        } else {
            if let Some(auth) = &proxy.authorization {
                request.insert_header("proxy-authorization", auth.as_str());
            }
            let method = request.method();
            let mut target = url.clone();
            let _ = target.set_username("");
            let _ = target.set_password(None);
            target.set_fragment(None);
            // Preserve async-h1's header/body encoder, changing only the request
            // target from origin-form to absolute-form for the forward proxy.
            let mut encoded = async_h1::client::Encoder::new(request);
            let mut line = Vec::new();
            while !line.ends_with(b"\r\n") {
                if line.len() >= 16384 {
                    return Err(Box::new(Error::ProxyProtocol));
                }
                let mut byte = [0];
                encoded
                    .read_exact(&mut byte)
                    .await
                    .map_err(|e| Box::new(Error::IO(e)))?;
                line.push(byte[0]);
            }
            stream
                .write_all(format!("{method} {target} HTTP/1.1\r\n").as_bytes())
                .await
                .map_err(|e| Box::new(Error::IO(e)))?;
            io::copy(&mut encoded, &mut stream)
                .await
                .map_err(|e| Box::new(Error::IO(e)))?;
            let response = async_h1::client::decode(stream)
                .await
                .map_err(|e| Box::new(Error::HTTP(e)))?;
            if response.status() as u16 == 407 {
                return Err(Box::new(Error::ProxyStatus(407)));
            }
            Ok(response)
        }
    };
    let inner = fiber::block_on(exchange.timeout(request_timeout)).map_err(|e| match e {
        timeout::Error::Expired => Box::new(Error::Timeout),
        timeout::Error::Failed(e) => e,
    })?;
    Ok(response::Response::new(url, inner, response_timeout))
}
