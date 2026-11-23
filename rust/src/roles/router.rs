use shors::transport::http::{route::Builder, server::Server, Request, Response};
use shors::transport::Context;
use std::error::Error;
use std::time::Duration;

const OPEN_METEO_FORECAST_URL: &str = "https://127.0.0.1:8443/v1/forecast";
const OPEN_METEO_TIMEOUT: Duration = Duration::from_secs(10);

#[tarantool::proc]
pub fn router_init() -> tarantool::Result<bool> {
    let route = Builder::new()
        .with_method("GET")
        .with_path("/forecast")
        .build(
            |_: &mut Context, request: Request| -> Result<Response, Box<dyn Error>> {
                Ok(fetch_forecast(&request.query))
            },
        );
    Server::new().register(Box::new(route));
    Ok(true)
}

fn fetch_forecast(query: &str) -> Response {
    let url = if query.is_empty() {
        OPEN_METEO_FORECAST_URL.to_owned()
    } else {
        format!("{OPEN_METEO_FORECAST_URL}?{query}")
    };
    let client = fibreq::ClientBuilder::new().build();
    let result = client.get(url).and_then(|request| {
        request
            .tls_timeout(OPEN_METEO_TIMEOUT)
            .request_timeout(OPEN_METEO_TIMEOUT)
            .response_timeout(OPEN_METEO_TIMEOUT)
            .send()?
            .bytes()
    });

    match result {
        Ok(body) => Response {
            status: 200,
            headers: Default::default(),
            body,
        },
        Err(err) if matches!(*err, fibreq::Error::Timeout) => {
            gateway_error(504, "Open-Meteo request timed out")
        }
        _ => gateway_error(502, "Open-Meteo request failed"),
    }
}

fn gateway_error(status: u32, message: &str) -> Response {
    Response {
        status,
        headers: [("content-type".into(), "text/plain; charset=utf-8".into())].into(),
        body: message.as_bytes().to_vec(),
    }
}
