use shors::transport::http::{route::Builder, server::Server, Request, Response};
use shors::transport::Context;
use std::error::Error;

#[tarantool::proc]
pub fn router_init() -> tarantool::Result<bool> {
    let route = Builder::new()
        .with_method("GET")
        .with_path("/forecast")
        .build(
            |_: &mut Context, _: Request| -> Result<Response, Box<dyn Error>> {
                Ok(Response {
                    status: 200,
                    headers: Default::default(),
                    body: b"Hello world!".to_vec(),
                })
            },
        );
    Server::new().register(Box::new(route));
    Ok(true)
}
