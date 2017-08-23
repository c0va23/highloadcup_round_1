extern crate futures;
extern crate hyper;

use std::env;

use hyper::server;
use futures::{
    Future,
    future,
};

mod models;

struct Router;

impl server::Service for Router {
    type Request = server::Request;
    type Response = server::Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, _req: Self::Request) -> Self::Future {
        future::ok(server::Response::new().with_body("Hello!")).boxed()
    }
}

const DEFAULT_LISTEN: &'static str = "127.0.0.1:9999";

fn main() {
    let address = env::var("LISTEN").unwrap_or(DEFAULT_LISTEN.to_string())
        .parse().unwrap();
    hyper::server::Http::new()
        .bind(&address, || Ok(Router)).unwrap()
        .run().unwrap()
}
