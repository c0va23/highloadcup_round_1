extern crate futures;
extern crate hyper;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use std::env;

use hyper::server;
use futures::{
    Future,
    future,
};

mod models;
mod store;

struct Router<'a> {
    store: &'a store::Store,
}

impl<'a> Router<'a> {
    fn new(store: &'a store::Store) -> Self {
        Self {
            store: store,
        }
    }
}

impl<'a> server::Service for Router<'a> {
    type Request = server::Request;
    type Response = server::Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        future::ok(server::Response::new().with_status(hyper::StatusCode::NotFound)).boxed()
    }
}

const DEFAULT_LISTEN: &'static str = "127.0.0.1:9999";

fn main() {
    let address = env::var("LISTEN").unwrap_or(DEFAULT_LISTEN.to_string())
        .parse().unwrap();
    let store = store::Store::new();
    hyper::server::Http::new()
        .bind(&address, move || Ok(Router::new(&store))).unwrap()
        .run().unwrap()
}
