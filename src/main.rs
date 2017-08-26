extern crate futures;
extern crate hyper;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use std::env;
use std::str;
use std::sync::Arc;

use hyper::server;
use futures::{
    Future,
    future,
};

mod models;
mod store;

struct Router {
    store: Arc<store::Store>,
}

impl Router {
    fn new(store: Arc<store::Store>) -> Self {
        Self {
            store: store,
        }
    }

    fn not_found() -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        future::ok(server::Response::new().with_status(hyper::StatusCode::NotFound)).boxed()
    }

    fn internal_error() -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        future::ok(server::Response::new().with_status(hyper::StatusCode::InternalServerError)).boxed()
    }

    fn get_user(&self, id: u32) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        match self.store.get_user(id) {
            Ok(user) =>
                match serde_json::to_string(&user) {
                    Ok(json) => future::ok(server::Response::new().with_body(json.to_string())).boxed(),
                    Err(_) => Self::internal_error(),
                },
            Err(store::StoreError::EntryExists) => Self::not_found(),
            Err(_) => Self::internal_error(),
        }
    }
}

impl server::Service for Router {
    type Request = server::Request;
    type Response = server::Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let mut path_parts = req.path().split('/');
        match (req.method(), path_parts.next(), path_parts.next(), path_parts.next(), path_parts.next()) {
            (_, _, _, _, Some(_)) => Self::not_found(),
            (&hyper::Method::Get, Some(entity), Some(id_src), None, None) =>
                match (entity, id_src.parse()) {
                    ("users", Ok(id)) => self.get_user(id),
                    _ => Self::not_found(),
                }
            _ => Self::not_found(),
        }
    }
}

const DEFAULT_LISTEN: &'static str = "127.0.0.1:9999";

fn main() {
    let address = env::var("LISTEN").unwrap_or(DEFAULT_LISTEN.to_string())
        .parse().unwrap();
    let store = Arc::new(store::Store::new());
    hyper::server::Http::new()
        .bind(&address, move || Ok(Router::new(store.clone()))).unwrap()
        .run().unwrap()
}
