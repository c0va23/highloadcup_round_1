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

struct PathParts<'a> {
    entity: &'a str,
    id: Option<u32>,
    action: Option<&'a str>,
}

impl<'a> Router<'a> {
    fn new(store: &'a store::Store) -> Self {
        Self {
            store: store,
        }
    }

    fn parse_path(path: &str) -> Result<PathParts, ()> {
        let parts = path.split("/");
        if 2 <= parts.clone().count() && parts.clone().count() <= 3 {
            match parts.clone().nth(1).unwrap().parse() {
                Ok(id) => Ok(PathParts{
                    entity: parts.clone().nth(0).unwrap(),
                    id: Some(id),
                    action: parts.clone().nth(2),
                }),
                Err(_) => Ok(PathParts {
                    entity: parts.clone().nth(0).unwrap(),
                    id: None,
                    action: parts.clone().nth(1),
                }),
            }
        } else {
            return Err(())
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

impl<'a> server::Service for Router<'a> {
    type Request = server::Request;
    type Response = server::Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        match (req.method(), Self::parse_path(req.path())) {
            (&hyper::Method::Get, Ok(PathParts{ action: None, id: Some(id), entity: "users" })) =>
                self.get_user(id),
            _ => Self::not_found(),
        }
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
