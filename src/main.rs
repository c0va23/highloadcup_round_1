extern crate futures;
extern crate hyper;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate net2;
extern crate tokio_core;

use std::env;
use std::str;
use std::sync::Arc;
use std::thread;

use hyper::server;
use futures::{
    Future,
    future,
    Stream,
};

use net2::unix::UnixTcpBuilderExt;

mod models;
mod store;

#[derive(Debug)]
enum AppError {
    JsonError(serde_json::Error),
    StoreError(store::StoreError),
}

impl From<store::StoreError> for AppError {
    fn from(err: store::StoreError) -> AppError {
        AppError::StoreError(err)
    }
}

#[derive(Clone)]
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

    fn app_error(err: AppError) -> server::Response {
        match err {
            AppError::JsonError(_) =>
                server::Response::new().with_status(hyper::StatusCode::BadRequest),
            AppError::StoreError(store::StoreError::EntryExists) =>
                server::Response::new().with_status(hyper::StatusCode::BadRequest),
            AppError::StoreError(store::StoreError::EntityNotExists) =>
                server::Response::new().with_status(hyper::StatusCode::NotFound),
            AppError::StoreError(_) =>
                server::Response::new().with_status(hyper::StatusCode::InternalServerError),
        }
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

    fn add_user(self, req: server::Request) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(req.body().concat2()
            .and_then(move |chunk: hyper::Chunk|
                serde_json::from_slice(&chunk)
                    .map_err(AppError::JsonError)
                    .and_then(|user| Ok(self.store.add_user(user)?))
                    .map(|_| Ok(server::Response::new().with_body("{}")))
                    .unwrap_or_else(|err| {
                        error!("Request error: {:?}", err);
                        Ok(Self::app_error(err))
                    })
            )
        )
    }

    fn update_user(self, id: u32, req: server::Request) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(req.body().concat2()
            .and_then(move |chunk: hyper::Chunk|
                serde_json::from_slice(&chunk)
                    .map_err(AppError::JsonError)
                    .and_then(|user| Ok(self.store.update_user(id, user)?))
                    .map(|_| Ok(server::Response::new().with_body("{}")))
                    .unwrap_or_else(|err| {
                        error!("Request error: {:?}", err);
                        Ok(Self::app_error(err))
                    })
            )
        )
    }
}

impl server::Service for Router {
    type Request = server::Request;
    type Response = server::Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let path = req.path().to_string();
        let mut path_parts = path.split('/').skip(1);

        let result = match (req.method(), path_parts.next(), path_parts.next(), path_parts.next(),
                path_parts.next()) {
            (_, _, _, _, Some(_)) => Self::not_found(),
            (&hyper::Method::Get, Some(entity), Some(id_src), None, None) =>
                match (entity, id_src.parse()) {
                    ("users", Ok(id)) => self.get_user(id),
                    _ => Self::not_found(),
                }
            (&hyper::Method::Post, Some(entity), Some("new"), None, None) =>
                match entity {
                    "users" => self.clone().add_user(req),
                    _ => Self::not_found(),
                },
            (&hyper::Method::Post, Some(entity), Some(id_src), None, None) =>
                match (entity, id_src.parse()) {
                    ("users", Ok(id)) => self.clone().update_user(id, req),
                    _ => Self::not_found(),
                }
            _ => Self::not_found(),
        }.map(|response|
            response.with_header(
                hyper::header::Connection(
                    vec!(hyper::header::ConnectionOption::KeepAlive)
                )
            )
        );

        Box::new(result)
    }
}

const DEFAULT_LISTEN: &'static str = "127.0.0.1:9999";
const DEFAULT_THREADS: &'static str = "1";
const DEFAULT_BACKLOG: &'static str = "1024";

fn main() {
    env_logger::init().unwrap();

    let address = env::var("LISTEN").unwrap_or(DEFAULT_LISTEN.to_string())
        .parse().unwrap();
    let thread_count = env::var("THREADS").unwrap_or(DEFAULT_THREADS.to_string())
        .parse::<usize>().unwrap();
    let backlog = env::var("BACKLOG").unwrap_or(DEFAULT_BACKLOG.to_string())
        .parse::<i32>().unwrap();

    info!("Start listen {} on {} threads with backlog", address, thread_count);

    let store = Arc::new(store::Store::new());

    let threads = (0..thread_count).map(move |thread_index|{
        let store = store.clone();
        thread::Builder::new()
            .name(format!("Server {}", thread_index))
            .spawn(move || {
                info!("Start thread {}", thread_index);
                let net_listener = net2::TcpBuilder::new_v4().unwrap()
                    .reuse_port(true).unwrap()
                    .bind(address).unwrap()
                    .listen(backlog).unwrap();

                let mut core = tokio_core::reactor::Core::new().unwrap();
                let handle = core.handle();

                let core_listener = tokio_core::net::TcpListener::from_listener(net_listener, &address, &handle).unwrap();

                core.run(
                    core_listener.incoming().for_each(move |(stream, socket_addr)| {
                        info!("Connection on thread #{} from {}", thread_index, socket_addr);
                        hyper::server::Http::new()
                            .keep_alive(true)
                            .bind_connection(&handle, stream, socket_addr, Router::new(store.clone()));
                        Ok(())
                    })
                )
            }).unwrap()
    }).collect::<Vec<_>>();

    for join_handler in threads {
        if let Err(err) = join_handler.join() {
            error!("Thread error {:?}", err);
        }
    }
}
