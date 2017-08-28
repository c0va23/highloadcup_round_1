extern crate futures;
extern crate hyper;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde_urlencoded;

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate net2;
extern crate tokio_core;

extern crate zip;

use std::env;
use std::str;
use std::sync::Arc;
use std::thread;

use hyper::server;
use hyper::mime;

use futures::{
    Future,
    future,
    Stream,
};

use net2::unix::UnixTcpBuilderExt;

mod models;
mod store;
mod loader;

#[derive(Debug)]
enum AppError {
    JsonError(serde_json::Error),
    StoreError(store::StoreError),
    ParamsError(serde_urlencoded::de::Error),
    ParamsMissed,
    NullValue,
}

impl From<store::StoreError> for AppError {
    fn from(err: store::StoreError) -> AppError {
        AppError::StoreError(err)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(err: serde_json::Error) -> AppError {
        AppError::JsonError(err)
    }
}

impl From<serde_urlencoded::de::Error> for AppError {
    fn from(err: serde_urlencoded::de::Error) -> AppError {
        AppError::ParamsError(err)
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

    fn app_error(err: AppError) -> server::Response {
        error!("{:?}", err);
        match err {
            AppError::JsonError(_) =>
                server::Response::new().with_status(hyper::StatusCode::BadRequest),
            AppError::StoreError(store::StoreError::EntryExists) |
                    AppError::StoreError(store::StoreError::InvalidEntity) |
                    AppError::NullValue =>
                server::Response::new().with_status(hyper::StatusCode::BadRequest),
            AppError::ParamsMissed | AppError::ParamsError(_) =>
                server::Response::new().with_status(hyper::StatusCode::BadRequest),
            AppError::StoreError(store::StoreError::EntityNotExists) =>
                server::Response::new().with_status(hyper::StatusCode::NotFound),
            AppError::StoreError(_) =>
                server::Response::new().with_status(hyper::StatusCode::InternalServerError),
        }
    }

    fn get_user(&self, id: u32) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(self.store.get_user(id)
            .map_err(AppError::StoreError)
            .and_then(|user| Ok(serde_json::to_string(&user)?))
            .map(|json| {
                let length = json.len() as u64;
                future::ok(server::Response::new().with_body(json)
                    .with_header(hyper::header::ContentType(mime::APPLICATION_JSON))
                    .with_header(hyper::header::ContentLength(length))
                )
            })
            .unwrap_or_else(|err| future::ok(Self::app_error(err)))
        )
    }

    fn add_user(self, req: server::Request) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(req.body().concat2()
            .and_then(move |chunk|
                serde_json::from_slice(&chunk)
                    .map_err(AppError::JsonError)
                    .and_then(|map: serde_json::map::Map<String, serde_json::value::Value>|
                        if map.values().find(|v| **v == serde_json::value::Value::Null).is_some() {
                            Err(AppError::NullValue)
                        } else {
                            Ok(serde_json::value::Value::Object(map))
                        }
                    )
                    .and_then(|value| Ok(serde_json::from_value(value)?))
                    .and_then(|user| Ok(self.store.add_user(user)?))
                    .map(|_|
                        Ok(server::Response::new().with_body("{}")
                            .with_header(hyper::header::ContentType(mime::APPLICATION_JSON))
                            .with_header(hyper::header::ContentLength(2))
                        )
                    )
                    .unwrap_or_else(|err| Ok(Self::app_error(err)))
            )
        )
    }

    fn update_user(self, id: u32, req: server::Request) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(req.body().concat2()
            .and_then(move |chunk|
                serde_json::from_slice(&chunk)
                    .map_err(AppError::JsonError)
                    .and_then(|map: serde_json::map::Map<String, serde_json::value::Value>|
                        if map.values().find(|v| **v == serde_json::value::Value::Null).is_some() {
                            Err(AppError::NullValue)
                        } else {
                            Ok(serde_json::value::Value::Object(map))
                        }
                    )
                    .and_then(|value| Ok(serde_json::from_value(value)?))
                    .and_then(|user| Ok(self.store.update_user(id, user)?))
                    .map(|_|
                        Ok(server::Response::new().with_body("{}")
                            .with_header(hyper::header::ContentType(mime::APPLICATION_JSON))
                            .with_header(hyper::header::ContentLength(2))
                        )
                    )
                    .unwrap_or_else(|err| Ok(Self::app_error(err)))
            )
        )
    }

    fn get_location(&self, id: models::Id) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(self.store.get_location(id)
            .map_err(AppError::StoreError)
            .and_then(|location| Ok(serde_json::to_string(&location)?))
            .map(|json| {
                let length = json.len() as u64;
                future::ok(server::Response::new().with_body(json)
                    .with_header(hyper::header::ContentType(mime::APPLICATION_JSON))
                    .with_header(hyper::header::ContentLength(length))
                )
            })
            .unwrap_or_else(|err| future::ok(Self::app_error(err)))
        )
    }

    fn add_location(self, req: server::Request) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(req.body().concat2()
            .and_then(move |chunk|
                serde_json::from_slice(&chunk)
                    .map_err(AppError::JsonError)
                    .and_then(|map: serde_json::map::Map<String, serde_json::value::Value>|
                        if map.values().find(|v| **v == serde_json::value::Value::Null).is_some() {
                            Err(AppError::NullValue)
                        } else {
                            Ok(serde_json::value::Value::Object(map))
                        }
                    )
                    .and_then(|value| Ok(serde_json::from_value(value)?))
                    .and_then(|location| Ok(self.store.add_location(location)?))
                    .map(|_|
                        Ok(server::Response::new().with_body("{}")
                            .with_header(hyper::header::ContentType(mime::APPLICATION_JSON))
                            .with_header(hyper::header::ContentLength(2))
                        )
                    )
                    .unwrap_or_else(|err| Ok(Self::app_error(err)))
            )
        )
    }

    fn update_location(self, id: models::Id, req: server::Request) ->
            Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(req.body().concat2()
            .and_then(move |chunk|
                serde_json::from_slice(&chunk)
                    .map_err(AppError::JsonError)
                    .and_then(|map: serde_json::map::Map<String, serde_json::value::Value>|
                        if map.values().find(|v| **v == serde_json::value::Value::Null).is_some() {
                            Err(AppError::NullValue)
                        } else {
                            Ok(serde_json::value::Value::Object(map))
                        }
                    )
                    .and_then(|value| Ok(serde_json::from_value(value)?))
                    .and_then(|location_data| Ok(self.store.update_location(id, location_data)?))
                    .map(|_|
                        Ok(server::Response::new().with_body("{}")
                            .with_header(hyper::header::ContentType(mime::APPLICATION_JSON))
                            .with_header(hyper::header::ContentLength(2))
                        )
                    )
                    .unwrap_or_else(|err| Ok(Self::app_error(err)))
            )
        )
    }

    fn get_visit(&self, id: models::Id) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(self.store.get_visit(id)
            .map_err(AppError::StoreError)
            .and_then(|visit| Ok(serde_json::to_string(&visit)?))
            .map(|json| {
                let length = json.len() as u64;
                future::ok(server::Response::new().with_body(json)
                    .with_header(hyper::header::ContentType(mime::APPLICATION_JSON))
                    .with_header(hyper::header::ContentLength(length))
                )
            })
            .unwrap_or_else(|err| future::ok(Self::app_error(err)))
        )
    }

    fn add_visit(self, req: server::Request) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(req.body().concat2()
            .and_then(move |chunk|
                serde_json::from_slice(&chunk)
                    .map_err(AppError::JsonError)
                    .and_then(|map: serde_json::map::Map<String, serde_json::value::Value>|
                        if map.values().find(|v| **v == serde_json::value::Value::Null).is_some() {
                            Err(AppError::NullValue)
                        } else {
                            Ok(serde_json::value::Value::Object(map))
                        }
                    )
                    .and_then(|value| Ok(serde_json::from_value(value)?))
                    .and_then(|visit| Ok(self.store.add_visit(visit)?))
                    .map(|_|
                        Ok(server::Response::new().with_body("{}")
                            .with_header(hyper::header::ContentType(mime::APPLICATION_JSON))
                            .with_header(hyper::header::ContentLength(2))
                        )
                    )
                    .unwrap_or_else(|err| Ok(Self::app_error(err)))
            )
        )
    }

    fn update_visit(self, id: models::Id, req: server::Request) ->
            Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(req.body().concat2()
            .and_then(move |chunk: hyper::Chunk|
                serde_json::from_slice(&chunk)
                    .map_err(AppError::JsonError)
                    .and_then(|map: serde_json::map::Map<String, serde_json::value::Value>|
                        if map.values().find(|v| **v == serde_json::value::Value::Null).is_some() {
                            Err(AppError::NullValue)
                        } else {
                            Ok(serde_json::value::Value::Object(map))
                        }
                    )
                    .and_then(|value| Ok(serde_json::from_value(value)?))
                    .and_then(|visit_data| Ok(self.store.update_visit(id, visit_data)?))
                    .map(|_|
                        Ok(server::Response::new().with_body("{}")
                            .with_header(hyper::header::ContentType(mime::APPLICATION_JSON))
                            .with_header(hyper::header::ContentLength(2))
                        )
                    )
                    .unwrap_or_else(|err| Ok(Self::app_error(err)))
            )
        )
    }

    fn find_user_visits(&self, user_id: models::Id, req: server::Request) ->
            Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(
            req.query().or(Some(""))
                .ok_or(AppError::ParamsMissed) // TODO: Remove it
                .and_then(|query| Ok(serde_urlencoded::from_str(query)?))
                .and_then(|options| Ok(self.store.find_user_visits(user_id, options)?))
                .and_then(|user_visits| Ok(serde_json::to_string(&user_visits)?))
                .map(|json| {
                    let length = json.len() as u64;
                    future::ok(server::Response::new().with_body(json)
                        .with_header(hyper::header::ContentType(mime::APPLICATION_JSON))
                        .with_header(hyper::header::ContentLength(length))
                    )
                })
                .unwrap_or_else(|err| future::ok(Self::app_error(err)))
        )
    }

    fn get_location_rating(&self, user_id: models::Id, req: server::Request) ->
            Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(
            req.query().or(Some(""))
                .ok_or(AppError::ParamsMissed) // TODO: Remove it
                .and_then(|query| Ok(serde_urlencoded::from_str(query)?))
                .and_then(|options| Ok(self.store.get_location_rating(user_id, options)?))
                .and_then(|location_rating| Ok(serde_json::to_string(&location_rating)?))
                .map(|json| {
                    let length = json.len() as u64;
                    future::ok(server::Response::new().with_body(json)
                        .with_header(hyper::header::ContentType(mime::APPLICATION_JSON))
                        .with_header(hyper::header::ContentLength(length))
                    )
                })
                .unwrap_or_else(|err| future::ok(Self::app_error(err)))
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
            (&hyper::Method::Get, Some(entity), Some(id_src), action, None) =>
                match (entity, id_src.parse(), action) {
                    ("users", Ok(id), None) => self.get_user(id),
                    ("users", Ok(id), Some("visits")) => self.find_user_visits(id, req),
                    ("locations", Ok(id), None) => self.get_location(id),
                    ("locations", Ok(id), Some("avg")) => self.get_location_rating(id, req),
                    ("visits", Ok(id), None) => self.get_visit(id),
                    _ => Self::not_found(),
                }
            (&hyper::Method::Post, Some(entity), Some("new"), None, None) =>
                match entity {
                    "users" => self.clone().add_user(req),
                    "locations" => self.clone().add_location(req),
                    "visits" => self.clone().add_visit(req),
                    _ => Self::not_found(),
                },
            (&hyper::Method::Post, Some(entity), Some(id_src), None, None) =>
                match (entity, id_src.parse()) {
                    ("users", Ok(id)) => self.clone().update_user(id, req),
                    ("locations", Ok(id)) => self.clone().update_location(id, req),
                    ("visits", Ok(id)) => self.clone().update_visit(id, req),
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
    let data_path = env::var("DATA_PATH").unwrap();

    info!("Start listen {} on {} threads with backlog", address, thread_count);

    let store = Arc::new(store::Store::new());

    loader::load_data(store.clone(), &data_path).unwrap();

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
