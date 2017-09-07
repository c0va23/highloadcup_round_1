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
extern crate futures_cpupool;

extern crate zip;

extern crate chrono;

extern crate fnv;

use std::env;
use std::str;
use std::sync::Arc;
use std::thread;
use std::time;

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

const STREAM_KEEPALIVE_SECS: Option<u64> = Some(30);

#[derive(Debug)]
enum AppError {
    HyperError(hyper::Error),
    JsonError(serde_json::Error),
    StoreError(store::StoreError),
    ParamsError(serde_urlencoded::de::Error),
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
    cpupool: Arc<futures_cpupool::CpuPool>,
}

impl Router {
    fn new(
        store: Arc<store::Store>,
        cpupool: Arc<futures_cpupool::CpuPool>,
    ) -> Self {
        Self {
            store: store,
            cpupool: cpupool,
        }
    }

    fn not_found() -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        future::ok(server::Response::new().with_status(hyper::StatusCode::NotFound)).boxed()
    }

    fn app_error(err: AppError) -> server::Response {
        error!("{:?}", err);
        let status_code = match err {
            AppError::JsonError(_) =>
                hyper::StatusCode::BadRequest,
            AppError::StoreError(store::StoreError::EntryExists) |
            AppError::StoreError(store::StoreError::InvalidEntity(_)) |
            AppError::NullValue =>
                hyper::StatusCode::BadRequest,
            AppError::ParamsError(_) =>
                hyper::StatusCode::BadRequest,
            AppError::StoreError(store::StoreError::EntityNotExists) =>
                hyper::StatusCode::NotFound,
            AppError::StoreError(_) | AppError::HyperError(_) =>
                hyper::StatusCode::InternalServerError,
        };
        server::Response::new().with_status(status_code)
    }

    fn format_response<E>(result: Result<E, AppError>) ->
        Box<Future<Item = server::Response, Error = hyper::Error>>
    where
        E: serde::ser::Serialize,
    {
        Box::new(result
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

    fn parse_params<P>(query: Option<&str>) -> future::FutureResult<P, AppError>
    where P: serde::de::DeserializeOwned
    {
        future::result(serde_urlencoded::from_str(query.unwrap_or(""))
            .map_err(AppError::ParamsError))
    }

    fn check_json_value(map: serde_json::map::Map<String, serde_json::value::Value>) ->
        Result<serde_json::Value, AppError>
    {
        if map.values().find(|v| **v == serde_json::value::Value::Null).is_some() {
            Err(AppError::NullValue)
        } else {
            Ok(serde_json::value::Value::Object(map))
        }
    }

    fn parse_body(body: hyper::Body) -> Box<Future<Item = serde_json::Value, Error = AppError>> {
        Box::new(body.concat2().map_err(AppError::HyperError)
            .and_then(move |chunk| Ok(serde_json::from_slice(&chunk)?))
            .and_then(Self::check_json_value)
        )
    }

    fn get_location(self, id: models::Id) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(self.cpupool.clone()
            .spawn_fn(move || Ok(self.store.get_location(id)?))
            .then(Self::format_response)
        )
    }

    fn get_user(self, id: models::Id) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(self.cpupool.clone()
            .spawn_fn(move || Ok(self.store.get_user(id)?))
            .then(Self::format_response)
        )
    }

    fn get_visit(self, id: models::Id) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(self.cpupool.clone()
            .spawn_fn(move || Ok(self.store.get_visit(id)?))
            .then(Self::format_response)
        )
    }

    fn get_location_rating(self, id: models::Id, query: Option<&str>) ->
        Box<Future<Item = server::Response, Error = hyper::Error>>
    {
        Box::new(Self::parse_params(query)
            .and_then(move |options|
                self.cpupool.clone()
                    .spawn_fn(move ||
                        Ok(self.store.get_location_avg(id, options)?)
                    )
            )
            .then(Self::format_response)
        )
    }

    fn get_user_visits(self, id: models::Id, query: Option<&str>) ->
        Box<Future<Item = server::Response, Error = hyper::Error>>
    {
        Box::new(Self::parse_params(query)
            .and_then(move |options|
                self.cpupool.clone()
                    .spawn_fn(move ||
                        Ok(self.store.get_user_visits(id, options)?)
                    )
            )
            .then(Self::format_response)
        )
    }

    fn add_user(self, body: hyper::Body) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(Self::parse_body(body)
            .and_then(|value| Ok(serde_json::from_value(value)?))
            .and_then(move |user|
                self.cpupool.clone().spawn_fn(move ||
                    Ok(self.store.add_user(user)?)
                )
            )
            .then(Self::format_response)
        )
    }

    fn update_user(self, id: u32, body: hyper::Body) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(Self::parse_body(body)
            .and_then(|value| Ok(serde_json::from_value(value)?))
            .and_then(move |user|
                self.cpupool.clone().spawn_fn(move ||
                    Ok(self.store.update_user(id, user)?))
                )
            .then(Self::format_response)
        )
    }

    fn add_location(self, body: hyper::Body) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(Self::parse_body(body)
            .and_then(|value| Ok(serde_json::from_value(value)?))
            .and_then(move |location|
                self.cpupool.clone().spawn_fn(move ||
                    Ok(self.store.add_location(location)?))
                )
            .then(Self::format_response)
        )
    }

    fn update_location(self, id: models::Id, body: hyper::Body) ->
            Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(Self::parse_body(body)
            .and_then(|value| Ok(serde_json::from_value(value)?))
            .and_then(move |location_data|
                self.cpupool.clone().spawn_fn(move ||
                    Ok(self.store.update_location(id, location_data)?)
                )
            )
            .then(Self::format_response)
        )
    }

    fn add_visit(self, body: hyper::Body) -> Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(Self::parse_body(body)
            .and_then(|value| Ok(serde_json::from_value(value)?))
            .and_then(move |visit|
                self.cpupool.clone().spawn_fn(move ||
                    Ok(self.store.add_visit(visit)?))
                )
            .then(Self::format_response)
        )
    }

    fn update_visit(self, id: models::Id, body: hyper::Body) ->
            Box<Future<Item = server::Response, Error = hyper::Error>> {
        Box::new(Self::parse_body(body)
            .and_then(|value| Ok(serde_json::from_value(value)?))
            .and_then(move |visit_data|
                self.cpupool.clone().spawn_fn(move ||
                    Ok(self.store.update_visit(id, visit_data)?))
                )
            .then(Self::format_response)
        )
    }
}

impl server::Service for Router {
    type Request = server::Request;
    type Response = server::Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let (method, uri, _, _, body) = req.deconstruct();
        let mut path_parts = uri.path().split('/').skip(1);

        let result = match (method, path_parts.next(), path_parts.next(), path_parts.next(),
                path_parts.next()) {
            (_, _, _, _, Some(_)) => Self::not_found(),
            (hyper::Method::Get, Some(entity), Some(id_src), action, None) =>
                match (entity, id_src.parse(), action) {
                    ("users", Ok(id), None) =>
                        self.clone().get_user(id),
                    ("users", Ok(id), Some("visits")) =>
                        self.clone().get_user_visits(id, uri.query()),
                    ("locations", Ok(id), None) =>
                        self.clone().get_location(id),
                    ("locations", Ok(id), Some("avg")) =>
                        self.clone().get_location_rating(id, uri.query()),
                    ("visits", Ok(id), None) =>
                        self.clone().get_visit(id),
                    _ => Self::not_found(),
                }
            (hyper::Method::Post, Some(entity), Some("new"), None, None) =>
                match entity {
                    "users" => self.clone().add_user(body),
                    "locations" => self.clone().add_location(body),
                    "visits" => self.clone().add_visit(body),
                    _ => Self::not_found(),
                },
            (hyper::Method::Post, Some(entity), Some(id_src), None, None) =>
                match (entity, id_src.parse()) {
                    ("users", Ok(id)) => self.clone().update_user(id, body),
                    ("locations", Ok(id)) => self.clone().update_location(id, body),
                    ("visits", Ok(id)) => self.clone().update_visit(id, body),
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
const DEFAULT_SERVER_THREADS: &'static str = "1";
const DEFAULT_BACKLOG: &'static str = "1024";
const DEFAULT_DATA_PATH: &'static str = "data/data.zip";
const DEFAULT_CPUPOOL_SIZE: &'static str = "1";

fn main() {
    env_logger::init().unwrap();

    let address = env::var("LISTEN").unwrap_or(DEFAULT_LISTEN.to_string())
        .parse().unwrap();
    let server_thread_count = env::var("SERVER_THREADS").unwrap_or(DEFAULT_SERVER_THREADS.to_string())
        .parse::<usize>().unwrap();
    let backlog = env::var("BACKLOG").unwrap_or(DEFAULT_BACKLOG.to_string())
        .parse::<i32>().unwrap();
    let data_path = env::var("DATA_PATH").unwrap_or(DEFAULT_DATA_PATH.to_string());
    let cpupool_size = env::var("CPUPOOL_SIZE").unwrap_or(DEFAULT_CPUPOOL_SIZE.to_string())
        .parse::<usize>().unwrap();

    info!("Start listen {} on {} threads with backlog", address, server_thread_count);

    let store = Arc::new(store::Store::new());
    let cpupool = Arc::new(futures_cpupool::CpuPool::new(cpupool_size));

    loader::load_data(store.clone(), &data_path).unwrap();

    let keepalive = STREAM_KEEPALIVE_SECS.map(|secs| time::Duration::new(secs, 0));

    let threads = (0..server_thread_count).map(move |thread_index|{
        let store = store.clone();
        let cpupool = cpupool.clone();
        thread::Builder::new()
            .name(format!("Server {}", thread_index))
            .spawn(move || {
                info!("Start thread {}", thread_index);
                let net_listener = net2::TcpBuilder::new_v4().unwrap()
                    .reuse_port(true).unwrap()
                    .bind(address).unwrap()
                    .listen(backlog).unwrap();

                net_listener.set_nonblocking(true).unwrap();

                let mut core = tokio_core::reactor::Core::new().unwrap();
                let handle = core.handle();

                let core_listener = tokio_core::net::TcpListener::from_listener(net_listener, &address, &handle).unwrap();

                core.run(
                    core_listener.incoming().for_each(move |(stream, socket_addr)| {
                        stream.set_keepalive(keepalive).unwrap();
                        stream.set_nodelay(true).unwrap();
                        info!("Connection on thread #{} from {}", thread_index, socket_addr);
                        let router = Router::new(store.clone(), cpupool.clone());
                        hyper::server::Http::new()
                            .keep_alive(true)
                            .bind_connection(&handle, stream, socket_addr, router);
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
