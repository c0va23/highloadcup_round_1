use std::sync::Arc;

use zip;
use std::fs;
use std::io;
use serde_json;

use super::store;
use super::models;

#[derive(Debug)]
pub enum Error {
    IoError(io::Error),
    ZipError(zip::result::ZipError),
    JsonError(serde_json::Error),
    StoreError(store::StoreError),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::IoError(err)
    }
}

impl From<zip::result::ZipError> for Error {
    fn from(err: zip::result::ZipError) -> Self {
        Error::ZipError(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::JsonError(err)
    }
}

impl From<store::StoreError> for Error {
    fn from(err: store::StoreError) -> Self {
        Error::StoreError(err)
    }
}

#[derive(Deserialize)]
struct LocationsData {
    locations: Vec<models::Location>,
}

#[derive(Deserialize)]
struct UsersData {
    users: Vec<models::User>,
}

#[derive(Deserialize)]
struct VisitsData {
    visits: Vec<models::Visit>,
}

pub fn load_data(store: Arc<store::Store>, file_path: &str) -> Result<(), Error> {
    let reader = fs::File::open(file_path)?;
    let mut archive = zip::ZipArchive::new(reader)?;
    for i in 0..archive.len() {
        let file = archive.by_index(i)?;
        let file_name = file.name().to_string();
        if file_name.starts_with("locations_") {
            debug!("Load file {}", file_name);
            let locations_data: LocationsData = serde_json::from_reader(file)?;
            for location in locations_data.locations {
                store.add_location(location)?;
            }
        } else if file_name.starts_with("users_") {
            debug!("Load file {}", file_name);
            let users_data: UsersData = serde_json::from_reader(file)?;
            for user in users_data.users {
                store.add_user(user)?;
            }
        }
    }
    for i in 0..archive.len() {
        let file = archive.by_index(i)?;
        let file_name = file.name().to_string();
        if file_name.starts_with("visits_") {
            debug!("Load file {}", file_name);
            let visits_data: VisitsData = serde_json::from_reader(file)?;
            for visit in visits_data.visits {
                store.add_visit(visit)?;
            }
        }
    }
    Ok(())
}