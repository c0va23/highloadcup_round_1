use zip;
use std::fs;
use std::io;
use std::num;
use serde_json;

use super::store;
use super::models;

#[derive(Debug)]
pub enum Error {
    IoError(io::Error),
    ZipError(zip::result::ZipError),
    JsonError(serde_json::Error),
    StoreError(store::StoreError),
    InvalidOptinsLines {
        lines: usize,
    },
    InvalidOptinsTime(num::ParseIntError),
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

pub struct Options {
    pub generated_at: models::Timestamp,
    pub is_full: bool,
}

pub fn load_options(data_dir: &str) -> Result<Options, Error> {
    use std::io::BufRead;

    let file = fs::File::open(data_dir.to_string() + "/options.txt")?;
    let lines = io::BufReader::new(file).lines().collect::<Result<Vec<String>, io::Error>>()?;

    if lines.len() != 2 {
        return Err(Error::InvalidOptinsLines {
            lines: lines.len(),
        })
    }

    Ok(Options {
        generated_at: lines[0].parse().map_err(Error::InvalidOptinsTime)?,
        is_full: lines[1] == "1",
    })
}

pub fn load_data(store: &store::Store, data_dir: &str) -> Result<(), Error> {
    let reader = fs::File::open(data_dir.to_string() + "/data.zip")?;
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