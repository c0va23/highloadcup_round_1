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

const JSON_SUFIX: &'static str = ".json";

fn get_sorted_file_names(archive: &mut zip::ZipArchive<fs::File>, prefix: &str) -> Result<Vec<String>, Error> {
    let mut file_names = Vec::new();
    for i in 0..archive.len() {
        let file = archive.by_index(i)?;
        let file_name = file.name().to_string();
        if file_name.starts_with(prefix) && file_name.ends_with(JSON_SUFIX) {
            file_names.push(file_name)
        }
    }
    file_names.sort_by(|left_filne_name, right_file_name| {
        let left_index: usize = left_filne_name[prefix.len()..(left_filne_name.len()-JSON_SUFIX.len())].parse().unwrap();
        let right_index: usize = right_file_name[prefix.len()..(right_file_name.len()-JSON_SUFIX.len())].parse().unwrap();
        left_index.cmp(&right_index)
    });
    Ok(file_names)
}

pub fn load_data(store: &mut store::Store, data_dir: &str) -> Result<(), Error> {
    let reader = fs::File::open(data_dir.to_string() + "/data.zip")?;
    let mut archive = zip::ZipArchive::new(reader)?;

    for file_name in get_sorted_file_names(&mut archive, "locations_")?.iter() {
        let file = archive.by_name(file_name)?;
        debug!("Load file {}", file_name);
        let locations_data: LocationsData = serde_json::from_reader(file)?;
        for location in locations_data.locations {
            store.add_location(location)?;
        }
    }
    for file_name in get_sorted_file_names(&mut archive, "users_")?.iter() {
        let file = archive.by_name(file_name)?;
        debug!("Load file {}", file_name);
        let users_data: UsersData = serde_json::from_reader(file)?;
        for user in users_data.users {
            store.add_user(user)?;
        }
    }

    for file_name in get_sorted_file_names(&mut archive, "visits_")?.iter() {
        let file = archive.by_name(file_name)?;
        debug!("Load file {}", file_name);
        let visits_data: VisitsData = serde_json::from_reader(file)?;
        for visit in visits_data.visits {
            store.add_visit(visit)?;
        }
    }

    Ok(())
}