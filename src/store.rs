use std::collections::HashMap;
use std::sync::{
    RwLock,
    PoisonError,
};

use super::models::*;

#[derive(Debug)]
pub enum StoreError {
    EntryExists,
    EntityNotExists,
    MutexPoisoned,
}

impl<A> From<PoisonError<A>> for StoreError {
    fn from(_err: PoisonError<A>) -> Self {
        StoreError::MutexPoisoned
    }
}

pub struct Store {
    users: RwLock<HashMap<Id, User>>,
    locations: RwLock<HashMap<Id, Location>>,
    // visits: RwLock<HashMap<Id, Visit>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            locations: RwLock::new(HashMap::new()),
            // visits: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn get_user(&self, id: Id) -> Result<User, StoreError> {
        let users = self.users.read()?;
        users.get(&id).map(move |u| u.clone()).ok_or(StoreError::EntityNotExists)
    }

    pub fn add_user(&self, user: User) -> Result<(), StoreError> {
        let mut users = self.users.write()?;
        if let Some(_) = users.get(&user.id) {
            return Err(StoreError::EntryExists)
        }

        users.insert(user.id, user);
        Ok(())
    }

    pub fn update_user(&self, id: Id, user_data: UserData) -> Result<(), StoreError> {
        let mut users = self.users.write()?;
        if let Some(user) = users.get_mut(&id) {
            if let Some(email) = user_data.email {
                user.email = email;
            }
            if let Some(first_name) = user_data.first_name {
                user.first_name = first_name;
            }
            if let Some(last_name) = user_data.last_name {
                user.last_name = last_name;
            }
            if let Some(gender) = user_data.gender {
                user.gender = gender;
            }
            if let Some(birth_date) = user_data.birth_date {
                user.birth_date = birth_date;
            }
            Ok(())
        } else {
            Err(StoreError::EntityNotExists)
        }
    }

    pub fn get_location(&self, id: Id) -> Result<Location, StoreError> {
        let locations = self.locations.read()?;
        locations.get(&id)
            .map(|l| l.clone())
            .ok_or(StoreError::EntityNotExists)
    }

    pub fn add_location(&self, location: Location) -> Result<(), StoreError> {
        let mut locations = self.locations.write()?;
        if let Some(_) = locations.get(&location.id) {
            return Err(StoreError::EntryExists)
        }
        locations.insert(location.id, location);
        Ok(())
    }

    pub fn update_location(&self, id: Id, location_data: LocationData) -> Result<(), StoreError> {
        let mut locations = self.locations.write()?;
        if let Some(location) = locations.get_mut(&id) {
            if let Some(distance) = location_data.distance {
                location.distance = distance;
            }
            if let Some(place) = location_data.place {
                location.place = place;
            }
            if let Some(country) = location_data.country {
                location.country = country;
            }
            if let Some(city) = location_data.city {
                location.city = city;
            }
            Ok(())
        } else {
            Err(StoreError::EntityNotExists)
        }
    }
}
