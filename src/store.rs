use std::collections::btree_map::BTreeMap;
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
    users: RwLock<BTreeMap<Id, User>>,
    // locations: Mutex<BTreeMap<Id, Location>>,
    // visits: Mutex<BTreeMap<Id, Visit>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            users: RwLock::new(BTreeMap::new()),
            // locations: Mutex::new(BTreeMap::new()),
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
}
