use std::collections::btree_map::BTreeMap;
use std::sync::{
    Mutex,
    PoisonError,
};

use super::models::*;

pub enum StoreError {
    EntryExists,
    MutexPoisoned,
}

impl<A> From<PoisonError<A>> for StoreError {
    fn from(_err: PoisonError<A>) -> Self {
        StoreError::MutexPoisoned
    }
}

pub struct Store {
    users: Mutex<BTreeMap<Id, User>>,
    // locations: Mutex<BTreeMap<Id, Location>>,
    // visits: Mutex<BTreeMap<Id, Visit>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            users: Mutex::new(BTreeMap::new()),
            // locations: Mutex::new(BTreeMap::new()),
            // visits: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn get_user(&self, id: Id) -> Result<Option<User>, StoreError> {
        let users = self.users.lock()?;
        Ok(users.get(&id).map(move |u| u.clone()))
    }

    pub fn add_user(&mut self, user: User) -> Result<(), StoreError> {
        let mut users = self.users.get_mut()?;
        if let Some(_) = users.get(&user.id) {
            return Err(StoreError::EntryExists)
        }

        users.insert(user.id, user);
        Ok(())
    }
}
