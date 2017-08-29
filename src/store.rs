use std::collections::{
    HashMap,
};
use std::sync::{
    RwLock,
    PoisonError,
};
use std::time;

use super::models::*;

const YAER_DURATION: f64 = 365.25 * 24.0 * 60.0 * 60.0;
const AVG_ACCURACY: f64 = 5.0_f64;

#[derive(Debug)]
pub enum StoreError {
    EntryExists,
    EntityNotExists,
    MutexPoisoned,
    BrokenData,
    InvalidEntity,
}

impl<A> From<PoisonError<A>> for StoreError {
    fn from(_err: PoisonError<A>) -> Self {
        StoreError::MutexPoisoned
    }
}

pub struct Store {
    users: RwLock<HashMap<Id, User>>,
    locations: RwLock<HashMap<Id, Location>>,
    visits: RwLock<HashMap<Id, Visit>>,
    user_visits: RwLock<HashMap<Id, Vec<Id>>>,
    location_visits: RwLock<HashMap<Id, Vec<Id>>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            locations: RwLock::new(HashMap::new()),
            visits: RwLock::new(HashMap::new()),
            user_visits: RwLock::new(HashMap::new()),
            location_visits: RwLock::new(HashMap::new()),
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

        if !user.valid() {
            return Err(StoreError::InvalidEntity)
        }

        users.insert(user.id, user);
        Ok(())
    }

    pub fn update_user(&self, id: Id, user_data: UserData) -> Result<(), StoreError> {
        let mut users = self.users.write()?;
        if let Some(user) = users.get_mut(&id) {
            let mut updated_user = user.clone();
            if let Some(email) = user_data.email {
                updated_user.email = email;
            }
            if let Some(first_name) = user_data.first_name {
                updated_user.first_name = first_name;
            }
            if let Some(last_name) = user_data.last_name {
                updated_user.last_name = last_name;
            }
            if let Some(gender) = user_data.gender {
                updated_user.gender = gender;
            }
            if let Some(birth_date) = user_data.birth_date {
                updated_user.birth_date = birth_date;
            }
            if updated_user.valid() {
                *user = updated_user
            } else {
                return Err(StoreError::InvalidEntity)
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
        if !location.valid() {
            return Err(StoreError::InvalidEntity)
        }
        locations.insert(location.id, location);
        Ok(())
    }

    pub fn update_location(&self, id: Id, location_data: LocationData) -> Result<(), StoreError> {
        let mut locations = self.locations.write()?;
        if let Some(location) = locations.get_mut(&id) {
            let mut updated_location = location.clone();
            if let Some(distance) = location_data.distance {
                updated_location.distance = distance;
            }
            if let Some(place) = location_data.place {
                updated_location.place = place;
            }
            if let Some(country) = location_data.country {
                updated_location.country = country;
            }
            if let Some(city) = location_data.city {
                updated_location.city = city;
            }
            if updated_location.valid() {
                *location = updated_location;
            } else {
                return Err(StoreError::InvalidEntity)
            }
            Ok(())
        } else {
            Err(StoreError::EntityNotExists)
        }
    }

    pub fn get_visit(&self, id: Id) -> Result<Visit, StoreError> {
        self.visits.read()?
            .get(&id)
            .map(move |u| u.clone())
            .ok_or(StoreError::EntityNotExists)
    }

    fn add_visit_to_user(user_visits: &mut HashMap<Id, Vec<Id>>, visit: &Visit) {
        let user_visit_ids = user_visits.entry(visit.user).or_insert(Vec::new());
        user_visit_ids.push(visit.id);
    }

    fn remove_visit_from_user(user_visits: &mut HashMap<Id, Vec<Id>>, visit: &Visit) {
        let user_visit_ids = user_visits.entry(visit.user).or_insert(Vec::new());
        if let Some(position) = user_visit_ids.iter().position(|id| *id == visit.id) {
            user_visit_ids.remove(position);
        }
    }

    fn add_visit_to_location(location_visits: &mut HashMap<Id, Vec<Id>>, visit: &Visit) {
        let location_visit_ids = location_visits.entry(visit.location).or_insert(Vec::new());
        location_visit_ids.push(visit.id);
    }

    fn remove_visit_from_location(location_visits: &mut HashMap<Id, Vec<Id>>, visit: &Visit) {
        let location_visit_ids = location_visits.entry(visit.location).or_insert(Vec::new());
        if let Some(position) = location_visit_ids.iter().position(|id| *id == visit.id) {
            location_visit_ids.remove(position);
        }
    }

    pub fn add_visit(&self, visit: Visit) -> Result<(), StoreError> {
        let mut visits = self.visits.write()?;

        if let Some(_) = visits.get(&visit.id) {
            return Err(StoreError::EntryExists)
        }

        if !visit.valid() {
            return Err(StoreError::InvalidEntity)
        }

        let mut user_visits = self.user_visits.write()?;
        Self::add_visit_to_user(&mut user_visits, &visit);

        let mut location_visits = self.location_visits.write()?;
        Self::add_visit_to_location(&mut location_visits, &visit);

        visits.insert(visit.id, visit);
        Ok(())
    }

    pub fn update_visit(&self, id: Id, visit_data: VisitData) -> Result<(), StoreError> {
        let mut visits = self.visits.write()?;

        if let Some(visit) = visits.get_mut(&id) {
            let mut updated_visit = visit.clone();
            if let Some(location) = visit_data.location {
                updated_visit.location = location;
            }
            if let Some(user) = visit_data.user {
                updated_visit.user = user;
            }
            if let Some(visited_at) = visit_data.visited_at {
                updated_visit.visited_at = visited_at;
            }
            if let Some(mark) = visit_data.mark {
                updated_visit.mark = mark;
            }
            if !updated_visit.valid() {
                return Err(StoreError::InvalidEntity)
            }
            if visit.user != updated_visit.user {
                let mut user_visits = self.user_visits.write()?;
                Self::remove_visit_from_user(&mut user_visits, &visit);
                Self::add_visit_to_user(&mut user_visits, &updated_visit);
            }
            if visit.location != updated_visit.location {
                let mut location_visits = self.location_visits.write()?;
                Self::remove_visit_from_location(&mut location_visits, &visit);
                Self::add_visit_to_location(&mut location_visits, &updated_visit);
            }
            *visit = updated_visit;
            Ok(())
        } else {
            Err(StoreError::EntityNotExists)
        }
    }

    pub fn find_user_visits(&self, user_id: Id, options: FindVisitOptions) ->
            Result<UserVisits, StoreError> {
        debug!("Find user {} visits by {:?}", user_id, options);
        if self.users.read()?.get(&user_id).is_none() {
            return Err(StoreError::EntityNotExists)
        }

        let locations = self.locations.read()?;
        let visits = self.visits.read()?;

        let user_visit_ids = match self.user_visits.read()?.get(&user_id) {
            Some(user_visit_ids) => user_visit_ids.clone(),
            None => return Ok(UserVisits::default()),
        };

        let visit_location_pairs = user_visit_ids
            .iter()
            .map(|vid| {
                let v = visits.get(vid);
                let l = v.and_then(|v| locations.get(&v.location));
                match (v, l) {
                    (Some(v), Some(l)) => Ok((v.clone(), l.clone())),
                    _ => Err(StoreError::BrokenData),
                }
            })
            .collect::<Result<Vec<(Visit, Location)>, StoreError>>()?
            .into_iter()
            .filter(|&(ref v, ref l)|
                if let Some(from_date) = options.from_date { from_date < v.visited_at  } else { true }
                && if let Some(to_date) = options.to_date { v.visited_at < to_date } else { true }
                && if let Some(ref country) = options.country { &l.country == country } else { true }
                && if let Some(to_distance) = options.to_distance { l.distance < to_distance  } else { true }
            );

        let mut user_visits = visit_location_pairs
            .map(|(v, l)| UserVisit {
                mark: v.mark,
                place: l.place,
                visited_at: v.visited_at,
            })
            .collect::<Vec<UserVisit>>();

        user_visits.sort_by(|l, r| l.visited_at.cmp(&r.visited_at));

        Ok(UserVisits {
            visits: user_visits,
        })
    }

    pub fn get_location_rating(&self, user_id: Id, options: LocationRateOptions) ->
            Result<LocationRate, StoreError> {
        debug!("Find user {} visits by {:?}", user_id, options);
        let users = self.users.read()?;
        if users.get(&user_id).is_none() {
            return Err(StoreError::EntityNotExists)
        }

        let location_visit_ids = match self.location_visits.read()?.get(&user_id) {
            Some(ids) => ids.clone(),
            None => return Ok(LocationRate::default()),
        };

        let visits = self.visits.read()?;

        let now = time::SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap().as_secs() as Timestamp;
        debug!("Now {}", now);

        let from_age = options.from_age.map(|from_age| now - ((YAER_DURATION * from_age as f64) as Timestamp));
        debug!("Age from {:?}", from_age);
        let to_age = options.to_age.map(|to_age| now - ((YAER_DURATION * to_age as f64) as Timestamp));
        debug!("Age to {:?}", to_age);

        let (sum_mark, count_mark) = location_visit_ids
            .iter()
            .map(|vid| {
                let visit = visits.get(vid);
                let user = visit.and_then(|v| users.get(&v.user));
                match (visit, user) {
                    (Some(visit), Some(user)) => Ok((visit.clone(), user.clone())),
                    _ => Err(StoreError::BrokenData)
                }
            })
            .collect::<Result<Vec<(Visit, User)>, StoreError>>()?
            .into_iter()
            .filter(|&(ref v, ref u)| {
                (if let Some(from_date) = options.from_date { v.visited_at > from_date } else { true })
                && if let Some(to_date) = options.to_date { v.visited_at < to_date } else { true }
                && if let Some(gender) = options.gender { u.gender == gender } else { true }
                && if let Some(from_age) = from_age { u.birth_date > from_age } else { true }
                && if let Some(to_age) = to_age { u.birth_date < to_age } else { true }
            })
            .fold((0, 0), |(sum, count), (ref v, ref _v)| (sum + v.mark, count + 1));

        if 0 == count_mark {
            return Ok(LocationRate::default());
        }

        let delimiter = 10_f64.powf(AVG_ACCURACY);
        let avg_mark = ((sum_mark as f64 / count_mark as f64) * delimiter).round() / delimiter;

        Ok(LocationRate {
            avg: avg_mark,
        })
    }
}
