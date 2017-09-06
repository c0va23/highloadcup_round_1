use std::sync::{
    RwLock,
    PoisonError,
    Arc,
};
use chrono::prelude::*;
use fnv::FnvHashMap;

use super::models::*;

const AVG_ACCURACY: f64 = 5.0_f64;

type Map<K, V> = FnvHashMap<K, V>;

#[derive(Debug, PartialEq)]
pub enum StoreError {
    EntryExists,
    EntityNotExists,
    MutexPoisoned,
    InvalidEntity(ValidationError),
}

impl<A> From<PoisonError<A>> for StoreError {
    fn from(_err: PoisonError<A>) -> Self {
        StoreError::MutexPoisoned
    }
}

struct StoreInner {
    users: Map<Id, Arc<User>>,
    locations: Map<Id, Arc<Location>>,
    visits: Map<Id, Arc<Visit>>,
    user_visits: Map<Id, Vec<(Arc<Visit>, Arc<Location>)>>,
    location_visits: Map<Id, Vec<(Arc<Visit>, Arc<User>)>>,
}

pub struct Store {
    store_inner: RwLock<StoreInner>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            store_inner: RwLock::new(StoreInner {
                users: Map::default(),
                locations: Map::default(),
                visits: Map::default(),
                user_visits: Map::default(),
                location_visits: Map::default(),

            }),
        }
    }

    pub fn get_user(&self, id: Id) -> Result<User, StoreError> {
        self.store_inner.read()?
            .users.get(&id)
            .map(move |u| u.as_ref().clone())
            .ok_or(StoreError::EntityNotExists)
    }

    pub fn add_user(&self, user: User) -> Result<Empty, StoreError> {
        debug!("Add user {:?}", user);
        let mut store_inner = self.store_inner.write()?;
        if let Some(_) = store_inner.users.get(&user.id) {
            return Err(StoreError::EntryExists)
        }

        if let Err(error) = user.valid() {
            return Err(StoreError::InvalidEntity(error))
        }

        store_inner.users.insert(user.id, Arc::new(user));
        Ok(Empty{})
    }

    pub fn update_user(&self, id: Id, user_data: UserData) -> Result<Empty, StoreError> {
        debug!("Update user {} {:?}", id, user_data);
        let mut store_inner = self.store_inner.write()?;
        if let Some(user) = store_inner.users.get_mut(&id) {
            let mut updated_user = user.as_ref().clone();
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
            if let Err(error) = updated_user.valid() {
                return Err(StoreError::InvalidEntity(error))
            }
            *Arc::make_mut(user) = updated_user;
            Ok(Empty{})
        } else {
            Err(StoreError::EntityNotExists)
        }
    }

    pub fn get_location(&self, id: Id) -> Result<Location, StoreError> {
        self.store_inner.read()?
            .locations.get(&id)
            .map(|l| l.as_ref().clone())
            .ok_or(StoreError::EntityNotExists)
    }

    pub fn add_location(&self, location: Location) -> Result<Empty, StoreError> {
        debug!("Add location {:?}", location);
        let mut store_inner = self.store_inner.write()?;
        if let Some(_) = store_inner.locations.get(&location.id) {
            return Err(StoreError::EntryExists)
        }
        if let Err(error) = location.valid() {
            return Err(StoreError::InvalidEntity(error))
        }
        store_inner.locations.insert(location.id, Arc::new(location));
        Ok(Empty{})
    }

    pub fn update_location(&self, id: Id, location_data: LocationData) -> Result<Empty, StoreError> {
        debug!("Update location {} {:?}", id, location_data);
        let mut store_inner = self.store_inner.write()?;
        if let Some(location) = store_inner.locations.get_mut(&id) {
            let mut updated_location = location.as_ref().clone();
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
            if let Err(error) = updated_location.valid() {
                return Err(StoreError::InvalidEntity(error))
            }
            *Arc::make_mut(location) = updated_location;
            Ok(Empty{})
        } else {
            Err(StoreError::EntityNotExists)
        }
    }

    pub fn get_visit(&self, id: Id) -> Result<Visit, StoreError> {
        self.store_inner.read()?
            .visits
            .get(&id)
            .map(|u| u.as_ref().clone())
            .ok_or(StoreError::EntityNotExists)
    }

    fn add_visit_to_user(
        user_visits: &mut Map<Id, Vec<(Arc<Visit>, Arc<Location>)>>,
        visit: Arc<Visit>,
        location: Arc<Location>,
    ) {
        let user_visits = user_visits.entry(visit.user).or_insert(Vec::new());
        if let Some(position) = user_visits.iter().position(|&(ref v, ref _l)| visit.visited_at <  v.visited_at) {
            user_visits.insert(position, (visit.clone(), location.clone()));
        } else {
            user_visits.push((visit.clone(), location.clone()));
        }
    }

    fn remove_visit_from_user(
        user_visits: &mut Map<Id, Vec<(Arc<Visit>, Arc<Location>)>>,
        visit: &Visit,
    ) {
        let user_visits = user_visits.entry(visit.user).or_insert(Vec::new());
        if let Some(position) = user_visits.iter().position(|&(ref v, ref _l)| v.id == visit.id) {
            user_visits.remove(position);
        }
    }

    fn add_visit_to_location(
        location_visits: &mut Map<Id, Vec<(Arc<Visit>, Arc<User>)>>,
        visit: Arc<Visit>,
        user: Arc<User>,
    ) {
        let location_visit_ids = location_visits.entry(visit.location).or_insert(Vec::new());
        location_visit_ids.push((visit.clone(), user.clone()));
    }

    fn remove_visit_from_location(
        location_visits: &mut Map<Id, Vec<(Arc<Visit>, Arc<User>)>>,
        visit: &Visit,
    ) {
        let location_visits = location_visits.entry(visit.location).or_insert(Vec::new());
        if let Some(position) = location_visits.iter().position(|&(ref v, ref _u)| v.id == visit.id) {
            location_visits.remove(position);
        }
    }

    fn get_visit_user(store_inner: &StoreInner, visit: &Visit) -> Result<Arc<User>, StoreError> {
        store_inner.users.get(&visit.user).ok_or_else(||
            StoreError::InvalidEntity(ValidationError{
                field: "user".to_string(),
                message: format!("User with ID {} not exists", visit.user),
            })
        ).map(|u| u.clone())
    }

    fn get_visit_location(store_inner: &StoreInner, visit: &Visit) -> Result<Arc<Location>, StoreError> {
        store_inner.locations.get(&visit.location).ok_or_else(||
            StoreError::InvalidEntity(ValidationError{
                field: "location".to_string(),
                message: format!("Location with ID {} not exists", visit.location),
            })
        ).map(|l| l.clone())
    }

    pub fn add_visit(&self, visit: Visit) -> Result<Empty, StoreError> {
        debug!("Add visit {:?}", visit);
        let mut store_inner = self.store_inner.write()?;

        if store_inner.visits.get(&visit.id).is_some() {
            return Err(StoreError::EntryExists)
        }

        if let Err(error) = visit.valid() {
            return Err(StoreError::InvalidEntity(error))
        }

        let user = Self::get_visit_user(&store_inner, &visit)?.clone();
        let location = Self::get_visit_location(&store_inner, &visit)?.clone();

        let visit = Arc::new(visit);

        Self::add_visit_to_user(&mut store_inner.user_visits, visit.clone(), location);
        Self::add_visit_to_location(&mut store_inner.location_visits, visit.clone(), user);

        store_inner.visits.insert(visit.id, visit.clone());
        Ok(Empty{})
    }

    pub fn update_visit(&self, id: Id, visit_data: VisitData) -> Result<Empty, StoreError> {
        debug!("Update visit {} {:?}", id, visit_data);
        let mut store_inner = self.store_inner.write()?;

        let original_visit = store_inner.visits.get(&id).ok_or(StoreError::EntityNotExists)?.clone().as_ref().clone();
        debug!("Original visit {:?}", original_visit);
        let visit = {
            let mut updated_visit = original_visit.clone();
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
            if let Err(error) = updated_visit.valid() {
                return Err(StoreError::InvalidEntity(error))
            }
            let visit = store_inner.visits.get_mut(&id).ok_or(StoreError::EntityNotExists)?;
            debug!("Replace visit {:?} wiht {:?}", visit, updated_visit);
            *Arc::make_mut(visit) = updated_visit;
            visit.clone()
        };
        debug!("Updated visit {:?}", visit);
        if original_visit.user != visit.user {
            debug!("Update visit user from {} to {}", original_visit.user, visit.user);
            let location = Self::get_visit_location(&store_inner, &visit)?.clone();
            Self::remove_visit_from_user(&mut store_inner.user_visits, &original_visit);
            Self::add_visit_to_user(&mut store_inner.user_visits, visit.clone(), location);
        }
        if original_visit.location != visit.location {
            debug!("Update visit locatoin from {} to {}", original_visit.location, visit.location);
            let user = Self::get_visit_user(&store_inner, &visit)?.clone();
            Self::remove_visit_from_location(&mut store_inner.location_visits, &original_visit);
            Self::add_visit_to_location(&mut store_inner.location_visits, visit.clone(), user);
        }
        Ok(Empty{})
    }

    pub fn get_user_visits(&self, user_id: Id, options: GetUserVisitsOptions) ->
            Result<UserVisits, StoreError> {
        debug!("Get user {} visits by {:?}", user_id, options);
        let store_inner = self.store_inner.read()?;
        if store_inner.users.get(&user_id).is_none() {
            return Err(StoreError::EntityNotExists)
        }

        let user_visit_ids = match store_inner.user_visits.get(&user_id) {
            Some(user_visit_ids) => user_visit_ids.clone(),
            None => return Ok(UserVisits::default()),
        };

        let user_visits = user_visit_ids
            .into_iter()
            .filter(|&(ref v, ref l)|
                if let Some(from_date) = options.from_date { from_date < v.visited_at  } else { true }
                && if let Some(to_date) = options.to_date { v.visited_at < to_date } else { true }
                && if let Some(ref country) = options.country { &l.country == country } else { true }
                && if let Some(to_distance) = options.to_distance { l.distance < to_distance  } else { true }
            )
            .map(move |(ref v, ref l)| UserVisit {
                mark: v.mark,
                place: l.place.clone(),
                visited_at: v.visited_at,
            })
            .collect::<Vec<UserVisit>>();

        Ok(UserVisits {
            visits: user_visits,
        })
    }

    pub fn get_location_avg(&self, location_id: Id, options: GetLocationAvgOptions) ->
            Result<LocationRate, StoreError> {
        debug!("Find location {} avg by {:?}", location_id, options);
        let store_inner = self.store_inner.read()?;

        if store_inner.locations.get(&location_id).is_none() {
            return Err(StoreError::EntityNotExists)
        }

        let location_visit_ids = match store_inner.location_visits.get(&location_id) {
            Some(ids) => ids.clone(),
            None => return Ok(LocationRate::default()),
        };

        let now = Utc::now();
        debug!("Now {}", now);

        let from_age = options.from_age.and_then(|from_age| now.with_year(now.year() - from_age))
            .map(|t| t.timestamp());
        debug!("Age from {:?}", from_age);

        let to_age = options.to_age.and_then(|to_age| now.with_year(now.year() - to_age))
            .map(|t| t.timestamp());
        debug!("Age to {:?}", to_age);

        let (sum_mark, count_mark) = location_visit_ids
            .into_iter()
            .filter(|&(ref v, ref u)| {
                (if let Some(from_date) = options.from_date { v.visited_at > from_date } else { true })
                && if let Some(to_date) = options.to_date { v.visited_at < to_date } else { true }
                && if let Some(gender) = options.gender { u.gender == gender } else { true }
                && if let Some(from_age) = from_age { u.birth_date < from_age } else { true }
                && if let Some(to_age) = to_age { u.birth_date > to_age } else { true }
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

#[cfg(test)]
mod tests {
    use super::*;
    use env_logger;

    fn setup() {
        env_logger::init().unwrap();
    }

    fn old_user() -> User {
       User {
            id: 1,
            email: "vasia.pupkin@mail.com".into(),
            first_name: "Vasia".into(),
            last_name: "Pupkin".into(),
            gender: 'm',
            birth_date: 315532800, // 1980-01-01T00:00:00
        }
    }

    fn new_user() -> User {
        User {
            id: 2,
            email: "dasha.petrova@mail.com".into(),
            first_name: "Dasha".into(),
            last_name: "Petrova".into(),
            gender: 'f',
            birth_date: 631152000, // 1990-01-01T00:00:00
        }
    }

    fn old_location() -> Location {
        Location {
            id: 1,
            place: "Musei".into(),
            city: "Krasnodar".into(),
            country: "Russia".into(),
            distance: 10,
        }
    }

    fn new_location() -> Location {
        Location {
            id: 2,
            place: "Biblioteka".into(),
            city: "Moscow".into(),
            country: "Russia".into(),
            distance: 0,
        }
    }

    fn visit(user: &User, location: &Location) -> Visit {
        Visit {
            id: 1,
            user: user.id,
            location: location.id,
            mark: 3,
            visited_at: 1262304000, // 2010-01-01T00:00:00
        }
    }

    #[test]
    fn update_visit_with_all_valid_fields() {
        setup();

        let store = Store::new();

        let old_user = old_user();
        store.add_user(old_user.clone()).unwrap();

        let new_user = new_user();
        store.add_user(new_user.clone()).unwrap();

        let old_location = old_location();
        store.add_location(old_location.clone()).unwrap();

        let new_location = new_location();
        store.add_location(new_location.clone()).unwrap();

        let visit = visit(&old_user, &old_location);
        store.add_visit(visit.clone()).unwrap();

        let visit_data = VisitData {
            location: Some(new_location.id),
            user: Some(new_user.id),
            mark: Some(4),
            visited_at: Some(1293840000), // 2011-01-01T00:00:00
        };

        assert!(store.update_visit(visit.id, visit_data.clone()).is_ok());

        assert_eq!(
            store.get_user_visits(old_user.id, GetUserVisitsOptions::default()),
            Ok(UserVisits{
                visits: Vec::new(),
            })
        );

        assert_eq!(
            store.get_visit(visit.id),
            Ok(Visit {
                id: visit.id,
                location: visit_data.location.unwrap(),
                user: visit_data.user.unwrap(),
                mark: visit_data.mark.unwrap(),
                visited_at: visit_data.visited_at.unwrap(),
            })
        );

        assert_eq!(
            store.get_user_visits(new_user.id, GetUserVisitsOptions::default()),
            Ok(UserVisits{
                visits: vec![
                    UserVisit {
                        mark: visit_data.mark.unwrap(),
                        visited_at: visit_data.visited_at.unwrap(),
                        place: new_location.place,
                    },
                ],
            })
        );

        assert_eq!(
            store.get_user_visits(old_user.id, GetUserVisitsOptions::default()),
            Ok(UserVisits{ visits: vec![] })
        );

        assert_eq!(
            store.get_location_avg(new_location.id, GetLocationAvgOptions::default()),
            Ok(LocationRate { avg: visit_data.mark.unwrap() as f64 })
        );

        assert_eq!(
            store.get_location_avg(old_location.id, GetLocationAvgOptions::default()),
            Ok(LocationRate { avg: 0_f64 })
        );
    }
}