use std::sync::{
    RwLock,
    PoisonError,
};

use chrono::prelude::*;
use fnv;

use super::models::*;

const AVG_ACCURACY: f64 = 5.0_f64;

type Hash<Value> = fnv::FnvHashMap<Id, Value>;

#[derive(Debug, PartialEq, Clone)]
pub enum StoreError {
    EntryExists,
    EntityNotExists,
    InvalidEntity(ValidationError),
    LockError,
}

impl<Guard> From<PoisonError<Guard>> for StoreError {
    fn from(_err: PoisonError<Guard>) -> Self {
        StoreError::LockError
    }
}

pub struct Store {
    now: DateTime<Utc>,
    users: Hash<(User, Vec<(Id, Id)>)>, // (Visit.id, Location.id)
    locations: Hash<(Location, Vec<(Id, Id)>)>, // (Visit.id, User.id)
    visits: Hash<Visit>,
}

impl Store {
    pub fn new(now: Timestamp) -> Self {
        let now = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(now, 0),
            Utc,
        );
        Self {
            now: now,
            users: Hash::default(),
            locations: Hash::default(),
            visits: Hash::default(),
        }
    }

    pub fn get_user(&self, id: Id) -> Result<User, StoreError> {
        self.users.get(&id)
            .map(|&(ref u, _)| u.clone())
            .ok_or(StoreError::EntityNotExists)
    }

    pub fn add_user(&mut self, user: User) -> Result<Empty, StoreError> {
        debug!("Add user {:?}", user);

        if self.users.get(&user.id).is_some() {
            return Err(StoreError::EntryExists)
        }

        if let Err(error) = user.valid() {
            return Err(StoreError::InvalidEntity(error))
        }

        self.users.insert(user.id, (user, Vec::new()));
        Ok(Empty{})
    }

    pub fn update_user(&mut self, id: Id, user_data: UserData) -> Result<Empty, StoreError> {
        debug!("Update user {} {:?}", id, user_data);
        let user_record = self.users.get_mut(&id).ok_or(StoreError::EntityNotExists)?;
        let mut updated_user = user_record.0.clone();

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

        user_record.0 = updated_user;

        Ok(Empty{})
    }

    pub fn get_location(&self, id: Id) -> Result<Location, StoreError> {
        self.locations.get(&id)
            .map(|&(ref l, _)| l.clone())
            .ok_or(StoreError::EntityNotExists)
    }

    pub fn add_location(&mut self, location: Location) -> Result<Empty, StoreError> {
        debug!("Add location {:?}", location);

        if self.locations.get(&location.id).is_some() {
            return Err(StoreError::EntryExists)
        }

        if let Err(error) = location.valid() {
            return Err(StoreError::InvalidEntity(error))
        }

        self.locations.insert(location.id, (location, Vec::new()));
        Ok(Empty{})
    }

    pub fn update_location(&mut self, id: Id, location_data: LocationData) -> Result<Empty, StoreError> {
        debug!("Update location {} {:?}", id, location_data);

        let location_record = self.locations.get_mut(&id)
            .ok_or(StoreError::EntityNotExists)?;

        let mut updated_location = location_record.0.clone();

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

        location_record.0 = updated_location;

        Ok(Empty{})
    }

    pub fn get_visit(&self, visit_id: Id) -> Result<Visit, StoreError> {
        self.visits.get(&visit_id)
            .map(|v| v.clone())
            .ok_or(StoreError::EntityNotExists)
    }

    fn add_visit_to_user(
        &mut self,
        visit: &Visit,
        location: &Location,
    ) -> Result<(), StoreError> {
        let position = {
            let user_visits = &self.users.get(&visit.user)
                .ok_or(StoreError::EntityNotExists)?.1;

            user_visits.iter()
                .map(|&(visit_id, _)|
                    self.visits
                        .get(&visit_id)
                        .map(|v| v.visited_at)
                )
                .collect::<Option<Vec<Timestamp>>>()
                .ok_or(StoreError::EntityNotExists)?
                .into_iter()
                .position(|visited_at| visit.visited_at < visited_at)
        };

        let user_visits = &mut self.users.get_mut(&visit.user)
            .ok_or(StoreError::EntityNotExists)?.1;

        let pair = (visit.id, location.id);

        match position {
            Some(position) => user_visits.insert(position, pair),
            None => user_visits.push(pair),
        }

        Ok(())
    }

    fn remove_visit_from_user(
        &mut self,
        visit: &Visit,
    ) -> Result<(), StoreError> {
        let user_visits = &mut self.users
            .get_mut(&visit.user)
            .ok_or(StoreError::EntityNotExists)?
            .1;

        user_visits.retain(|&(visit_id, _)| visit_id != visit.id);

        Ok(())
    }

    fn add_visit_to_location(
        &mut self,
        visit: &Visit,
        user: &User,
    ) -> Result<(), StoreError> {
        let location_visits = &mut self.locations
            .get_mut(&visit.location)
            .ok_or(StoreError::EntityNotExists)?
            .1;

        location_visits.push((visit.id, user.id));

        Ok(())
    }

    fn remove_visit_from_location(
        &mut self,
        visit: &Visit,
    ) -> Result<(), StoreError> {
        let location_visits = &mut self.locations
            .get_mut(&visit.location)
            .ok_or(StoreError::EntityNotExists)?
            .1;

        location_visits.retain(|&(visit_id, _)| visit_id != visit.id);

        Ok(())
    }

    fn get_visit_user(&self, user_id: Id) -> Result<User, StoreError> {
        match self.users.get(&user_id) {
            None =>
                Err(StoreError::InvalidEntity(ValidationError{
                    field: "user".to_string(),
                    message: format!("User with ID {} not exists", user_id),
                })),
            Some(&(ref user, _)) => Ok(user.clone()),
        }
    }

    fn get_visit_location(&self, location_id: Id) -> Result<Location, StoreError> {
        match self.locations.get(&location_id) {
            None =>
                Err(StoreError::InvalidEntity(ValidationError{
                    field: "location".to_string(),
                    message: format!("Location with ID {} not exists", location_id),
                })),
            Some(&(ref location, _)) =>
                Ok(location.clone()),
        }
    }

    pub fn add_visit(&mut self, visit: Visit) -> Result<Empty, StoreError> {
        debug!("Add visit {:?}", visit);

        if self.visits.get(&visit.id).is_some() {
            return Err(StoreError::EntryExists)
        }

        if let Err(error) = visit.valid() {
            return Err(StoreError::InvalidEntity(error))
        }

        let user = self.get_visit_user(visit.user)?;
        let location = self.get_visit_location(visit.location)?;

        self.add_visit_to_user(&visit, &location)?;
        self.add_visit_to_location(&visit, &user)?;

        self.visits.insert(visit.id, visit);

        Ok(Empty{})
    }

    pub fn update_visit(&mut self, id: Id, visit_data: VisitData) -> Result<Empty, StoreError> {
        debug!("Update visit {} {:?}", id, visit_data);

        let original_visit = self.visits
            .get(&id)
            .ok_or(StoreError::EntityNotExists)?
            .clone()
        ;

        debug!("Original visit {:?}", original_visit);

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

        debug!("Updated visit {:?}", updated_visit);

        let location = self.get_visit_location(updated_visit.location)?.clone();
        let user = self.get_visit_user(updated_visit.user)?.clone();

        debug!("Replace visit {:?} wiht {:?}", original_visit, updated_visit);
        *self.visits.get_mut(&id).unwrap() = updated_visit.clone();

        if original_visit.user != updated_visit.user ||
                original_visit.visited_at != updated_visit.visited_at ||
                original_visit.location != updated_visit.location {
            debug!("Update visit user from {} to {}", original_visit.user, updated_visit.user);
            self.remove_visit_from_user(&original_visit)?;
            self.add_visit_to_user(&updated_visit, &location)?;
        }
        if original_visit.location != updated_visit.location || original_visit.user != updated_visit.user {
            debug!("Update visit locatoin from {} to {}", original_visit.location, updated_visit.location);
            self.remove_visit_from_location(&original_visit)?;
            self.add_visit_to_location(&updated_visit, &user)?;
        }

        Ok(Empty{})
    }

    pub fn get_user_visits(&self, user_id: Id, options: GetUserVisitsOptions) ->
            Result<UserVisits, StoreError> {
        debug!("Get user {} visits by {:?}", user_id, options);

        let user_record = self.users.get(&user_id)
            .ok_or(StoreError::EntityNotExists)?;

        let user_visits = user_record.1
            .iter()
            .map(|&(visit_id, location_id)|
                self.visits.get(&visit_id).and_then(|visit|
                    self.locations.get(&location_id).map(|&(ref location, _)|
                        (visit.clone(), location.clone())
                    )
                )
            )
            .collect::<Option<Vec<(Visit, Location)>>>()
            .ok_or(StoreError::EntityNotExists)?
            .into_iter()
            .filter(|&(ref v, ref l)| {
                (if let Some(from_date) = options.from_date { from_date < v.visited_at  } else { true })
                && if let Some(to_date) = options.to_date { v.visited_at < to_date } else { true }
                && if let Some(ref country) = options.country { &l.country == country } else { true }
                && if let Some(to_distance) = options.to_distance { l.distance < to_distance  } else { true }
            })
            .map(|(ref v, ref l)| {
                UserVisit {
                    mark: v.mark,
                    place: l.place.clone(),
                    visited_at: v.visited_at,
                }
            })
            .collect::<Vec<UserVisit>>();

        Ok(UserVisits {
            visits: user_visits,
        })
    }

    pub fn get_location_avg(&self, location_id: Id, options: GetLocationAvgOptions) ->
            Result<LocationRate, StoreError> {
        debug!("Find location {} avg by {:?}", location_id, options);

        let location_visits = &self.locations.get(&location_id)
            .ok_or(StoreError::EntityNotExists)?
            .1;

        debug!("Location visits: {:?}", location_visits);

        debug!("Now {}", self.now);

        let from_age = options.from_age
            .and_then(|from_age| self.now.with_year(self.now.year() - from_age))
            .map(|t| t.timestamp());
        debug!("Age from {:?}", from_age);

        let to_age = options.to_age
            .and_then(|to_age| self.now.with_year(self.now.year() - to_age))
            .map(|t| t.timestamp());
        debug!("Age to {:?}", to_age);

       let filtered_location_visits: Vec<(Visit, User)>  = location_visits
            .iter()
            .map(|&(visit_id, user_id)|
                self.visits.get(&visit_id).and_then(|visit|
                    self.users.get(&user_id).map(|&(ref user, _)|
                        (visit.clone(), user.clone())
                    )
                )
            )
            .collect::<Option<Vec<(Visit, User)>>>()
            .ok_or(StoreError::EntityNotExists)?
            .into_iter()
            .filter(|&(ref v, ref u)| {
                (if let Some(from_date) = options.from_date { v.visited_at > from_date } else { true })
                && if let Some(to_date) = options.to_date { v.visited_at < to_date } else { true }
                && if let Some(gender) = options.gender { u.gender == gender } else { true }
                && if let Some(from_age) = from_age { u.birth_date < from_age } else { true }
                && if let Some(to_age) = to_age { u.birth_date > to_age } else { true }
            })
            .collect::<Vec<(Visit, User)>>();

        debug!("Filtered location vistis: {:?}", filtered_location_visits);

        let (sum_mark, count_mark) = filtered_location_visits.iter()
            .fold((0u64, 0u64), |(sum, count), &(ref v, ref _v)| (sum + v.mark as u64, count + 1));

        debug!("Sum/count: {}/{}", sum_mark, count_mark);

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

pub struct StoreWrapper {
    store: RwLock<Store>,
}

impl StoreWrapper {
    pub fn new(store: Store) -> Self {
        Self {
            store: RwLock::new(store),
        }
    }

    pub fn get_user(&self, user_id: Id) -> Result<User, StoreError> {
        self.store.read()?.get_user(user_id)
    }

    pub fn add_user(&self, user: User) -> Result<Empty, StoreError> {
        self.store.write()?.add_user(user)
    }

    pub fn update_user(&self, user_id: Id, user_data: UserData) -> Result<Empty, StoreError> {
        self.store.write()?.update_user(user_id, user_data)
    }

    pub fn get_location(&self, location_id: Id) -> Result<Location, StoreError> {
        self.store.read()?.get_location(location_id)
    }

    pub fn add_location(&self, location: Location) -> Result<Empty, StoreError> {
        self.store.write()?.add_location(location)
    }

    pub fn update_location(&self, location_id: Id, location_data: LocationData) -> Result<Empty, StoreError> {
        self.store.write()?.update_location(location_id, location_data)
    }

    pub fn get_visit(&self, visit_id: Id) -> Result<Visit, StoreError> {
        self.store.read()?.get_visit(visit_id)
    }

    pub fn add_visit(&self, visit: Visit) -> Result<Empty, StoreError> {
        self.store.write()?.add_visit(visit)
    }

    pub fn update_visit(&self, visit_id: Id, visit_data: VisitData) -> Result<Empty, StoreError> {
        self.store.write()?.update_visit(visit_id, visit_data)
    }

    pub fn get_user_visits(&self, user_id: Id, options: GetUserVisitsOptions) -> Result<UserVisits, StoreError> {
        self.store.read()?.get_user_visits(user_id, options)
    }

    pub fn get_location_avg(&self, location_id: Id, options: GetLocationAvgOptions) -> Result<LocationRate, StoreError> {
        self.store.read()?.get_location_avg(location_id, options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use env_logger;
    use chrono::Utc;

    #[allow(unused_must_use)]
    fn setup() {
        env_logger::init();
    }

    fn year_ago(age: i32) -> Timestamp {
        let now = Utc::now();
        now.with_year(now.year() - age).unwrap().timestamp()
    }

    fn create_store() -> Store {
        Store::new(Utc::now().timestamp())
    }

    fn old_user() -> User {
       User {
            id: 1,
            email: "vasia.pupkin@mail.com".into(),
            first_name: "Vasia".into(),
            last_name: "Pupkin".into(),
            gender: 'm',
            birth_date: year_ago(70),
        }
    }

    fn new_user() -> User {
        User {
            id: 2,
            email: "dasha.petrova@mail.com".into(),
            first_name: "Dasha".into(),
            last_name: "Petrova".into(),
            gender: 'f',
            birth_date: year_ago(25),
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
            visited_at: year_ago(5)
        }
    }

    #[test]
    fn update_visit_with_all_valid_fields() {
        setup();

        let mut store = create_store();

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
            visited_at: Some(year_ago(4)),
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

    #[test]
    fn update_visit_with_valid_mark() {
        setup();

        let mut store = create_store();

        let user = old_user();
        store.add_user(user.clone()).unwrap();

        let location = old_location();
        store.add_location(location.clone()).unwrap();

        let visit = visit(&user, &location);
        store.add_visit(visit.clone()).unwrap();

        let visit_data = VisitData {
            mark: Some(4),
            ..Default::default()
        };

        assert!(store.update_visit(visit.id, visit_data.clone()).is_ok());

        assert_eq!(
            store.get_visit(visit.id),
            Ok(Visit {
                id: visit.id,
                location: visit.location,
                user: visit.user,
                mark: visit_data.mark.unwrap(),
                visited_at: visit.visited_at,
            })
        );

        assert_eq!(
            store.get_user_visits(user.id, GetUserVisitsOptions::default()),
            Ok(UserVisits{
                visits: vec![
                    UserVisit {
                        mark: visit_data.mark.unwrap(),
                        visited_at: visit.visited_at,
                        place: location.place,
                    },
                ],
            })
        );

        assert_eq!(
            store.get_location_avg(location.id, GetLocationAvgOptions::default()),
            Ok(LocationRate { avg: visit_data.mark.unwrap() as f64 })
        );
    }

    #[test]
    fn update_visit_with_valid_user() {
        setup();

        let mut store = create_store();

        let old_user = old_user();
        store.add_user(old_user.clone()).unwrap();

        let new_user = new_user();
        store.add_user(new_user.clone()).unwrap();

        let location = old_location();
        store.add_location(location.clone()).unwrap();

        let visit = visit(&old_user, &location);
        store.add_visit(visit.clone()).unwrap();

        let visit_data = VisitData {
            user: Some(new_user.id),
            ..Default::default()
        };

        assert!(store.update_visit(visit.id, visit_data.clone()).is_ok());

        assert_eq!(
            store.get_visit(visit.id),
            Ok(Visit {
                id: visit.id,
                location: visit.location,
                user: new_user.id,
                mark: visit.mark,
                visited_at: visit.visited_at,
            })
        );

        assert_eq!(
            store.get_user_visits(old_user.id, GetUserVisitsOptions::default()),
            Ok(UserVisits{
                visits: vec![]
            })
        );

        assert_eq!(
            store.get_user_visits(new_user.id, GetUserVisitsOptions::default()),
            Ok(UserVisits{
                visits: vec![
                    UserVisit {
                        mark: visit.mark,
                        visited_at: visit.visited_at,
                        place: location.place,
                    },
                ],
            })
        );

        assert_eq!(
            store.get_location_avg(location.id, GetLocationAvgOptions::default()),
            Ok(LocationRate { avg: visit.mark as f64 })
        );

        assert_eq!(
            store.get_location_avg(
                location.id,
                GetLocationAvgOptions {
                    from_age: Some(40),
                    ..GetLocationAvgOptions::default()
                },
            ),
            Ok(LocationRate { avg: 0.0 })
        );
    }

    #[test]
    fn update_visit_with_valid_visited_at() {
        setup();

        let mut store = create_store();

        let user = old_user();
        store.add_user(user.clone()).unwrap();

        let old_location = old_location();
        store.add_location(old_location.clone()).unwrap();

        let new_location = new_location();
        store.add_location(new_location.clone()).unwrap();

        let old_visit = Visit { id: 1, location: old_location.id, user: user.id, visited_at: 1, mark: 3 };
        assert_eq!(store.add_visit(old_visit.clone()), Ok(Empty{}));

        let new_visit = Visit { id: 2, location: new_location.id, user: user.id, visited_at: 2, mark: 4 };
        assert_eq!(store.add_visit(new_visit.clone()), Ok(Empty{}));

        let visit_data = VisitData {
            visited_at: Some(3),
            ..Default::default()
        };

        assert!(store.update_visit(old_visit.id, visit_data.clone()).is_ok());

        assert_eq!(
            store.get_visit(old_visit.id),
            Ok(Visit {
                id: old_visit.id,
                location: old_visit.location,
                user: old_visit.user,
                mark: old_visit.mark,
                visited_at: visit_data.visited_at.unwrap(),
            })
        );

        assert_eq!(
            store.get_user_visits(user.id, GetUserVisitsOptions::default()),
            Ok(UserVisits{
                visits: vec![
                    UserVisit {
                        mark: new_visit.mark,
                        visited_at: new_visit.visited_at,
                        place: new_location.place,
                    },
                    UserVisit {
                        mark: old_visit.mark,
                        visited_at: visit_data.visited_at.unwrap(),
                        place: old_location.place,
                    },
                ],
            })
        );
    }

    #[test]
    fn update_visit_with_invalid_location() {
        setup();

        let mut store = create_store();

        let user = old_user();
        store.add_user(user.clone()).unwrap();

        let location = old_location();
        store.add_location(location.clone()).unwrap();

        let visit = visit(&user, &location);
        store.add_visit(visit.clone()).unwrap();

        let visit_data = VisitData {
            location: Some(100),
            ..Default::default()
        };

        assert_matches!(
            store.update_visit(visit.id, visit_data),
            Err(StoreError::InvalidEntity(ValidationError{ .. }))
        );

        assert_eq!(store.get_visit(visit.id), Ok(visit.clone()));
    }

    #[test]
    fn update_visit_with_invalid_user() {
        setup();

        let mut store = create_store();

        let user = old_user();
        store.add_user(user.clone()).unwrap();

        let location = old_location();
        store.add_location(location.clone()).unwrap();

        let visit = visit(&user, &location);
        store.add_visit(visit.clone()).unwrap();

        let visit_data = VisitData {
            user: Some(100),
            ..Default::default()
        };

        assert_matches!(
            store.update_visit(visit.id, visit_data),
            Err(StoreError::InvalidEntity(ValidationError{ .. }))
        );

        assert_eq!(store.get_visit(visit.id), Ok(visit.clone()));
    }

    #[test]
    fn update_location_with_valid_fields() {
        setup();

        let mut store = create_store();

        let user = old_user();
        store.add_user(user.clone()).unwrap();

        let location = old_location();
        store.add_location(location.clone()).unwrap();

        let visit = visit(&user, &location);
        store.add_visit(visit.clone()).unwrap();

        let location_data = LocationData {
            place: Some("Biblioteka".into()),
            city: Some("Moscow".into()),
            country: Some("Russia".into()),
            distance: Some(100),
        };

        assert_eq!(
            store.update_location(location.id, location_data.clone()),
            Ok(Empty{})
        );

        assert_eq!(
            store.get_user_visits(user.id, GetUserVisitsOptions::default()),
            Ok(UserVisits {
                visits: vec![
                    UserVisit {
                        mark: visit.mark,
                        visited_at: visit.visited_at,
                        place: location_data.place.unwrap(),
                    }
                ],
            })
        );
    }

    #[test]
    fn complex_update() {
        setup();

        let mut store = create_store();

        let user = old_user();
        store.add_user(user.clone()).unwrap();

        let old_location = old_location();
        store.add_location(old_location.clone()).unwrap();

        let new_location = new_location();
        store.add_location(new_location.clone()).unwrap();

        let visit = visit(&user, &old_location);
        store.add_visit(visit.clone()).unwrap();

        let visit_data = VisitData {
            location: Some(new_location.id),
            mark: Some(2),
            ..Default::default()
        };

        assert!(store.update_visit(visit.id, visit_data.clone()).is_ok());

        assert_eq!(
            store.get_visit(visit.id),
            Ok(Visit {
                location: new_location.id,
                mark: visit_data.mark.unwrap(),
                ..visit
            })
        );

        assert_eq!(
            store.get_location_avg(old_location.id, Default::default()),
            Ok(LocationRate {
                avg: 0f64,
            })
        );

        assert_eq!(
            store.get_location_avg(new_location.id, Default::default()),
            Ok(LocationRate {
                avg: visit_data.mark.unwrap() as f64,
            })
        );

        let new_place = "Teatr";
        let location_data = LocationData {
            place: Some(new_place.into()),
            ..Default::default()
        };

        assert!(store.update_location(new_location.id, location_data.clone()).is_ok());

        assert_eq!(
            store.get_location(new_location.id),
            Ok(Location {
                place: new_place.into(),
                ..new_location
            })
        );

        assert_eq!(
            store.get_user_visits(user.id, GetUserVisitsOptions::default()),
            Ok(UserVisits{
                visits: vec![
                    UserVisit {
                        mark: visit_data.mark.unwrap(),
                        visited_at: visit.visited_at,
                        place: new_place.into(),
                    },
                ],
            })
        );
    }

    #[test]
    fn get_location_avg_overflow() {
        let mut store = create_store();

        let user = old_user();
        store.add_user(user.clone()).unwrap();

        let location = old_location();
        store.add_location(location.clone()).unwrap();

        for i in 1_u32..100_u32 {
            let visit = Visit {
                id: i,
                user: user.id,
                location: location.id,
                mark: 5,
                visited_at: 0,
            };
            assert_eq!(store.add_visit(visit), Ok(Empty{}));
        }

        assert_eq!(store.get_location_avg(location.id, Default::default()), Ok(LocationRate{ avg: 5.0 }));
    }
}
