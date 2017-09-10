use std::cell::RefCell;
use std::rc::Rc;

use chrono::prelude::*;
use fnv::FnvHashMap;

use super::models::*;

const AVG_ACCURACY: f64 = 5.0_f64;

type Map<K, V> = FnvHashMap<K, V>;

#[derive(Debug, PartialEq, Clone)]
pub enum StoreError {
    EntryExists,
    EntityNotExists,
    InvalidEntity(ValidationError),
}

pub struct Store {
    users: RefCell<Map<Id, Rc<RefCell<User>>>>,
    locations: RefCell<Map<Id, Rc<RefCell<Location>>>>,
    visits: RefCell<Map<Id, Rc<RefCell<Visit>>>>,
    users_visits: RefCell<Map<Id, Vec<(Rc<RefCell<Visit>>, Rc<RefCell<Location>>)>>>,
    locations_visits: RefCell<Map<Id, Vec<(Rc<RefCell<Visit>>, Rc<RefCell<User>>)>>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            users: RefCell::new(Map::default()),
            locations: RefCell::new(Map::default()),
            visits: RefCell::new(Map::default()),
            users_visits: RefCell::new(Map::default()),
            locations_visits: RefCell::new(Map::default()),
        }
    }

    pub fn get_user(&self, id: Id) -> Result<User, StoreError> {
        Ok(self.users.borrow().get(&id).map(|u| u.borrow().clone()).ok_or(StoreError::EntityNotExists)?)
    }

    pub fn add_user(&self, user: User) -> Result<Empty, StoreError> {
        debug!("Add user {:?}", user);

        if self.users.borrow().get(&user.id).is_some() {
            return Err(StoreError::EntryExists)
        }

        if let Err(error) = user.valid() {
            return Err(StoreError::InvalidEntity(error))
        }

        self.users.borrow_mut().insert(user.id, Rc::new(RefCell::new(user)));
        Ok(Empty{})
    }

    pub fn update_user(&self, id: Id, user_data: UserData) -> Result<Empty, StoreError> {
        debug!("Update user {} {:?}", id, user_data);
        if let Some(user) = self.users.borrow().get(&id) {
            let mut updated_user = user.borrow().clone();
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
            *user.borrow_mut() = updated_user;
            Ok(Empty{})
        } else {
            Err(StoreError::EntityNotExists)
        }
    }

    pub fn get_location(&self, id: Id) -> Result<Location, StoreError> {
        Ok(self.locations.borrow().get(&id).map(|l| l.borrow().clone()).ok_or(StoreError::EntityNotExists)?)
    }

    pub fn add_location(&self, location: Location) -> Result<Empty, StoreError> {
        debug!("Add location {:?}", location);

        if self.locations.borrow().get(&location.id).is_some() {
            return Err(StoreError::EntryExists)
        }
        if let Err(error) = location.valid() {
            return Err(StoreError::InvalidEntity(error))
        }
        self.locations.borrow_mut().insert(location.id, Rc::new(RefCell::new(location)));
        Ok(Empty{})
    }

    pub fn update_location(&self, id: Id, location_data: LocationData) -> Result<Empty, StoreError> {
        debug!("Update location {} {:?}", id, location_data);
        if let Some(location) = self.locations.borrow().get(&id) {
            let mut updated_location = location.borrow().clone();
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
            *location.borrow_mut() = updated_location;
            Ok(Empty{})
        } else {
            Err(StoreError::EntityNotExists)
        }
    }

    pub fn get_visit(&self, id: Id) -> Result<Visit, StoreError> {
        Ok(self.visits.borrow().get(&id).map(|v| v.borrow().clone()).ok_or(StoreError::EntityNotExists)?)
    }

    fn add_visit_to_user(
        &self,
        visit: Rc<RefCell<Visit>>,
        location: Rc<RefCell<Location>>,
    ) {
        let (user, visited_at) = {
            let v = visit.borrow();
            (v.user, v.visited_at)
        };

        let mut users_visits = self.users_visits.borrow_mut();
        let user_visits = users_visits
            .entry(user)
            .or_insert(Vec::new());

        let position = user_visits.iter().position(|&(ref visit, _)|
            visited_at < visit.borrow().visited_at
        );

        let pair = (visit.clone(), location.clone());

        match position {
            Some(position) => user_visits.insert(position, pair),
            None => user_visits.push(pair),
        }
    }

    fn remove_visit_from_user(
        &self,
        visit: &Visit,
    ) {
        let mut users_visits = self.users_visits.borrow_mut();
        let user_visits = users_visits
            .entry(visit.user)
            .or_insert(Vec::new());

        let position = user_visits.iter().position(|&(ref v, _)|
            visit.id == v.borrow().id
        );

        if let Some(position) = position {
            user_visits.remove(position);
        } else {
            error!("Visit {} not found on user {}", visit.id, visit.user);
        }
    }

    fn add_visit_to_location(
        &self,
        visit: Rc<RefCell<Visit>>,
        user: Rc<RefCell<User>>,
    ) {
        let mut locations_visits = self.locations_visits.borrow_mut();
        let location_visits = locations_visits
            .entry(visit.borrow().location)
            .or_insert(Vec::new());

        location_visits.push((visit.clone(), user.clone()));
    }

    fn remove_visit_from_location(
        &self,
        visit: &Visit,
    ) {
        let mut locations_visits = self.locations_visits.borrow_mut();
        let location_visits = locations_visits
            .entry(visit.location)
            .or_insert(Vec::new());

        let position = location_visits.iter().position(|&(ref v, _)|
            v.borrow().id == visit.id
        );

        if let Some(position) = position {
            location_visits.remove(position);
        } else {
            error!("Visit {} not found on location {}", visit.id, visit.location);
        }
    }

    fn get_visit_user(&self, user_id: Id) -> Result<Rc<RefCell<User>>, StoreError> {
        self.users.borrow().get(&user_id).ok_or_else(||
            StoreError::InvalidEntity(ValidationError{
                field: "user".to_string(),
                message: format!("User with ID {} not exists", user_id),
            })
        ).map(|u| u.clone())
    }

    fn get_visit_location(&self, location_id: Id) -> Result<Rc<RefCell<Location>>, StoreError> {
        self.locations.borrow().get(&location_id).ok_or_else(||
            StoreError::InvalidEntity(ValidationError{
                field: "location".to_string(),
                message: format!("Location with ID {} not exists", location_id),
            })
        ).map(|l| l.clone())
    }

    pub fn add_visit(&self, visit: Visit) -> Result<Empty, StoreError> {
        debug!("Add visit {:?}", visit);

        if self.visits.borrow().get(&visit.id).is_some() {
            return Err(StoreError::EntryExists)
        }

        if let Err(error) = visit.valid() {
            return Err(StoreError::InvalidEntity(error))
        }

        let user = self.get_visit_user(visit.user)?.clone();
        let location = self.get_visit_location(visit.location)?.clone();

        let visit_id = visit.id;
        let visit_arc = Rc::new(RefCell::new(visit));

        self.add_visit_to_user(visit_arc.clone(), location);
        self.add_visit_to_location(visit_arc.clone(), user);

        self.visits.borrow_mut().insert(visit_id, visit_arc.clone());
        Ok(Empty{})
    }

    pub fn update_visit(&self, id: Id, visit_data: VisitData) -> Result<Empty, StoreError> {
        debug!("Update visit {} {:?}", id, visit_data);

        let visit = self.visits.borrow().get(&id).ok_or(StoreError::EntityNotExists)?.clone();
        let original_visit = visit.borrow().clone();
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

        debug!("Replace visit {:?} wiht {:?}", visit, updated_visit);
        *visit.borrow_mut() = updated_visit.clone();

        if original_visit.user != updated_visit.user ||
                original_visit.visited_at != updated_visit.visited_at ||
                original_visit.location != updated_visit.location {
            debug!("Update visit user from {} to {}", original_visit.user, updated_visit.user);
            self.remove_visit_from_user(&original_visit);
            self.add_visit_to_user(visit.clone(), location);
        }
        if original_visit.location != updated_visit.location {
            debug!("Update visit locatoin from {} to {}", original_visit.location, updated_visit.location);
            self.remove_visit_from_location(&original_visit);
            self.add_visit_to_location(visit.clone(), user);
        }

        Ok(Empty{})
    }

    pub fn get_user_visits(&self, user_id: Id, options: GetUserVisitsOptions) ->
            Result<UserVisits, StoreError> {
        debug!("Get user {} visits by {:?}", user_id, options);

        if self.users.borrow().get(&user_id).is_none() {
            return Err(StoreError::EntityNotExists)
        }

        let user_visits = match self.users_visits.borrow().get(&user_id) {
            Some(user_visits) => user_visits.clone(),
            None => return Ok(UserVisits::default()),
        };

        let user_visits = user_visits
            .iter()
            .map(|&(ref v, ref l)|
                (v.borrow(), l.borrow())
            )
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

        if self.locations.borrow().get(&location_id).is_none() {
            return Err(StoreError::EntityNotExists)
        }

        let location_visits = match self.locations_visits.borrow().get(&location_id) {
            Some(ids) => ids.clone(),
            None => return Ok(LocationRate::default()),
        };

        debug!("Location visits: {:?}", location_visits);

        let now = Utc::now();
        debug!("Now {}", now);

        let from_age = options.from_age.and_then(|from_age| now.with_year(now.year() - from_age))
            .map(|t| t.timestamp());
        debug!("Age from {:?}", from_age);

        let to_age = options.to_age.and_then(|to_age| now.with_year(now.year() - to_age))
            .map(|t| t.timestamp());
        debug!("Age to {:?}", to_age);

        let (sum_mark, count_mark) = location_visits
            .iter()
            .map(|&(ref v, ref u)|
                (v.borrow(), u.borrow())
            )
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

    #[allow(unused_must_use)]
    fn setup() {
        env_logger::init();
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

        let mut store = Store::new();

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

    #[test]
    fn update_visit_with_valid_mark() {
        setup();

        let mut store = Store::new();

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
    fn update_visit_with_valid_visited_at() {
        setup();

        let mut store = Store::new();

        let user = old_user();
        store.add_user(user.clone()).unwrap();

        let new_location = new_location();
        store.add_location(new_location.clone()).unwrap();

        let old_location = old_location();
        store.add_location(old_location.clone()).unwrap();

        let old_visit = Visit { id: 0, location: old_location.id, user: user.id, visited_at: 1, mark: 3 };
        store.add_visit(old_visit.clone()).unwrap();

        let new_visit = Visit { id: 1, location: new_location.id, user: user.id, visited_at: 2, mark: 4 };
        store.add_visit(new_visit.clone()).unwrap();

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

        let mut store = Store::new();

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

        let mut store = Store::new();

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

        let mut store = Store::new();

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

        let mut store = Store::new();

        let user = new_user();
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
}
