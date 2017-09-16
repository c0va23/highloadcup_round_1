use std::sync::{
    RwLock,
    PoisonError,
};

use chrono::prelude::*;

use super::models::*;

const AVG_ACCURACY: f64 = 5.0_f64;

#[derive(Debug, PartialEq, Clone)]
pub enum StoreError {
    EntryExists,
    EntityNotExists,
    InvalidEntity(ValidationError),
    UnexpectedIndex {
        vec_len: usize,
        new_index: usize,
    },
    LockError,
}

impl<Guard> From<PoisonError<Guard>> for StoreError {
    fn from(_err: PoisonError<Guard>) -> Self {
        StoreError::LockError
    }
}

pub struct Store {
    now: DateTime<Utc>,
    users: Vec<(User, Vec<(usize, usize)>)>,
    locations: Vec<(Location, Vec<(usize, usize)>)>,
    visits: Vec<Visit>,
}

impl Store {
    pub fn new(now: Timestamp) -> Self {
        let now = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(now, 0),
            Utc,
        );
        Self {
            now: now,
            users: Vec::new(),
            locations: Vec::new(),
            visits: Vec::new(),
        }
    }

    pub fn get_user(&self, id: Id) -> Result<User, StoreError> {
        let user_index = id_to_index(id);
        if self.users.len() > user_index {
            Ok(self.users[user_index].0.clone())
        } else {
            Err(StoreError::EntityNotExists)
        }
    }

    pub fn add_user(&mut self, user: User) -> Result<Empty, StoreError> {
        debug!("Add user {:?}", user);

        let user_index = id_to_index(user.id);
        if self.users.len() > user_index {
            return Err(StoreError::EntryExists)
        }

        if self.users.len() != user_index {
            return Err(StoreError::UnexpectedIndex{
                new_index: user_index,
                vec_len: self.users.len(),
            })
        }

        if let Err(error) = user.valid() {
            return Err(StoreError::InvalidEntity(error))
        }

        self.users.push((user, Vec::new()));
        Ok(Empty{})
    }

    pub fn update_user(&mut self, id: Id, user_data: UserData) -> Result<Empty, StoreError> {
        debug!("Update user {} {:?}", id, user_data);
        let user_index = id_to_index(id);

        if self.users.len() < user_index {
            return Err(StoreError::EntityNotExists)
        }

        let mut updated_user = self.users[user_index].0.clone();

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

        self.users[user_index].0 = updated_user;

        Ok(Empty{})
    }

    pub fn get_location(&self, id: Id) -> Result<Location, StoreError> {
        let location_index = id_to_index(id);
        if self.locations.len() > location_index {
            Ok(self.locations[location_index].0.clone())
        } else {
            Err(StoreError::EntityNotExists)
        }
    }

    pub fn add_location(&mut self, location: Location) -> Result<Empty, StoreError> {
        debug!("Add location {:?}", location);

        let location_index = location.index();

        if self.locations.len() > location_index {
            return Err(StoreError::EntryExists)
        }

        if self.locations.len() != location_index {
            return Err(StoreError::UnexpectedIndex {
                new_index: location_index,
                vec_len: self.locations.len(),

            })
        }

        if let Err(error) = location.valid() {
            return Err(StoreError::InvalidEntity(error))
        }

        self.locations.push((location, Vec::new()));
        Ok(Empty{})
    }

    pub fn update_location(&mut self, id: Id, location_data: LocationData) -> Result<Empty, StoreError> {
        debug!("Update location {} {:?}", id, location_data);
        let location_index = id_to_index(id);
        if self.locations.len() < location_index {
            return Err(StoreError::EntityNotExists)
        }

        let location = &mut self.locations[location_index];
        let mut updated_location = location.0.clone();

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

        location.0 = updated_location;

        Ok(Empty{})
    }

    pub fn get_visit(&self, visit_id: Id) -> Result<Visit, StoreError> {
        let visit_index = id_to_index(visit_id);
        if self.visits.len() > visit_index {
            Ok(self.visits[visit_index].clone())
        } else {
            Err(StoreError::EntityNotExists)
        }
    }

    fn add_visit_to_user(
        &mut self,
        visit: &Visit,
        location: &Location,
    ) {
        let position = {
            let user_visits = &self.users[visit.user_index()].1;

            user_visits.iter().position(|&(visit_index, _)|
                visit.visited_at < self.visits[visit_index].visited_at
            )
        };

        let pair = (visit.index(), location.index());

        let user_visits = &mut self.users[visit.user_index()].1;

        match position {
            Some(position) => user_visits.insert(position, pair),
            None => user_visits.push(pair),
        }
    }

    fn remove_visit_from_user(
        &mut self,
        visit: &Visit,
    ) {
        let position = {
            let user_visits = &self.users[visit.user_index()].1;

            let visit_index = visit.index();
            user_visits.iter().position(|&(index, _)|
                visit_index == index
            )
        };

        let user_visits = &mut self.users[visit.user_index()].1;

        if let Some(position) = position {
            user_visits.remove(position);
        } else {
            error!("Visit {} not found on user {}", visit.id, visit.user);
        }
    }

    fn add_visit_to_location(
        &mut self,
        visit: &Visit,
        user: &User,
    ) {
        let location_visits = &mut self.locations[visit.location_index()].1;

        location_visits.push((visit.index(), user.index()));
    }

    fn remove_visit_from_location(
        &mut self,
        visit: &Visit,
    ) {
        let position = {
            let location_visits = &self.locations[visit.location_index()].1;

            let visit_index = visit.index();
            location_visits.iter().position(|&(index, _)|
                visit_index == index
            )
        };

        let location_visits = &mut self.locations[visit.location_index()].1;

        if let Some(position) = position {
            location_visits.remove(position);
        } else {
            error!("Visit {} not found on location {}", visit.id, visit.location);
        }
    }

    fn get_visit_user(&self, user_id: Id) -> Result<User, StoreError> {
        let user_index = id_to_index(user_id);
        if self.users.len() <= user_index {
            Err(StoreError::InvalidEntity(ValidationError{
                field: "user".to_string(),
                message: format!("User with ID {} not exists", user_id),
            }))
        } else {
            Ok(self.users[user_index].0.clone())
        }
    }

    fn get_visit_location(&self, location_id: Id) -> Result<Location, StoreError> {
        let location_index = id_to_index(location_id);

        if self.locations.len() <= location_index {
            Err(StoreError::InvalidEntity(ValidationError{
                field: "location".to_string(),
                message: format!("Location with ID {} not exists", location_id),
            }))
        } else {
            Ok(self.locations[location_index].0.clone())
        }
    }

    pub fn add_visit(&mut self, visit: Visit) -> Result<Empty, StoreError> {
        debug!("Add visit {:?}", visit);

        let visit_index = visit.index();
        if self.visits.len() > visit_index {
            return Err(StoreError::EntryExists)
        }

        if self.visits.len() != visit_index {
            return Err(StoreError::UnexpectedIndex {
                new_index: visit_index,
                vec_len: self.visits.len(),
            })
        }

        if let Err(error) = visit.valid() {
            return Err(StoreError::InvalidEntity(error))
        }

        let user = self.get_visit_user(visit.user)?;
        let location = self.get_visit_location(visit.location)?;

        self.add_visit_to_user(&visit, &location);
        self.add_visit_to_location(&visit, &user);

        self.visits.push(visit);

        Ok(Empty{})
    }

    pub fn update_visit(&mut self, id: Id, visit_data: VisitData) -> Result<Empty, StoreError> {
        debug!("Update visit {} {:?}", id, visit_data);

        let visit_index = id_to_index(id);
        if self.visits.len() <= visit_index {
            return Err(StoreError::EntityNotExists)
        }

        let original_visit = self.visits[visit_index].clone();

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
        self.visits[visit_index] = updated_visit.clone();

        if original_visit.user != updated_visit.user ||
                original_visit.visited_at != updated_visit.visited_at ||
                original_visit.location != updated_visit.location {
            debug!("Update visit user from {} to {}", original_visit.user, updated_visit.user);
            self.remove_visit_from_user(&original_visit);
            self.add_visit_to_user(&updated_visit, &location);
        }
        if original_visit.location != updated_visit.location || original_visit.user != updated_visit.user {
            debug!("Update visit locatoin from {} to {}", original_visit.location, updated_visit.location);
            self.remove_visit_from_location(&original_visit);
            self.add_visit_to_location(&updated_visit, &user);
        }

        Ok(Empty{})
    }

    pub fn get_user_visits(&self, user_id: Id, options: GetUserVisitsOptions) ->
            Result<UserVisits, StoreError> {
        debug!("Get user {} visits by {:?}", user_id, options);

        let user_index = id_to_index(user_id);
        if self.users.len() <= user_index {
            return Err(StoreError::EntityNotExists)
        }

        let user_visits = self.users[user_index].1
            .iter()
            .map(|&(visit_index, location_index)|
                (self.visits[visit_index].clone(), self.locations[location_index].0.clone())
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

        let location_index = id_to_index(location_id);
        if self.locations.len() <= location_index {
            return Err(StoreError::EntityNotExists)
        }

        let location_visits = &self.locations[location_index].1;

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

        let filtered_location_visits = location_visits
            .iter()
            .map(|&(visit_index, user_index)|
                (self.visits[visit_index].clone(), self.users[user_index].0.clone())
            )
            .filter(|&(ref v, ref u)| {
                (if let Some(from_date) = options.from_date { v.visited_at > from_date } else { true })
                && if let Some(to_date) = options.to_date { v.visited_at < to_date } else { true }
                && if let Some(gender) = options.gender { u.gender == gender } else { true }
                && if let Some(from_age) = from_age { u.birth_date < from_age } else { true }
                && if let Some(to_age) = to_age { u.birth_date > to_age } else { true }
            }).collect::<Vec<(Visit, User)>>();

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
        store.add_visit(old_visit.clone()).unwrap();

        let new_visit = Visit { id: 2, location: new_location.id, user: user.id, visited_at: 2, mark: 4 };
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
            store.add_visit(visit).unwrap();
        }

        assert_eq!(store.get_location_avg(location.id, Default::default()), Ok(LocationRate{ avg: 5.0 }));
    }
}
