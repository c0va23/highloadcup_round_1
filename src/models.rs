use std::time;

type Id = u32;
type Timestamp = u64;

pub struct User {
    pub id: Id,
    pub email: String,
    pub first_name: String,
    pub last_name: String,
    pub gender: char,
    pub birth_date: Timestamp,
}

pub struct Location {
    pub id: Id,
    pub place: String,
    pub country: String,
    pub city: String,
    pub distance: u32,
}

pub struct Visit {
    pub id: Id,
    pub location: Id,
    pub user: Id,
    pub visited_at: Timestamp,
    pub mark: u8,
}
