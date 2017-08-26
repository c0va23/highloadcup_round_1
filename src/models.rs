pub type Id = u32;
pub type Timestamp = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct User {
    pub id: Id,
    pub email: String,
    pub first_name: String,
    pub last_name: String,
    pub gender: char,
    pub birth_date: Timestamp,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UserData {
    pub email: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub gender: Option<char>,
    pub birth_date: Option<Timestamp>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Location {
    pub id: Id,
    pub place: String,
    pub country: String,
    pub city: String,
    pub distance: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct LocationData {
    pub place: Option<String>,
    pub country: Option<String>,
    pub city: Option<String>,
    pub distance: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Visit {
    pub id: Id,
    pub location: Id,
    pub user: Id,
    pub visited_at: Timestamp,
    pub mark: u8,
}

#[derive(Clone, Debug, Deserialize)]
pub struct VisitData {
    pub location: Option<Id>,
    pub user: Option<Id>,
    pub visited_at: Option<Timestamp>,
    pub mark: Option<u8>,
}
