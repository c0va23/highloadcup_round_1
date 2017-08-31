pub type Id = u32;
pub type Timestamp = i64;
pub type Mark = u8;

pub trait Validate {
    fn valid(&self) -> bool;
}

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

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindVisitOptions {
    pub from_date: Option<Timestamp>,
    pub to_date: Option<Timestamp>,
    pub country: Option<String>,
    pub to_distance: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
pub struct UserVisit {
    pub mark: Mark,
    pub visited_at: Timestamp,
    pub place: String,
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct UserVisits {
    pub visits: Vec<UserVisit>
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocationRateOptions {
    pub from_date: Option<Timestamp>,
    pub to_date: Option<Timestamp>,
    pub from_age: Option<i32>,
    pub to_age: Option<i32>,
    pub gender: Option<char>,
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct LocationRate {
    pub avg: f64,
}

impl Validate for User {
    fn valid(&self) -> bool {
        self.email.len() <= 100
            && self.first_name.len() <= 50
            && self.last_name.len() <= 50
            && (self.gender == 'f' || self.gender == 'm')
    }
}

impl Validate for Location {
    fn valid(&self) -> bool {
        self.country.len() <= 50
            && self.city.len() <= 50
    }
}

impl Validate for Visit {
    fn valid(&self) -> bool {
        self.mark <= 5
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct Empty{}
