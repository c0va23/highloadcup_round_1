pub type Id = u32;
pub type Timestamp = i64;
pub type Mark = u8;

#[derive(Debug, Clone, PartialEq)]
pub struct ValidationError {
    pub field: String,
    pub message: String,
}

pub type ValidationResult = Result<(), ValidationError>;

pub trait Validate {
    fn valid(&self) -> ValidationResult;
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Location {
    pub id: Id,
    pub place: String,
    pub country: String,
    pub city: String,
    pub distance: u32,
}

#[derive(Clone, Debug, Deserialize, Default)]
pub struct LocationData {
    pub place: Option<String>,
    pub country: Option<String>,
    pub city: Option<String>,
    pub distance: Option<u32>,
}

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
)]
pub struct Visit {
    pub id: Id,
    pub location: Id,
    pub user: Id,
    pub visited_at: Timestamp,
    pub mark: u8,
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Default,
)]
pub struct VisitData {
    pub location: Option<Id>,
    pub user: Option<Id>,
    pub visited_at: Option<Timestamp>,
    pub mark: Option<u8>,
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Default,
)]
#[serde(rename_all = "camelCase")]
pub struct GetUserVisitsOptions {
    pub from_date: Option<Timestamp>,
    pub to_date: Option<Timestamp>,
    pub country: Option<String>,
    pub to_distance: Option<u32>,
}

#[derive(
    Clone,
    Debug,
    Serialize,
    PartialEq,
)]
pub struct UserVisit {
    pub mark: Mark,
    pub visited_at: Timestamp,
    pub place: String,
}

#[derive(
    Clone,
    Debug,
    Serialize,
    Default,
    PartialEq
)]
pub struct UserVisits {
    pub visits: Vec<UserVisit>
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetLocationAvgOptions {
    pub from_date: Option<Timestamp>,
    pub to_date: Option<Timestamp>,
    pub from_age: Option<i32>,
    pub to_age: Option<i32>,
    pub gender: Option<char>,
}

#[derive(
    Clone,
    Debug,
    Serialize,
    Default,
    PartialEq,
)]
pub struct LocationRate {
    pub avg: f64,
}

impl User {
    const MAX_EMAIL_LEN: usize = 100;
    const MAX_NAME_LEN: usize = 500;
    const ALLOWED_GENDER: &'static [char] = &['f', 'm'];
}

impl Validate for User {
    fn valid(&self) -> ValidationResult {
        if self.email.len() > Self::MAX_EMAIL_LEN {
            Err(ValidationError {
                field: "email".to_string(),
                message: format!("Email len is {} (max {})", self.email.len(), Self::MAX_EMAIL_LEN),
            })
        } else if self.first_name.len() > Self::MAX_NAME_LEN {
            Err(ValidationError {
                field: "first_name".to_string(),
                message: format!("Name len is {} (max {})", self.first_name.len(), Self::MAX_NAME_LEN),
            })
        } else if self.last_name.len() > Self::MAX_NAME_LEN {
            Err(ValidationError {
                field: "last_name".to_string(),
                message: format!("Name len is {} (max {})", self.last_name.len(), Self::MAX_NAME_LEN),
            })
        } else if !Self::ALLOWED_GENDER.contains(&self.gender) {
            Err(ValidationError {
                field: "gender".to_string(),
                message: format!("Gender is {} (allowed {:?})", self.gender, Self::ALLOWED_GENDER),
            })
        } else {
            Ok(())
        }
    }
}

impl Location {
    const MAX_COUNTRY_LEN: usize = 50;
    const MAX_CITY_LEN: usize = 50;
}

impl Validate for Location {
    fn valid(&self) -> ValidationResult {
        if self.country.len() > Self::MAX_COUNTRY_LEN {
            Err(ValidationError {
                field: "contry".to_string(),
                message: format!("Contry len is {} (max {})", self.country.len(), Self::MAX_COUNTRY_LEN),
            })
        } else if self.city.len() > Self::MAX_CITY_LEN {
            Err(ValidationError{
                field: "city".to_string(),
                message: format!("City len is {} (max {})", self.city.len(), Self::MAX_CITY_LEN),
            })
        } else {
            Ok(())
        }
    }
}

impl Visit {
    const MAX_MARK: u8 = 5;
}

impl Validate for Visit {
    fn valid(&self) -> ValidationResult {
        if self.mark > Self::MAX_MARK {
            Err(ValidationError {
                field: "mark".to_string(),
                message: format!("Mark is {} (max {})", self.mark, Self::MAX_MARK),
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct Empty{}
