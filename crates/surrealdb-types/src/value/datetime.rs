use chrono::{DateTime, Utc};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Datetime(pub DateTime<Utc>);