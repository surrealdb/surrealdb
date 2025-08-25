use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Represents a datetime value in SurrealDB
///
/// A datetime represents a specific point in time, stored as UTC.
/// This type wraps the `chrono::DateTime<Utc>` type.
#[derive(
	Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
pub struct Datetime(pub DateTime<Utc>);
