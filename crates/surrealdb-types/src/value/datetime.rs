use std::ops::Deref;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Represents a datetime value in SurrealDB
///
/// A datetime represents a specific point in time, stored as UTC.
/// This type wraps the `chrono::DateTime<Utc>` type.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Datetime(pub(crate) DateTime<Utc>);

impl Default for Datetime {
	fn default() -> Self {
		Self(Utc::now())
	}
}

impl Datetime {
	/// The minimum UTC datetime
	pub const MIN_UTC: Self = Datetime(DateTime::<Utc>::MIN_UTC);
	/// The maximum UTC datetime
	pub const MAX_UTC: Self = Datetime(DateTime::<Utc>::MAX_UTC);

	/// Returns the current UTC datetime
	pub fn now() -> Self {
		Self(Utc::now())
	}
}

impl From<DateTime<Utc>> for Datetime {
	fn from(v: DateTime<Utc>) -> Self {
		Self(v)
	}
}

impl From<Datetime> for DateTime<Utc> {
	fn from(x: Datetime) -> Self {
		x.0
	}
}

impl Deref for Datetime {
	type Target = DateTime<Utc>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
