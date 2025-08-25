use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

/// Represents a datetime value in SurrealDB
///
/// A datetime represents a specific point in time, stored as UTC.
/// This type wraps the `chrono::DateTime<Utc>` type.
#[derive(
	Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
pub struct Datetime(pub DateTime<Utc>);

impl Datetime {
	/// The minimum UTC datetime
	pub const MIN_UTC: Self = Datetime(DateTime::<Utc>::MIN_UTC);
	/// The maximum UTC datetime
	pub const MAX_UTC: Self = Datetime(DateTime::<Utc>::MAX_UTC);

	/// Convert the Datetime to a raw String
	pub fn into_raw_string(&self) -> String {
		self.0.to_rfc3339_opts(SecondsFormat::AutoSi, true)
	}

	/// Convert to nanosecond timestamp.
	pub fn to_u64(&self) -> Option<u64> {
		self.0.timestamp_nanos_opt().map(|v| v as u64)
	}
}
