use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::utils::escape::QuoteStr;

/// Represents a UUID value in SurrealDB
///
/// A UUID (Universally Unique Identifier) is a 128-bit identifier that is unique across space and
/// time. This type wraps the `uuid::Uuid` type.
#[derive(
	Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
pub struct Uuid(pub uuid::Uuid);

impl Display for Uuid {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "u{}", &QuoteStr(&self.0.to_string()))
	}
}
