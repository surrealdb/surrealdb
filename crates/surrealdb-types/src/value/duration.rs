use serde::{Deserialize, Serialize};

/// Represents a duration value in SurrealDB
///
/// A duration represents a span of time, typically used for time-based calculations and
/// comparisons. This type wraps the standard `std::time::Duration` type.
#[derive(
	Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
pub struct Duration(pub std::time::Duration);
