use serde::{Deserialize, Serialize};

/// Represents a UUID value in SurrealDB
///
/// A UUID (Universally Unique Identifier) is a 128-bit identifier that is unique across space and
/// time. This type wraps the `uuid::Uuid` type.
#[derive(
	Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
pub struct Uuid(pub uuid::Uuid);
