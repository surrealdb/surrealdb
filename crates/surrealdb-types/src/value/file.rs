use serde::{Deserialize, Serialize};

/// Represents a file reference in SurrealDB
///
/// A file reference points to a file stored in a bucket with a specific key.
/// This is used for file storage and retrieval operations.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct File {
	/// The bucket name where the file is stored
	pub bucket: String,
	/// The key/identifier for the file within the bucket
	pub key: String,
}
