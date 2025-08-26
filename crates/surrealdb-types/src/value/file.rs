use serde::{Deserialize, Serialize};

/// Represents a file reference in SurrealDB
///
/// A file reference points to a file stored in a bucket with a specific key.
/// This is used for file storage and retrieval operations.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct File {
	/// The bucket name where the file is stored
	pub(crate) bucket: String,
	/// The key/identifier for the file within the bucket
	pub(crate) key: String,
}

impl File {
	/// Create a new file pointer
	pub fn new<B: Into<String>, K: Into<String>>(bucket: B, key: K) -> Self {
		let bucket: String = bucket.into();
		let key: String = key.into();

		let key = if key.starts_with("/") {
			key
		} else {
			format!("/{key}")
		};

		Self {
			bucket,
			key,
		}
	}

	/// Get the bucket name
	pub fn bucket(&self) -> &str {
		&self.bucket
	}

	/// Get the key/identifier for the file within the bucket
	/// The key always starts with a "/"
	pub fn key(&self) -> &str {
		&self.key
	}
}
