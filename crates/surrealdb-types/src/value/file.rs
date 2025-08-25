use std::fmt::Display;

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

impl File {
	/// Display the file without the `f` prefix
	pub fn display_inner(&self) -> String {
		format!("{}:{}", fmt_inner(&self.bucket, true), fmt_inner(&self.key, false))
	}
}

impl Display for File {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "f\"{}\"", self.display_inner())
	}
}

fn fmt_inner(v: &str, escape_slash: bool) -> String {
	v.chars()
		.flat_map(|c| {
			if c.is_ascii_alphanumeric()
				|| matches!(c, '-' | '_' | '.')
				|| (!escape_slash && c == '/')
			{
				vec![c]
			} else {
				vec!['\\', c]
			}
		})
		.collect::<String>()
}
