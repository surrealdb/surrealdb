use serde::{Deserialize, Serialize};

use crate::sql::ToSql;
use crate::write_sql;

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

impl ToSql for crate::File {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "f\"{}:{}\"", fmt_inner(&self.bucket, true), fmt_inner(&self.key, false));
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

#[cfg(feature = "arbitrary")]
mod arb {
	use super::*;
	impl<'a> arbitrary::Arbitrary<'a> for File {
		fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
			static CHAR: [u8; 56] = [
				b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h', b'i', b'j', b'k', b'l', b'm', b'n',
				b'o', b'p', b'q', b'r', b's', b't', b'u', b'v', b'w', b'x', b'y', b'z', b'A', b'B',
				b'C', b'D', b'E', b'F', b'G', b'H', b'I', b'J', b'K', b'L', b'M', b'N', b'O', b'P',
				b'Q', b'R', b'S', b'T', b'U', b'V', b'W', b'X', b'Y', b'Z', b'_', b'-', b'.', b'/',
			];

			let mut bucket = String::new();
			// Forward slash is not allowed in the bucket name so we exclude it by limiting the
			// range.
			bucket.push(CHAR[u.int_in_range(0u8..=54)? as usize] as char);
			for _ in 0..u.arbitrary_len::<u8>()? {
				bucket.push(CHAR[u.int_in_range(0u8..=54)? as usize] as char);
			}
			let mut key = "/".to_string();
			for _ in 0..u.arbitrary_len::<u8>()? {
				key.push(CHAR[u.int_in_range(0u8..=55)? as usize] as char);
			}
			Ok(File {
				bucket,
				key,
			})
		}
	}
}
