use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use surrealdb_types_derive::write_sql;

use crate as surrealdb_types;
use crate::sql::{SqlFormat, ToSql};
use crate::utils::escape::QuoteStr;

/// Represents a file reference in SurrealDB
///
/// A file reference points to a file stored in a bucket with a specific key.
/// This is used for file storage and retrieval operations.
///
/// A file's key starts with `/`. [`File::new`], deserialisation and every wire decoder add it
/// when it is missing, and so does the engine when it receives a file. A file built field by
/// field without it stands for the same file as one built with it.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(from = "FileFields")]
pub struct File {
	/// The bucket name where the file is stored
	pub bucket: String,
	/// The key/identifier for the file within the bucket, starting with `/`
	pub key: String,
}

/// A file as it is serialised, read back through [`File::new`] so its key starts with `/`.
#[derive(Deserialize)]
struct FileFields {
	bucket: String,
	key: String,
}

impl From<FileFields> for File {
	fn from(fields: FileFields) -> Self {
		File::new(fields.bucket, fields.key)
	}
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
	/// Writes the file with its key starting with `/`, the file every reader of it holds,
	/// whether or not this value was built with one.
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let key = if self.key.starts_with('/') {
			Cow::Borrowed(self.key.as_str())
		} else {
			Cow::Owned(format!("/{}", self.key))
		};
		if file_string_admits(&self.bucket, &key) {
			write_sql!(f, sql_fmt, "f\"{}:{}\"", self.bucket, key.as_ref())
		} else {
			write_sql!(f, sql_fmt, "type::file({}, {})", QuoteStr(&self.bucket), QuoteStr(&key))
		}
	}
}

/// Whether the file string `f"bucket:key"` reads back as `bucket` and `key`: the bucket is
/// non-empty and holds only alphanumerics, `_`, `-` and `.`, and the key starts with `/`, names
/// at least one character after it, and holds those characters and `/`.
fn file_string_admits(bucket: &str, key: &str) -> bool {
	let word = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.');
	!bucket.is_empty()
		&& bucket.chars().all(word)
		&& key
			.strip_prefix('/')
			.is_some_and(|rest| !rest.is_empty() && rest.chars().all(|c| word(c) || c == '/'))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn file_to_sql_uses_type_file_when_bucket_or_key_needs_quoting() {
		assert_eq!(File::new("bucket", "/a/b.txt").to_sql(), "f\"bucket:/a/b.txt\"");
		assert_eq!(
			File::new("bucket", "/my file.txt").to_sql(),
			"type::file('bucket', '/my file.txt')"
		);
		assert_eq!(
			File {
				bucket: String::new(),
				key: "/path".to_string(),
			}
			.to_sql(),
			"type::file('', '/path')"
		);
	}

	/// A key without its leading `/` is written, deserialised and decoded as the key with it, so
	/// the SQL a file renders to reads back as the file every reader holds.
	#[test]
	fn a_key_without_its_leading_slash_is_read_with_it() {
		let bare = File {
			bucket: "bucket".to_string(),
			key: "a/b.txt".to_string(),
		};
		assert_eq!(bare.to_sql(), "f\"bucket:/a/b.txt\"");
		assert_eq!(
			File {
				bucket: "bucket".to_string(),
				key: "my file.txt".to_string(),
			}
			.to_sql(),
			"type::file('bucket', '/my file.txt')"
		);
		let read: File =
			serde_json::from_str(r#"{ "bucket": "bucket", "key": "a/b.txt" }"#).unwrap();
		assert_eq!(read, File::new("bucket", "/a/b.txt"));
	}
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
