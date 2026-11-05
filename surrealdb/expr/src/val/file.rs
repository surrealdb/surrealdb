use common::fmt::QuoteStr;
use revision::{DeserializeRevisioned, Error, SkipCheckRevisioned, revisioned};
use storekey::{BorrowDecode, Encode};
use surrealdb_collections::bounded;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::val::{IndexFormat, IndexIdFormat};

#[revisioned(revision = 1, deserialize = false, skip = true, walk = true)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Encode, BorrowDecode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
#[storekey(format = "IndexIdFormat")]
pub struct File {
	pub bucket: String,
	pub key: String,
}

/// Reads exactly what the derived serializer writes (revision 1: the bucket, then the key) and
/// builds the value from its fields: `File::new` would prepend a `/` to a key that lacks one, which
/// is not what was stored.
impl DeserializeRevisioned for File {
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
		bounded::read_revision(reader, "File", 1)?;
		let bucket = bounded::read_string(reader)?;
		let key = bounded::read_string(reader)?;
		Ok(Self {
			bucket,
			key,
		})
	}
}

impl SkipCheckRevisioned for File {
	fn skip_check_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), Error> {
		Self::deserialize_revisioned(reader).map(drop)
	}
}

impl File {
	pub fn new(bucket: String, key: String) -> Self {
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

	/// Check if this File belongs to a certain bucket type
	pub fn is_bucket_type(&self, types: &[String]) -> bool {
		types.is_empty() || types.contains(&self.bucket)
	}

	pub fn display_inner(&self) -> String {
		format!("{}:{}", fmt_inner(&self.bucket, true), fmt_inner(&self.key, false))
	}
}

impl From<surrealdb_types::File> for File {
	/// The file `v` names, its key starting with `/` whether or not `v`'s does.
	fn from(v: surrealdb_types::File) -> Self {
		let v = surrealdb_types::File::new(v.bucket, v.key);
		Self {
			bucket: v.bucket,
			key: v.key,
		}
	}
}

impl From<File> for surrealdb_types::File {
	fn from(x: File) -> Self {
		surrealdb_types::File::new(x.bucket, x.key)
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

impl ToSql for File {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		// A file string has no escapes, so it can hold only the characters its grammar admits. A
		// file outside them is written as the function that builds it, which takes any string.
		if file_string_admits(&self.bucket, &self.key) {
			write_sql!(f, sql_fmt, "f\"{}:{}\"", self.bucket, self.key)
		} else {
			write_sql!(
				f,
				sql_fmt,
				"type::file({}, {})",
				QuoteStr(&self.bucket),
				QuoteStr(&self.key)
			)
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
	fn file_string_admits_only_a_named_bucket_and_key() {
		assert!(file_string_admits("bucket", "/a/b.txt"));
		assert!(!file_string_admits("", "/a"));
		assert!(!file_string_admits("bucket", "/"));
		assert!(!file_string_admits("bucket", ""));
		assert!(!file_string_admits("bucket", "a"));
		assert!(!file_string_admits("bu cket", "/a"));
	}

	#[test]
	fn an_unadmitted_file_is_written_as_the_function_that_builds_it() {
		let file = File {
			bucket: String::new(),
			key: "/path".to_string(),
		};
		assert_eq!(file.to_sql(), "type::file('', '/path')");
	}
}
