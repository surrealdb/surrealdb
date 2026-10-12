use revision::{DeserializeRevisioned, Error, SkipCheckRevisioned, revisioned};
use storekey::{BorrowDecode, Encode};
use surrealdb_collections::bounded;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::val::IndexFormat;

#[revisioned(revision = 1, deserialize = false, skip = true, walk = true)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Encode, BorrowDecode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
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
	fn from(v: surrealdb_types::File) -> Self {
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
		write_sql!(f, sql_fmt, "f\"{}\"", self.display_inner())
	}
}
