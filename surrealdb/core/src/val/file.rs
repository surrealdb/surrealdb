use revision::{Revisioned, SerializeRevisioned, revisioned};
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::val::IndexFormat;

#[revisioned(revision = 1, serialize = false)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Encode, BorrowDecode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub struct File {
	pub bucket: String,
	pub key: String,
}

impl File {
	pub(crate) fn new(bucket: String, key: String) -> Self {
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

	pub(crate) fn display_inner(&self) -> String {
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

// This has a custom serialization to ensure that the bucket is not empty. It should not be but this
// is a safe guard to ensure there's no way for it to slip in if written to disk.
impl SerializeRevisioned for File {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		if self.bucket.is_empty() {
			return Err(revision::Error::Serialize("bucket is empty".to_string()));
		}
		SerializeRevisioned::serialize_revisioned(&Self::revision(), writer)?;
		SerializeRevisioned::serialize_revisioned(&self.bucket, writer)?;
		SerializeRevisioned::serialize_revisioned(&self.key, writer)?;
		Ok(())
	}
}
