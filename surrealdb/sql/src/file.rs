use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct File {
	pub bucket: String,
	pub key: String,
}

impl File {
	/// Check if this File belongs to a certain bucket type
	pub fn is_bucket_type(&self, types: &[String]) -> bool {
		types.is_empty() || types.iter().any(|buc| **buc == self.bucket)
	}
}

impl ToSql for File {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "f\"{}:{}\"", fmt_inner(&self.bucket, true), fmt_inner(&self.key, false))
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

impl From<surrealdb_types::File> for File {
	fn from(v: surrealdb_types::File) -> Self {
		Self {
			bucket: v.bucket,
			key: v.key,
		}
	}
}
