use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self};

use super::Ident;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::File";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd)]
#[serde(rename = "$surrealdb::private::sql::File")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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
	pub fn is_bucket_type(&self, types: &[Ident]) -> bool {
		types.is_empty() || types.iter().any(|buc| buc.0 == self.bucket)
	}
}

impl fmt::Display for File {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "f\"{}:{}\"", fmt_inner(&self.bucket, true), fmt_inner(&self.key, false))
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
