use std::fmt;

use crate::sql::Ident;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct File {
	pub bucket: String,
	pub key: String,
}

impl File {
	/// Check if this File belongs to a certain bucket type
	pub fn is_bucket_type(&self, types: &[Ident]) -> bool {
		types.is_empty() || types.iter().any(|buc| **buc == self.bucket)
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

impl From<File> for crate::val::File {
	fn from(v: File) -> Self {
		Self {
			bucket: v.bucket,
			key: v.key,
		}
	}
}
impl From<crate::val::File> for File {
	fn from(v: crate::val::File) -> Self {
		Self {
			bucket: v.bucket,
			key: v.key,
		}
	}
}
