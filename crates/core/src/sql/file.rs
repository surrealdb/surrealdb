use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::Ident;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::File";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd)]
#[serde(rename = "$surrealdb::private::sql::File")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct File {
	pub bucket: Ident,
	pub key: String,
}

impl fmt::Display for File {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "file:/{}{}", &self.bucket, &self.key)
	}
}
