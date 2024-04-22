use crate::sql::statements::info::InfoStructure;
use crate::sql::{escape::escape_ident, strand::no_nul_bytes, Value};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Ident(#[serde(with = "no_nul_bytes")] pub String);

impl From<String> for Ident {
	fn from(v: String) -> Self {
		Self(v)
	}
}

impl From<&str> for Ident {
	fn from(v: &str) -> Self {
		Self::from(String::from(v))
	}
}

impl Deref for Ident {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Ident {
	/// Convert the Ident to a raw String
	pub fn to_raw(&self) -> String {
		self.0.to_string()
	}
	/// Checks if this field is the `id` field
	pub(crate) fn is_id(&self) -> bool {
		self.0.as_str() == "id"
	}
	/// Checks if this field is the `type` field
	pub(crate) fn is_type(&self) -> bool {
		self.0.as_str() == "type"
	}
	/// Checks if this field is the `coordinates` field
	pub(crate) fn is_coordinates(&self) -> bool {
		self.0.as_str() == "coordinates"
	}
	/// Checks if this field is the `geometries` field
	pub(crate) fn is_geometries(&self) -> bool {
		self.0.as_str() == "geometries"
	}
}

impl Display for Ident {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&escape_ident(&self.0), f)
	}
}

impl InfoStructure for Ident {
	fn structure(self) -> Value {
		self.0.into()
	}
}
