use crate::sql::{escape::EscapeIdent, strand::no_nul_bytes};
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

impl From<Ident> for crate::expr::Ident {
	fn from(v: Ident) -> Self {
		Self(v.0)
	}
}

impl From<crate::expr::Ident> for Ident {
	fn from(v: crate::expr::Ident) -> Self {
		Self(v.0)
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
}

impl Display for Ident {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		EscapeIdent(&self.0).fmt(f)
	}
}
