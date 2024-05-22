use crate::sql::{escape::escape_ident, fmt::Fmt, strand::no_nul_bytes, Id, Ident, Thing};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Accesses(pub Vec<Access>);

impl From<Access> for Accesses {
	fn from(v: Access) -> Self {
		Accesses(vec![v])
	}
}

impl Deref for Accesses {
	type Target = Vec<Access>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Accesses {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Access")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Access(#[serde(with = "no_nul_bytes")] pub String);

impl From<String> for Access {
	fn from(v: String) -> Self {
		Self(v)
	}
}

impl From<&str> for Access {
	fn from(v: &str) -> Self {
		Self::from(String::from(v))
	}
}

impl From<Ident> for Access {
	fn from(v: Ident) -> Self {
		Self(v.0)
	}
}

impl Deref for Access {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Access {
	pub fn generate(&self) -> Thing {
		Thing {
			tb: self.0.to_owned(),
			id: Id::rand(),
		}
	}
}

impl Display for Access {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&escape_ident(&self.0), f)
	}
}
