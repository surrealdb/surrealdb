use crate::sql::{escape::escape_ident, fmt::Fmt, strand::no_nul_bytes, Id, Ident, Thing};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Table";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Tables(pub Vec<Table>);

impl From<Table> for Tables {
	fn from(v: Table) -> Self {
		Tables(vec![v])
	}
}

impl Deref for Tables {
	type Target = Vec<Table>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Tables {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, Ord)]
#[serde(rename = "$surrealdb::private::sql::Table")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Table(#[serde(with = "no_nul_bytes")] pub String);

impl From<String> for Table {
	fn from(v: String) -> Self {
		Self(v)
	}
}

impl From<&str> for Table {
	fn from(v: &str) -> Self {
		Self::from(String::from(v))
	}
}

impl From<Ident> for Table {
	fn from(v: Ident) -> Self {
		Self(v.0)
	}
}

impl Deref for Table {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Table {
	pub fn generate(&self) -> Thing {
		Thing {
			tb: self.0.to_owned(),
			id: Id::rand(),
		}
	}
}

impl Display for Table {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&escape_ident(&self.0), f)
	}
}
