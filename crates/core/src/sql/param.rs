use crate::sql::ident::Ident;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::{fmt, ops::Deref, str};

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Param";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Param")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Param(pub Ident);

impl From<Ident> for Param {
	fn from(v: Ident) -> Self {
		Self(v)
	}
}

impl From<String> for Param {
	fn from(v: String) -> Self {
		Self(v.into())
	}
}

impl From<&str> for Param {
	fn from(v: &str) -> Self {
		Self(v.into())
	}
}

impl Deref for Param {
	type Target = Ident;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for Param {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "${}", &self.0.0)
	}
}

impl From<Param> for crate::expr::Param {
	fn from(v: Param) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Param> for Param {
	fn from(v: crate::expr::Param) -> Self {
		Self(v.0.into())
	}
}
