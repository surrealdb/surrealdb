use crate::sql::statements::info::InfoStructure;
use crate::sql::value::Value;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Cond(pub Value);

impl Deref for Cond {
	type Target = Value;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<Cond> for crate::expr::Cond {
	fn from(v: Cond) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Cond> for Cond {
	fn from(v: crate::expr::Cond) -> Self {
		Self(v.0.into())
	}
}

crate::sql::impl_display_from_sql!(Cond);

impl crate::sql::DisplaySql for Cond {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "WHERE {}", self.0)
	}
}

impl InfoStructure for Cond {
	fn structure(self) -> Value {
		self.0.structure()
	}
}
