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

impl fmt::Display for Cond {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "WHERE {}", self.0)
	}
}
impl InfoStructure for Cond {
	fn structure(self) -> Value {
		self.0.structure()
	}
}
