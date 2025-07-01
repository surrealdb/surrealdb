use crate::expr::statements::info::InfoStructure;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

use super::Expr;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Cond(pub Expr);

impl fmt::Display for Cond {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "WHERE {}", self.0)
	}
}

/*
impl InfoStructure for Cond {
	fn structure(self) -> Value {
		self.0.structure()
	}
}
*/
