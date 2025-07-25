use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::Expr;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
