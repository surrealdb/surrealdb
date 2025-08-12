use std::fmt;

use crate::sql::Expr;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Cond(pub Expr);

impl fmt::Display for Cond {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "WHERE {}", self.0)
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
