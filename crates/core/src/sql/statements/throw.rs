use crate::sql::Expr;
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ThrowStatement {
	pub error: Expr,
}

impl fmt::Display for ThrowStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "THROW {}", self.error)
	}
}

impl From<ThrowStatement> for crate::expr::statements::ThrowStatement {
	fn from(v: ThrowStatement) -> Self {
		Self {
			error: v.error.into(),
		}
	}
}

impl From<crate::expr::statements::ThrowStatement> for ThrowStatement {
	fn from(v: crate::expr::statements::ThrowStatement) -> Self {
		Self {
			error: v.error.into(),
		}
	}
}
