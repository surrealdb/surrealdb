use std::fmt;

use crate::sql::Expr;

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct KillStatement {
	// Uuid of Live Query
	// or Param resolving to Uuid of Live Query
	pub id: Expr,
}

impl fmt::Display for KillStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "KILL {}", self.id)
	}
}

impl From<KillStatement> for crate::expr::statements::KillStatement {
	fn from(v: KillStatement) -> Self {
		Self {
			id: v.id.into(),
		}
	}
}

impl From<crate::expr::statements::KillStatement> for KillStatement {
	fn from(v: crate::expr::statements::KillStatement) -> Self {
		Self {
			id: v.id.into(),
		}
	}
}
