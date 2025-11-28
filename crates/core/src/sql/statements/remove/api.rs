use std::fmt::{self, Display, Formatter};

use crate::fmt::CoverStmts;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RemoveApiStatement {
	pub name: Expr,
	pub if_exists: bool,
}

impl Default for RemoveApiStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl Display for RemoveApiStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE API")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", CoverStmts(&self.name))?;
		Ok(())
	}
}

impl From<RemoveApiStatement> for crate::expr::statements::remove::RemoveApiStatement {
	fn from(v: RemoveApiStatement) -> Self {
		crate::expr::statements::remove::RemoveApiStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::remove::RemoveApiStatement> for RemoveApiStatement {
	fn from(v: crate::expr::statements::remove::RemoveApiStatement) -> Self {
		RemoveApiStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
