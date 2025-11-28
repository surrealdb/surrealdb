use std::fmt::{self, Display, Formatter};

use crate::fmt::CoverStmts;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RemoveIndexStatement {
	pub name: Expr,
	pub what: Expr,
	pub if_exists: bool,
}

impl Default for RemoveIndexStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl Display for RemoveIndexStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE INDEX")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", CoverStmts(&self.name), CoverStmts(&self.what))?;
		Ok(())
	}
}

impl From<RemoveIndexStatement> for crate::expr::statements::RemoveIndexStatement {
	fn from(v: RemoveIndexStatement) -> Self {
		crate::expr::statements::RemoveIndexStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			what: v.what.into(),
		}
	}
}

impl From<crate::expr::statements::RemoveIndexStatement> for RemoveIndexStatement {
	fn from(v: crate::expr::statements::RemoveIndexStatement) -> Self {
		RemoveIndexStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			what: v.what.into(),
		}
	}
}
