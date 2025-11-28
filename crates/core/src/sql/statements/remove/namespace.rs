use std::fmt::{self, Display, Formatter};

use crate::fmt::CoverStmts;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RemoveNamespaceStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub expunge: bool,
}

impl Default for RemoveNamespaceStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			expunge: false,
		}
	}
}

impl Display for RemoveNamespaceStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE NAMESPACE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", CoverStmts(&self.name))?;
		Ok(())
	}
}

impl From<RemoveNamespaceStatement> for crate::expr::statements::RemoveNamespaceStatement {
	fn from(v: RemoveNamespaceStatement) -> Self {
		crate::expr::statements::RemoveNamespaceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}

impl From<crate::expr::statements::RemoveNamespaceStatement> for RemoveNamespaceStatement {
	fn from(v: crate::expr::statements::RemoveNamespaceStatement) -> Self {
		RemoveNamespaceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}
