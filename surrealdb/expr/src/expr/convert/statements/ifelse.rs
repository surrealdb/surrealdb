//! `sql` -> `expr` conversions for [`crate::sql::statements::ifelse`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::ifelse::*;

impl From<IfelseStatement> for crate::expr::statements::IfelseStatement {
	fn from(v: IfelseStatement) -> Self {
		crate::expr::statements::IfelseStatement {
			exprs: v.exprs.into_iter().map(|(a, b)| (From::from(a), From::from(b))).collect(),
			close: v.close.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::IfelseStatement> for IfelseStatement {
	fn from(v: crate::expr::statements::IfelseStatement) -> Self {
		IfelseStatement {
			exprs: v.exprs.into_iter().map(|(a, b)| (From::from(a), From::from(b))).collect(),
			close: v.close.map(Into::into),
		}
	}
}
