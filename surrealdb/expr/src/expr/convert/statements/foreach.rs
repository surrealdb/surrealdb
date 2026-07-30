//! `sql` -> `expr` conversions for [`crate::sql::statements::foreach`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::foreach::*;

impl From<ForeachStatement> for crate::expr::statements::ForeachStatement {
	fn from(v: ForeachStatement) -> Self {
		Self {
			param: v.param.into(),
			range: v.range.into(),
			block: v.block.into(),
		}
	}
}

impl From<crate::expr::statements::ForeachStatement> for ForeachStatement {
	fn from(v: crate::expr::statements::ForeachStatement) -> Self {
		Self {
			param: v.param.into(),
			range: v.range.into(),
			block: v.block.into(),
		}
	}
}
