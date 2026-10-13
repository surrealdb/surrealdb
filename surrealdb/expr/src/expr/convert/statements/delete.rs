//! `sql` -> `expr` conversions for [`crate::sql::statements::delete`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::delete::*;

impl From<DeleteStatement> for crate::expr::statements::DeleteStatement {
	fn from(v: DeleteStatement) -> Self {
		crate::expr::statements::DeleteStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			with: v.with.map(Into::into),
			cond: v.cond.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
			explain: v.explain.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::DeleteStatement> for DeleteStatement {
	fn from(v: crate::expr::statements::DeleteStatement) -> Self {
		DeleteStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			with: v.with.map(Into::into),
			cond: v.cond.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
			explain: v.explain.map(Into::into),
		}
	}
}
