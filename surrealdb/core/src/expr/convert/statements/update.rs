//! `sql` -> `expr` conversions for [`crate::sql::statements::update`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::update::*;

impl From<UpdateStatement> for crate::expr::statements::UpdateStatement {
	fn from(v: UpdateStatement) -> Self {
		crate::expr::statements::UpdateStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			with: v.with.map(Into::into),
			data: v.data.map(Into::into),
			cond: v.cond.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
			explain: v.explain.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::UpdateStatement> for UpdateStatement {
	fn from(v: crate::expr::statements::UpdateStatement) -> Self {
		UpdateStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			with: v.with.map(Into::into),
			data: v.data.map(Into::into),
			cond: v.cond.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
			explain: v.explain.map(Into::into),
		}
	}
}
