//! `sql` -> `expr` conversions for [`crate::sql::statements::relate`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::relate::*;

impl From<RelateStatement> for crate::expr::statements::RelateStatement {
	fn from(v: RelateStatement) -> Self {
		crate::expr::statements::RelateStatement {
			only: v.only,
			or_update: v.or_update,
			through: v.through.into(),
			from: v.from.into(),
			to: v.to.into(),
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
		}
	}
}

impl From<crate::expr::statements::RelateStatement> for RelateStatement {
	fn from(v: crate::expr::statements::RelateStatement) -> Self {
		RelateStatement {
			only: v.only,
			or_update: v.or_update,
			through: v.through.into(),
			from: v.from.into(),
			to: v.to.into(),
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
		}
	}
}
