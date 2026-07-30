//! `sql` -> `expr` conversions for [`crate::sql::statements::create`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::create::*;

impl From<CreateStatement> for crate::expr::statements::CreateStatement {
	fn from(v: CreateStatement) -> Self {
		crate::expr::statements::CreateStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
		}
	}
}

impl From<crate::expr::statements::CreateStatement> for CreateStatement {
	fn from(v: crate::expr::statements::CreateStatement) -> Self {
		CreateStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
		}
	}
}
