//! `sql` -> `expr` conversions for [`crate::sql::statements::live`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use uuid::Uuid;

use crate::sql::statements::live::*;

impl From<LiveFields> for crate::expr::statements::LiveFields {
	fn from(v: LiveFields) -> Self {
		match v {
			LiveFields::Diff => crate::expr::statements::LiveFields::Diff,
			LiveFields::Select(fields) => {
				crate::expr::statements::LiveFields::Select(fields.into())
			}
		}
	}
}

impl From<crate::expr::statements::LiveFields> for LiveFields {
	fn from(v: crate::expr::statements::LiveFields) -> Self {
		match v {
			crate::expr::statements::LiveFields::Diff => LiveFields::Diff,
			crate::expr::statements::LiveFields::Select(fields) => {
				LiveFields::Select(fields.into())
			}
		}
	}
}

impl From<LiveStatement> for crate::expr::statements::LiveStatement {
	fn from(v: LiveStatement) -> Self {
		crate::expr::statements::LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::new_v4(),
			fields: v.fields.into(),
			what: v.what.into(),
			cond: v.cond.map(Into::into),
			fetch: v.fetch.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::LiveStatement> for LiveStatement {
	fn from(v: crate::expr::statements::LiveStatement) -> Self {
		LiveStatement {
			fields: v.fields.into(),
			what: v.what.into(),
			cond: v.cond.map(Into::into),
			fetch: v.fetch.map(Into::into),
		}
	}
}
