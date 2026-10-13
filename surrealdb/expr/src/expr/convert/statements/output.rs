//! `sql` -> `expr` conversions for [`crate::sql::statements::output`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::output::*;

impl From<OutputStatement> for crate::expr::statements::OutputStatement {
	fn from(v: OutputStatement) -> Self {
		crate::expr::statements::OutputStatement {
			what: v.what.into(),
			fetch: v.fetch.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::OutputStatement> for OutputStatement {
	fn from(v: crate::expr::statements::OutputStatement) -> Self {
		OutputStatement {
			what: v.what.into(),
			fetch: v.fetch.map(Into::into),
		}
	}
}
