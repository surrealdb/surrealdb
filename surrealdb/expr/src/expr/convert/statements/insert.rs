//! `sql` -> `expr` conversions for [`crate::sql::statements::insert`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::insert::*;

impl From<InsertStatement> for crate::expr::statements::InsertStatement {
	fn from(v: InsertStatement) -> Self {
		crate::expr::statements::InsertStatement {
			into: v.into.map(Into::into),
			data: v.data.into(),
			ignore: v.ignore,
			update: v.update.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
			relation: v.relation,
		}
	}
}

impl From<crate::expr::statements::InsertStatement> for InsertStatement {
	fn from(v: crate::expr::statements::InsertStatement) -> Self {
		InsertStatement {
			into: v.into.map(Into::into),
			data: v.data.into(),
			ignore: v.ignore,
			update: v.update.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
			relation: v.relation,
		}
	}
}
