//! `sql` -> `expr` conversions for [`crate::sql::statements::remove::field`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::remove::field::*;

impl From<RemoveFieldStatement> for crate::expr::statements::RemoveFieldStatement {
	fn from(v: RemoveFieldStatement) -> Self {
		crate::expr::statements::RemoveFieldStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			table_name: v.what.into(),
		}
	}
}

impl From<crate::expr::statements::RemoveFieldStatement> for RemoveFieldStatement {
	fn from(v: crate::expr::statements::RemoveFieldStatement) -> Self {
		RemoveFieldStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			what: v.table_name.into(),
		}
	}
}
