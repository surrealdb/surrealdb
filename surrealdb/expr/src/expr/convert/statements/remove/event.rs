//! `sql` -> `expr` conversions for [`crate::sql::statements::remove::event`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::remove::event::*;

impl From<RemoveEventStatement> for crate::expr::statements::RemoveEventStatement {
	fn from(v: RemoveEventStatement) -> Self {
		crate::expr::statements::RemoveEventStatement {
			name: v.name.into(),
			table_name: v.what.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveEventStatement> for RemoveEventStatement {
	fn from(v: crate::expr::statements::RemoveEventStatement) -> Self {
		RemoveEventStatement {
			name: v.name.into(),
			what: v.table_name.into(),
			if_exists: v.if_exists,
		}
	}
}
