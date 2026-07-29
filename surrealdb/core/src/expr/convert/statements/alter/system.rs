//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::system`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::system::*;

impl From<AlterSystemStatement> for crate::expr::statements::alter::AlterSystemStatement {
	fn from(v: AlterSystemStatement) -> Self {
		crate::expr::statements::alter::AlterSystemStatement {
			query_timeout: v.query_timeout.into(),
			compact: v.compact,
		}
	}
}

impl From<crate::expr::statements::alter::AlterSystemStatement> for AlterSystemStatement {
	fn from(v: crate::expr::statements::alter::AlterSystemStatement) -> Self {
		AlterSystemStatement {
			query_timeout: v.query_timeout.into(),
			compact: v.compact,
		}
	}
}
