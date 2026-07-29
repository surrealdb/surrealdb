//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::database`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::database::*;

impl From<AlterDatabaseStatement> for crate::expr::statements::alter::AlterDatabaseStatement {
	fn from(v: AlterDatabaseStatement) -> Self {
		crate::expr::statements::alter::AlterDatabaseStatement {
			compact: v.compact,
		}
	}
}

impl From<crate::expr::statements::alter::AlterDatabaseStatement> for AlterDatabaseStatement {
	fn from(v: crate::expr::statements::alter::AlterDatabaseStatement) -> Self {
		AlterDatabaseStatement {
			compact: v.compact,
		}
	}
}
