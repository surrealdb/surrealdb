//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::namespace`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::namespace::*;

impl From<AlterNamespaceStatement> for crate::expr::statements::alter::AlterNamespaceStatement {
	fn from(v: AlterNamespaceStatement) -> Self {
		crate::expr::statements::alter::AlterNamespaceStatement {
			compact: v.compact,
		}
	}
}

impl From<crate::expr::statements::alter::AlterNamespaceStatement> for AlterNamespaceStatement {
	fn from(v: crate::expr::statements::alter::AlterNamespaceStatement) -> Self {
		AlterNamespaceStatement {
			compact: v.compact,
		}
	}
}
