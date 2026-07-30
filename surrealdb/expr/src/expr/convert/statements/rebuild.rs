//! `sql` -> `expr` conversions for [`crate::sql::statements::rebuild`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::rebuild::*;

impl From<RebuildStatement> for crate::expr::statements::rebuild::RebuildStatement {
	fn from(v: RebuildStatement) -> Self {
		match v {
			RebuildStatement::Index(v) => Self::Index(v.into()),
		}
	}
}

impl From<crate::expr::statements::rebuild::RebuildStatement> for RebuildStatement {
	fn from(v: crate::expr::statements::rebuild::RebuildStatement) -> Self {
		match v {
			crate::expr::statements::rebuild::RebuildStatement::Index(v) => Self::Index(v.into()),
		}
	}
}

impl From<RebuildIndexStatement> for crate::expr::statements::rebuild::RebuildIndexStatement {
	fn from(v: RebuildIndexStatement) -> Self {
		Self {
			name: v.name,
			table: v.what.into(),
			if_exists: v.if_exists,
			concurrently: v.concurrently,
		}
	}
}

impl From<crate::expr::statements::rebuild::RebuildIndexStatement> for RebuildIndexStatement {
	fn from(v: crate::expr::statements::rebuild::RebuildIndexStatement) -> Self {
		Self {
			name: v.name,
			what: v.table.into(),
			if_exists: v.if_exists,
			concurrently: v.concurrently,
		}
	}
}
