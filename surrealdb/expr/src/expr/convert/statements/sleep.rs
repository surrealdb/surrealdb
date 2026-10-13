//! `sql` -> `expr` conversions for [`crate::sql::statements::sleep`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::sleep::*;

impl From<SleepStatement> for crate::expr::statements::SleepStatement {
	fn from(v: SleepStatement) -> Self {
		crate::expr::statements::SleepStatement {
			duration: v.duration.into(),
		}
	}
}

impl From<crate::expr::statements::SleepStatement> for SleepStatement {
	fn from(v: crate::expr::statements::SleepStatement) -> Self {
		SleepStatement {
			duration: v.duration.into(),
		}
	}
}
