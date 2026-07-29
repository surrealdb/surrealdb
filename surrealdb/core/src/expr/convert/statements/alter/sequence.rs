//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::sequence`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::sequence::*;

impl From<AlterSequenceStatement> for crate::expr::statements::alter::AlterSequenceStatement {
	fn from(v: AlterSequenceStatement) -> Self {
		crate::expr::statements::alter::AlterSequenceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			timeout: v.timeout.map(From::from),
		}
	}
}

impl From<crate::expr::statements::alter::AlterSequenceStatement> for AlterSequenceStatement {
	fn from(v: crate::expr::statements::alter::AlterSequenceStatement) -> Self {
		AlterSequenceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			timeout: v.timeout.map(From::from),
		}
	}
}
