//! `sql` -> `expr` conversions for [`crate::sql::script`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::script::*;

impl From<Script> for crate::expr::Script {
	fn from(v: Script) -> Self {
		Self(v.0)
	}
}

impl From<crate::expr::Script> for Script {
	fn from(v: crate::expr::Script) -> Self {
		Self(v.0)
	}
}
