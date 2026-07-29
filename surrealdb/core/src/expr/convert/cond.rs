//! `sql` -> `expr` conversions for [`crate::sql::cond`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::cond::*;

impl From<Cond> for crate::expr::Cond {
	fn from(v: Cond) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Cond> for Cond {
	fn from(v: crate::expr::Cond) -> Self {
		Self(v.0.into())
	}
}
