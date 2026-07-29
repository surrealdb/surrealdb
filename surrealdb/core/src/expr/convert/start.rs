//! `sql` -> `expr` conversions for [`crate::sql::start`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::start::*;

impl From<Start> for crate::expr::Start {
	fn from(value: Start) -> Self {
		crate::expr::Start(value.0.into())
	}
}

impl From<crate::expr::Start> for Start {
	fn from(value: crate::expr::Start) -> Self {
		Start(value.0.into())
	}
}
