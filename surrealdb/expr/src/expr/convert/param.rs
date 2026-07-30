//! `sql` -> `expr` conversions for [`crate::sql::param`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::param::*;

impl From<Param> for crate::expr::Param {
	fn from(v: Param) -> Self {
		crate::expr::Param::from(v.into_strand())
	}
}

impl From<crate::expr::Param> for Param {
	fn from(v: crate::expr::Param) -> Self {
		Param::new(v.into_strand())
	}
}
