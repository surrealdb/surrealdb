//! `sql` -> `expr` conversions for [`crate::sql::limit`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::limit::*;

impl From<Limit> for crate::expr::Limit {
	fn from(value: Limit) -> Self {
		Self(value.0.into())
	}
}

impl From<crate::expr::Limit> for Limit {
	fn from(value: crate::expr::Limit) -> Self {
		Limit(value.0.into())
	}
}
