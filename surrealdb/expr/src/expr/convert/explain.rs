//! `sql` -> `expr` conversions for [`crate::sql::explain`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::explain::*;

impl From<Explain> for crate::expr::Explain {
	fn from(v: Explain) -> Self {
		Self(v.0)
	}
}

impl From<crate::expr::Explain> for Explain {
	fn from(v: crate::expr::Explain) -> Self {
		Self(v.0)
	}
}
