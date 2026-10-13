//! `sql` -> `expr` conversions for [`crate::sql::with`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::with::*;

impl From<With> for crate::expr::With {
	fn from(v: With) -> Self {
		match v {
			With::NoIndex => Self::NoIndex,
			With::Index(i) => Self::Index(i),
		}
	}
}

impl From<crate::expr::With> for With {
	fn from(v: crate::expr::With) -> Self {
		match v {
			crate::expr::With::NoIndex => Self::NoIndex,
			crate::expr::With::Index(i) => Self::Index(i),
		}
	}
}
