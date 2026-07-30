//! `sql` -> `expr` conversions for [`crate::sql::mock`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::mock::*;

impl From<Mock> for crate::expr::Mock {
	fn from(v: Mock) -> Self {
		match v {
			Mock::Count(tb, c) => crate::expr::Mock::Count(tb.into(), c),
			Mock::Range(tb, r) => crate::expr::Mock::Range(tb.into(), r),
		}
	}
}

impl From<crate::expr::Mock> for Mock {
	fn from(v: crate::expr::Mock) -> Self {
		match v {
			crate::expr::Mock::Count(tb, c) => Mock::Count(tb.into(), c),
			crate::expr::Mock::Range(tb, r) => Mock::Range(tb.into(), r),
		}
	}
}
