//! `sql` -> `expr` conversions for [`crate::sql::view`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::view::*;

impl From<View> for crate::expr::View {
	fn from(v: View) -> Self {
		crate::expr::View {
			expr: v.expr.into(),
			what: v.what.into_iter().map(Into::into).collect(),
			cond: v.cond.map(Into::into),
			group: v.group.map(Into::into),
		}
	}
}

impl From<crate::expr::View> for View {
	fn from(v: crate::expr::View) -> Self {
		View {
			expr: v.expr.into(),
			what: v.what.into_iter().map(Into::into).collect(),
			cond: v.cond.map(Into::into),
			group: v.group.map(Into::into),
		}
	}
}
