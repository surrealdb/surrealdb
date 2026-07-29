//! `sql` -> `expr` conversions for [`crate::sql::closure`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::closure::*;

impl From<Closure> for crate::expr::ClosureExpr {
	fn from(v: Closure) -> Self {
		Self {
			args: v.args.into_iter().map(|(i, k)| (i.into(), k.into())).collect(),
			returns: v.returns.map(Into::into),
			body: v.body.into(),
		}
	}
}

impl From<crate::expr::ClosureExpr> for Closure {
	fn from(v: crate::expr::ClosureExpr) -> Self {
		Self {
			args: v.args.into_iter().map(|(i, k)| (i.into(), k.into())).collect(),
			returns: v.returns.map(Into::into),
			body: v.body.into(),
		}
	}
}
