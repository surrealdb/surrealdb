//! `sql` -> `expr` conversions for [`crate::sql::statements::define::index`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::index::*;

impl From<DefineIndexStatement> for crate::expr::statements::DefineIndexStatement {
	fn from(v: DefineIndexStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			what: v.what.into(),
			cols: v.cols.into_iter().map(From::from).collect(),
			index: v.index.into(),
			comment: v.comment.into(),
			concurrently: v.concurrently,
		}
	}
}

impl From<crate::expr::statements::DefineIndexStatement> for DefineIndexStatement {
	fn from(v: crate::expr::statements::DefineIndexStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			what: v.what.into(),
			cols: v.cols.into_iter().map(From::from).collect(),
			index: v.index.into(),
			comment: v.comment.into(),
			concurrently: v.concurrently,
		}
	}
}
