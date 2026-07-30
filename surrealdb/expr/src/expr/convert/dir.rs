//! `sql` -> `expr` conversions for [`crate::sql::dir`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::dir::*;

impl From<Dir> for crate::expr::Dir {
	fn from(v: Dir) -> Self {
		match v {
			Dir::In => Self::In,
			Dir::Out => Self::Out,
			Dir::Both => Self::Both,
		}
	}
}

impl From<crate::expr::Dir> for Dir {
	fn from(v: crate::expr::Dir) -> Self {
		match v {
			crate::expr::Dir::In => Self::In,
			crate::expr::Dir::Out => Self::Out,
			crate::expr::Dir::Both => Self::Both,
		}
	}
}
