//! `sql` -> `expr` conversions for [`crate::sql::group`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::group::*;

impl From<Groups> for crate::expr::Groups {
	fn from(v: Groups) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Groups> for Groups {
	fn from(v: crate::expr::Groups) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<Group> for crate::expr::Group {
	fn from(v: Group) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Group> for Group {
	fn from(v: crate::expr::Group) -> Self {
		Self(v.0.into())
	}
}
