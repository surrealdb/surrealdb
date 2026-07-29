//! `sql` -> `expr` conversions for [`crate::sql::fetch`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::fetch::*;

impl From<Fetchs> for crate::expr::Fetchs {
	fn from(v: Fetchs) -> Self {
		Self::new(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Fetchs> for Fetchs {
	fn from(v: crate::expr::Fetchs) -> Self {
		Self(v.into_iter().map(Into::into).collect())
	}
}

impl From<Fetch> for crate::expr::Fetch {
	fn from(v: Fetch) -> Self {
		crate::expr::Fetch(v.0.into())
	}
}

impl From<crate::expr::Fetch> for Fetch {
	fn from(v: crate::expr::Fetch) -> Self {
		Fetch(v.0.into())
	}
}
