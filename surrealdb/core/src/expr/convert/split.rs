//! `sql` -> `expr` conversions for [`crate::sql::split`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::split::*;

impl From<Splits> for crate::expr::Splits {
	fn from(v: Splits) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Splits> for Splits {
	fn from(v: crate::expr::Splits) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<Split> for crate::expr::Split {
	fn from(v: Split) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Split> for Split {
	fn from(v: crate::expr::Split) -> Self {
		Self(v.0.into())
	}
}
