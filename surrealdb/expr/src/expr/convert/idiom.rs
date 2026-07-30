//! `sql` -> `expr` conversions for [`crate::sql::idiom`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::idiom::*;

impl From<Idiom> for crate::expr::Idiom {
	fn from(v: Idiom) -> Self {
		crate::expr::Idiom(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Idiom> for Idiom {
	fn from(v: crate::expr::Idiom) -> Self {
		Idiom(v.0.into_iter().map(Into::into).collect())
	}
}
