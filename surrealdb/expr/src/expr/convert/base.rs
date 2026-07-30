//! `sql` -> `expr` conversions for [`crate::sql::base`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::base::*;

impl From<Base> for crate::expr::Base {
	fn from(v: Base) -> Self {
		match v {
			Base::Root => Self::Root,
			Base::Ns => Self::Ns,
			Base::Db => Self::Db,
		}
	}
}

impl From<crate::expr::Base> for Base {
	fn from(v: crate::expr::Base) -> Self {
		match v {
			crate::expr::Base::Root => Self::Root,
			crate::expr::Base::Ns => Self::Ns,
			crate::expr::Base::Db => Self::Db,
		}
	}
}
