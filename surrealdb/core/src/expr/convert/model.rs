//! `sql` -> `expr` conversions for [`crate::sql::model`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::model::*;

impl From<Model> for crate::expr::Model {
	fn from(v: Model) -> Self {
		Self {
			name: v.name,
			version: v.version,
		}
	}
}

impl From<crate::expr::Model> for Model {
	fn from(v: crate::expr::Model) -> Self {
		Self {
			name: v.name,
			version: v.version,
		}
	}
}
