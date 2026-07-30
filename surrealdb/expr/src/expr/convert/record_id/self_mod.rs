//! `sql` -> `expr` conversions declared directly in [`crate::sql::record_id`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::record_id::*;

impl From<RecordIdLit> for crate::expr::RecordIdLit {
	fn from(v: RecordIdLit) -> Self {
		crate::expr::RecordIdLit {
			table: v.table.into(),
			key: v.key.into(),
		}
	}
}

impl From<crate::expr::RecordIdLit> for RecordIdLit {
	fn from(v: crate::expr::RecordIdLit) -> Self {
		RecordIdLit {
			table: v.table.into(),
			key: v.key.into(),
		}
	}
}
