//! `sql` -> `expr` conversions for [`crate::sql::record_id::range`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::record_id::range::*;

impl From<RecordIdKeyRangeLit> for crate::expr::RecordIdKeyRangeLit {
	fn from(value: RecordIdKeyRangeLit) -> Self {
		crate::expr::RecordIdKeyRangeLit {
			start: value.start.map(|x| x.into()),
			end: value.end.map(|x| x.into()),
		}
	}
}

impl From<crate::expr::RecordIdKeyRangeLit> for RecordIdKeyRangeLit {
	fn from(value: crate::expr::RecordIdKeyRangeLit) -> Self {
		RecordIdKeyRangeLit {
			start: value.start.map(|x| x.into()),
			end: value.end.map(|x| x.into()),
		}
	}
}
