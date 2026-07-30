//! `sql` -> `expr` conversions for [`crate::sql::statements::remove::analyzer`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::remove::analyzer::*;

impl From<RemoveAnalyzerStatement> for crate::expr::statements::RemoveAnalyzerStatement {
	fn from(v: RemoveAnalyzerStatement) -> Self {
		crate::expr::statements::RemoveAnalyzerStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveAnalyzerStatement> for RemoveAnalyzerStatement {
	fn from(v: crate::expr::statements::RemoveAnalyzerStatement) -> Self {
		RemoveAnalyzerStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
