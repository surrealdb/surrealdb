//! `sql` -> `expr` conversions for [`crate::sql::statements::define::analyzer`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::analyzer::*;

impl From<DefineAnalyzerStatement> for crate::expr::statements::DefineAnalyzerStatement {
	fn from(v: DefineAnalyzerStatement) -> Self {
		crate::expr::statements::DefineAnalyzerStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			function: v.function,
			tokenizers: v.tokenizers.map(|v| v.into_iter().map(Into::into).collect()),
			filters: v.filters.map(|v| v.into_iter().map(Into::into).collect()),
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::DefineAnalyzerStatement> for DefineAnalyzerStatement {
	fn from(v: crate::expr::statements::DefineAnalyzerStatement) -> Self {
		DefineAnalyzerStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			function: v.function,
			tokenizers: v.tokenizers.map(|v| v.into_iter().map(Into::into).collect()),
			filters: v.filters.map(|v| v.into_iter().map(Into::into).collect()),
			comment: v.comment.into(),
		}
	}
}
