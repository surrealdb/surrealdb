//! `sql` -> `expr` conversions for [`crate::sql::ast`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::ast::*;

impl From<ExplainFormat> for crate::expr::ExplainFormat {
	fn from(value: ExplainFormat) -> Self {
		match value {
			ExplainFormat::Text => crate::expr::ExplainFormat::Text,
			ExplainFormat::Json => crate::expr::ExplainFormat::Json,
		}
	}
}

impl From<crate::expr::ExplainFormat> for ExplainFormat {
	fn from(value: crate::expr::ExplainFormat) -> Self {
		match value {
			crate::expr::ExplainFormat::Text => ExplainFormat::Text,
			crate::expr::ExplainFormat::Json => ExplainFormat::Json,
		}
	}
}
