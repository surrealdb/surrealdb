//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::analyzer`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::AlterKind;
use crate::sql::statements::alter::analyzer::*;

impl From<AlterAnalyzerStatement> for crate::expr::statements::alter::AlterAnalyzerStatement {
	fn from(v: AlterAnalyzerStatement) -> Self {
		crate::expr::statements::alter::AlterAnalyzerStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			function: v.function.into(),
			tokenizers: match v.tokenizers {
				AlterKind::Set(x) => crate::expr::statements::alter::AlterKind::Set(
					x.into_iter().map(Into::into).collect(),
				),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Drop,
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			filters: match v.filters {
				AlterKind::Set(x) => crate::expr::statements::alter::AlterKind::Set(
					x.into_iter().map(Into::into).collect(),
				),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Drop,
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterAnalyzerStatement> for AlterAnalyzerStatement {
	fn from(v: crate::expr::statements::alter::AlterAnalyzerStatement) -> Self {
		AlterAnalyzerStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			function: v.function.into(),
			tokenizers: match v.tokenizers {
				crate::expr::statements::alter::AlterKind::Set(x) => {
					AlterKind::Set(x.into_iter().map(Into::into).collect())
				}
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			filters: match v.filters {
				crate::expr::statements::alter::AlterKind::Set(x) => {
					AlterKind::Set(x.into_iter().map(Into::into).collect())
				}
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			comment: v.comment.into(),
		}
	}
}
