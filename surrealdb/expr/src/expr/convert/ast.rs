//! `sql` -> `expr` conversions for [`crate::sql::ast`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::expr;
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

impl From<expr::LogicalPlan> for Ast {
	fn from(value: expr::LogicalPlan) -> Self {
		Ast {
			expressions: value.expressions.into_iter().map(From::from).collect(),
		}
	}
}

impl From<Ast> for expr::LogicalPlan {
	fn from(value: Ast) -> Self {
		expr::LogicalPlan {
			expressions: value.expressions.into_iter().map(From::from).collect(),
		}
	}
}

impl From<TopLevelExpr> for crate::expr::TopLevelExpr {
	fn from(value: TopLevelExpr) -> Self {
		match value {
			TopLevelExpr::Begin => crate::expr::TopLevelExpr::Begin,
			TopLevelExpr::Cancel => crate::expr::TopLevelExpr::Cancel,
			TopLevelExpr::Commit => crate::expr::TopLevelExpr::Commit,
			TopLevelExpr::Access(access_statement) => {
				crate::expr::TopLevelExpr::Access(Box::new((*access_statement).into()))
			}
			TopLevelExpr::Kill(kill_statement) => {
				crate::expr::TopLevelExpr::Kill(kill_statement.into())
			}
			TopLevelExpr::Live(live_statement) => {
				crate::expr::TopLevelExpr::Live(Box::new((*live_statement).into()))
			}
			TopLevelExpr::Option(option_statement) => {
				crate::expr::TopLevelExpr::Option(option_statement.into())
			}
			TopLevelExpr::Use(use_statement) => {
				crate::expr::TopLevelExpr::Use(use_statement.into())
			}
			TopLevelExpr::Show(show_statement) => {
				crate::expr::TopLevelExpr::Show(show_statement.into())
			}
			TopLevelExpr::Expr(expr) => crate::expr::TopLevelExpr::Expr(expr.into()),
		}
	}
}

impl From<crate::expr::TopLevelExpr> for TopLevelExpr {
	fn from(value: crate::expr::TopLevelExpr) -> Self {
		match value {
			crate::expr::TopLevelExpr::Begin => TopLevelExpr::Begin,
			crate::expr::TopLevelExpr::Cancel => TopLevelExpr::Cancel,
			crate::expr::TopLevelExpr::Commit => TopLevelExpr::Commit,
			crate::expr::TopLevelExpr::Access(access_statement) => {
				TopLevelExpr::Access(Box::new((*access_statement).into()))
			}
			crate::expr::TopLevelExpr::Kill(kill_statement) => {
				TopLevelExpr::Kill(kill_statement.into())
			}
			crate::expr::TopLevelExpr::Live(live_statement) => {
				TopLevelExpr::Live(Box::new((*live_statement).into()))
			}
			crate::expr::TopLevelExpr::Option(option_statement) => {
				TopLevelExpr::Option(option_statement.into())
			}
			crate::expr::TopLevelExpr::Use(use_statement) => {
				TopLevelExpr::Use(use_statement.into())
			}
			crate::expr::TopLevelExpr::Show(show_statement) => {
				TopLevelExpr::Show(show_statement.into())
			}
			crate::expr::TopLevelExpr::Expr(expr) => TopLevelExpr::Expr(expr.into()),
		}
	}
}
