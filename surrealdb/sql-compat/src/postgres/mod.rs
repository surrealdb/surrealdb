mod expressions;
mod functions;
mod statements;
mod types;

use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use surrealdb_core::expr::plan::{LogicalPlan, TopLevelExpr};

use crate::DialectTranslator;
use crate::capabilities::DialectCapabilities;
use crate::error::TranslateError;

pub struct PostgresTranslator;

impl DialectTranslator for PostgresTranslator {
	fn dialect_name(&self) -> &'static str {
		"postgres"
	}

	fn translate(&self, sql: &str) -> Result<LogicalPlan, TranslateError> {
		let dialect = PostgreSqlDialect {};
		let ast = Parser::parse_sql(&dialect, sql)?;
		let expressions: Result<Vec<TopLevelExpr>, TranslateError> =
			ast.into_iter().map(translate_statement).collect();
		Ok(LogicalPlan {
			expressions: expressions?,
		})
	}

	fn capabilities(&self) -> DialectCapabilities {
		DialectCapabilities {
			supports_transactions: true,
			supports_prepared_statements: true,
			supports_cursors: false,
		}
	}
}

fn translate_statement(stmt: Statement) -> Result<TopLevelExpr, TranslateError> {
	statements::translate_statement(stmt)
}
