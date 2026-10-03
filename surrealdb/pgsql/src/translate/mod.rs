//! Translation / lowering layer ($\tau$) from PostgreSQL AST to SurrealQL.
//!
//! Provides a lean, zero-overhead translator converting PostgreSQL statements
//! and expressions into canonical SurrealQL queries suitable for execution by SurrealDB.

pub mod context;
pub mod error;
pub mod expr;
pub mod stmt;

pub use context::{ParamStyle, TranslationContext};
pub use error::TranslationError;
pub use expr::translate_expr;
pub use stmt::{ToSurrealQl, translate_statement};

use crate::parser::Parser;

/// Translate a PostgreSQL SQL string directly into a SurrealQL query string using default settings.
pub fn translate_sql(sql: &str) -> Result<String, TranslationError> {
	translate_sql_with_context(sql, &TranslationContext::default())
}

/// Translate a PostgreSQL SQL string directly into a SurrealQL query string using custom context.
pub fn translate_sql_with_context(
	sql: &str,
	ctx: &TranslationContext,
) -> Result<String, TranslationError> {
	let stmt = Parser::new(sql)
		.map_err(|e| TranslationError::Invalid(e.to_string()))?
		.parse_statement()
		.map_err(|e| TranslationError::Invalid(e.to_string()))?;
	stmt.to_surrealql(ctx)
}
