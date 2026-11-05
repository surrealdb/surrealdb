//! Translation layer converting MySQL AST to SurrealQL.

pub mod context;
pub mod error;
pub mod expr;
pub mod show;
pub mod stmt;

pub use context::{ParamStyle, TranslationContext};
pub use error::TranslationError;
pub use expr::translate_expr;
pub use show::{ShowColumn, ShowColumnKind, show_columns};
pub use stmt::{translate_select, translate_statement};

use crate::ast::stmt::Statement;
use crate::parser::parse_statements;

/// Trait for types that can be translated to SurrealQL.
pub trait ToSurrealQl {
	fn to_surrealql(&self, ctx: &TranslationContext) -> Result<String, TranslationError>;
}

impl ToSurrealQl for Statement {
	fn to_surrealql(&self, ctx: &TranslationContext) -> Result<String, TranslationError> {
		let mut out = String::new();
		translate_statement(self, ctx, &mut out)?;
		Ok(out)
	}
}

/// Translate a MySQL SQL string directly into a SurrealQL query string using default settings.
pub fn translate_sql(sql: &str) -> Result<String, TranslationError> {
	translate_sql_with_context(sql, &TranslationContext::default())
}

/// Translate a MySQL SQL string directly into a SurrealQL query string using custom context.
pub fn translate_sql_with_context(
	sql: &str,
	ctx: &TranslationContext,
) -> Result<String, TranslationError> {
	let stmts = parse_statements(sql).map_err(|e| TranslationError::Invalid(e.to_string()))?;
	let mut out = String::new();
	for (i, stmt) in stmts.iter().enumerate() {
		if i > 0 {
			out.push('\n');
		}
		translate_statement(stmt, ctx, &mut out)?;
	}
	Ok(out)
}
