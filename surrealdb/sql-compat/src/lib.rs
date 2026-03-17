pub mod capabilities;
pub mod error;
pub mod postgres;

use surrealdb_core::expr::plan::LogicalPlan;

use crate::capabilities::DialectCapabilities;
use crate::error::TranslateError;

/// Translates a SQL string in a specific dialect into a SurrealDB LogicalPlan.
pub trait DialectTranslator {
	/// The dialect identifier (e.g. "postgres", "mysql").
	fn dialect_name(&self) -> &'static str;

	/// Parse and translate a SQL string into a SurrealDB LogicalPlan.
	fn translate(&self, sql: &str) -> Result<LogicalPlan, TranslateError>;

	/// Report which SQL features this dialect supports.
	fn capabilities(&self) -> DialectCapabilities;
}
