//! SQL:2023 / SQL:2016 SQL/JSON AST representations.

use crate::ast::expr::{Expr, TypeName};
use crate::ast::stmt::TableAlias;
use crate::span::Span;

/// `JSON_TABLE` table function in `FROM` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonTable {
	/// Context JSON expression.
	pub context_item: Box<Expr>,
	/// JSON path expression string.
	pub path: String,
	/// Column definitions extracted from JSON.
	pub columns: Vec<JsonTableColumn>,
	/// Optional table alias.
	pub alias: Option<TableAlias>,
	/// Source span.
	pub span: Span,
}

impl JsonTable {
	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		self.context_item.normalize_spans();
		for col in &mut self.columns {
			col.normalize_spans();
		}
		if let Some(a) = &mut self.alias {
			a.span = zero;
		}
	}
}

/// Column definition inside `JSON_TABLE (...) COLUMNS (...)`.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonTableColumn {
	/// Column name.
	pub name: String,
	/// Target SQL data type.
	pub data_type: TypeName,
	/// Relative JSON path expression for this column.
	pub path: Option<String>,
	/// Source span.
	pub span: Span,
}

impl JsonTableColumn {
	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		self.data_type.span = zero;
		self.data_type.name.span = zero;
	}
}
