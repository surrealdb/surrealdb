//! Expression AST representations for PostgreSQL.

use crate::ast::literal::Literal;
use crate::ast::op::{BinaryOperator, UnaryOperator};
use crate::span::Span;

/// A qualified identifier name (e.g. `col`, `tbl.col`, `schema.tbl.col`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedName {
	/// The path components of the qualified name.
	pub parts: Vec<String>,
	/// Source span.
	pub span: Span,
}

impl QualifiedName {
	/// Create a new qualified name from parts and span.
	pub const fn new(parts: Vec<String>, span: Span) -> Self {
		Self {
			parts,
			span,
		}
	}

	/// Create a single-identifier qualified name.
	pub fn single(name: String, span: Span) -> Self {
		Self {
			parts: vec![name],
			span,
		}
	}
}

/// A column reference in an SQL expression.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
	/// Name of column or qualified path (e.g. `[table, column]`).
	pub fields: Vec<String>,
	/// Source span.
	pub span: Span,
}

/// A PostgreSQL type name specification (e.g. `INTEGER`, `VARCHAR(255)`, `TEXT[]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeName {
	/// Qualified name of the type (e.g. `int4`, `pg_catalog.int4`).
	pub name: QualifiedName,
	/// Type modifiers (e.g. `(255)` in `VARCHAR(255)`).
	pub modifiers: Vec<String>,
	/// Number of array dimensions (e.g. 1 for `INT[]`, 2 for `INT[][]`).
	pub array_bounds: Vec<Option<i64>>,
	/// Span of the type specification.
	pub span: Span,
}

/// A `WHEN ... THEN ...` branch in a CASE expression.
#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
	/// Condition or comparison value.
	pub when: Expr,
	/// Result expression if condition matches.
	pub then: Expr,
	/// Span of the clause.
	pub span: Span,
}

/// An expression node in the PostgreSQL AST.
///
/// Implements `gram.y: a_expr`, `gram.y: b_expr`, and `gram.y: c_expr`.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
	/// A constant literal value.
	Literal {
		/// The literal value.
		lit: Literal,
		/// Source span.
		span: Span,
	},

	/// A column or table reference (e.g. `id`, `users.id`, `public.users.id`).
	ColumnRef(ColumnRef),

	/// Positional parameter (e.g. `$1`, `$2`).
	Param {
		/// Parameter index (1-based).
		id: u32,
		/// Source span.
		span: Span,
	},

	/// Unary prefix operator expression (e.g. `-x`, `NOT flag`, `~mask`).
	UnaryOp {
		/// Operator.
		op: UnaryOperator,
		/// Operand.
		expr: Box<Expr>,
		/// Source span.
		span: Span,
	},

	/// Binary operator expression (e.g. `a + b`, `x AND y`, `data -> 'field'`).
	BinaryOp {
		/// Left operand.
		left: Box<Expr>,
		/// Operator.
		op: BinaryOperator,
		/// Right operand.
		right: Box<Expr>,
		/// Source span.
		span: Span,
	},

	/// Type cast expression (e.g. `x::INT`, `CAST(x AS INT)`).
	TypeCast {
		/// Expression being cast.
		expr: Box<Expr>,
		/// Target type name.
		data_type: TypeName,
		/// Source span.
		span: Span,
	},

	/// Function or aggregate invocation (e.g. `COUNT(*)`, `SUBSTRING(s, 1, 3)`).
	FunctionCall {
		/// Function qualified name.
		name: QualifiedName,
		/// Argument list.
		args: Vec<Expr>,
		/// True if `DISTINCT` modifier was used.
		distinct: bool,
		/// Source span.
		span: Span,
	},

	/// Array subscripting (e.g. `arr[1]`).
	Subscript {
		/// Base array expression.
		expr: Box<Expr>,
		/// Index expression.
		index: Box<Expr>,
		/// Source span.
		span: Span,
	},

	/// Array slicing (e.g. `arr[1:5]`, `arr[:5]`, `arr[2:]`).
	Slice {
		/// Base array expression.
		expr: Box<Expr>,
		/// Lower slice bound (optional).
		lower: Option<Box<Expr>>,
		/// Upper slice bound (optional).
		upper: Option<Box<Expr>>,
		/// Source span.
		span: Span,
	},

	/// `BETWEEN` range check expression (e.g. `x BETWEEN 1 AND 10`, `x NOT BETWEEN SYMMETRIC a AND
	/// b`).
	Between {
		/// Target expression.
		expr: Box<Expr>,
		/// Lower bound.
		low: Box<Expr>,
		/// Upper bound.
		high: Box<Expr>,
		/// True if `NOT BETWEEN`.
		negated: bool,
		/// True if `BETWEEN SYMMETRIC`.
		symmetric: bool,
		/// Source span.
		span: Span,
	},

	/// `IN (...)` membership test with explicit list (e.g. `x IN (1, 2, 3)`).
	InList {
		/// Target expression.
		expr: Box<Expr>,
		/// Value list.
		list: Vec<Expr>,
		/// True if `NOT IN`.
		negated: bool,
		/// Source span.
		span: Span,
	},

	/// `IS NULL` or `IS NOT NULL` test.
	IsNull {
		/// Target expression.
		expr: Box<Expr>,
		/// True if `IS NOT NULL`.
		negated: bool,
		/// Source span.
		span: Span,
	},

	/// `IS TRUE` or `IS NOT TRUE` test.
	IsTrue {
		/// Target expression.
		expr: Box<Expr>,
		/// True if `IS NOT TRUE`.
		negated: bool,
		/// Source span.
		span: Span,
	},

	/// `IS FALSE` or `IS NOT FALSE` test.
	IsFalse {
		/// Target expression.
		expr: Box<Expr>,
		/// True if `IS NOT FALSE`.
		negated: bool,
		/// Source span.
		span: Span,
	},

	/// `IS UNKNOWN` or `IS NOT UNKNOWN` test.
	IsUnknown {
		/// Target expression.
		expr: Box<Expr>,
		/// True if `IS NOT UNKNOWN`.
		negated: bool,
		/// Source span.
		span: Span,
	},

	/// `IS DISTINCT FROM` or `IS NOT DISTINCT FROM` comparison.
	IsDistinctFrom {
		/// Left operand.
		left: Box<Expr>,
		/// Right operand.
		right: Box<Expr>,
		/// True if `IS NOT DISTINCT FROM`.
		negated: bool,
		/// Source span.
		span: Span,
	},

	/// `CASE [operand] WHEN ... THEN ... [ELSE ...] END` expression.
	Case {
		/// Optional operand for simple case: `CASE x WHEN 1 THEN 'a' ...`.
		operand: Option<Box<Expr>>,
		/// List of WHEN ... THEN ... branches.
		branches: Vec<WhenClause>,
		/// Optional ELSE branch expression.
		else_expr: Option<Box<Expr>>,
		/// Source span.
		span: Span,
	},

	/// `COLLATE "collation"` expression.
	Collate {
		/// Expression being collated.
		expr: Box<Expr>,
		/// Collation name.
		collation: QualifiedName,
		/// Source span.
		span: Span,
	},

	/// Explicit array constructor (e.g. `ARRAY[1, 2, 3]`).
	ArrayConstructor {
		/// Elements of the array.
		elements: Vec<Expr>,
		/// Source span.
		span: Span,
	},

	/// Row constructor (e.g. `ROW(1, 'a', TRUE)`).
	Row {
		/// Row components.
		fields: Vec<Expr>,
		/// Source span.
		span: Span,
	},

	/// Explicitly parenthesized expression `(expr)`.
	Grouping {
		/// Inner expression.
		expr: Box<Expr>,
		/// Source span.
		span: Span,
	},
}

impl Expr {
	/// Return the source span of the expression.
	pub const fn span(&self) -> Span {
		match self {
			Self::Literal {
				span,
				..
			} => *span,
			Self::ColumnRef(col) => col.span,
			Self::Param {
				span,
				..
			} => *span,
			Self::UnaryOp {
				span,
				..
			} => *span,
			Self::BinaryOp {
				span,
				..
			} => *span,
			Self::TypeCast {
				span,
				..
			} => *span,
			Self::FunctionCall {
				span,
				..
			} => *span,
			Self::Subscript {
				span,
				..
			} => *span,
			Self::Slice {
				span,
				..
			} => *span,
			Self::Between {
				span,
				..
			} => *span,
			Self::InList {
				span,
				..
			} => *span,
			Self::IsNull {
				span,
				..
			} => *span,
			Self::IsTrue {
				span,
				..
			} => *span,
			Self::IsFalse {
				span,
				..
			} => *span,
			Self::IsUnknown {
				span,
				..
			} => *span,
			Self::IsDistinctFrom {
				span,
				..
			} => *span,
			Self::Case {
				span,
				..
			} => *span,
			Self::Collate {
				span,
				..
			} => *span,
			Self::ArrayConstructor {
				span,
				..
			} => *span,
			Self::Row {
				span,
				..
			} => *span,
			Self::Grouping {
				span,
				..
			} => *span,
		}
	}
}
