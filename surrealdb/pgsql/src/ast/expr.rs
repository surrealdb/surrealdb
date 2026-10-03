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

/// Direction of sorting in `ORDER BY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
	/// Ascending sort (`ASC`).
	Asc,
	/// Descending sort (`DESC`).
	Desc,
}

/// Ordering of NULL values in `ORDER BY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
	/// NULL values sort first (`NULLS FIRST`).
	First,
	/// NULL values sort last (`NULLS LAST`).
	Last,
}

/// An item in an `ORDER BY` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
	/// Expression to order by.
	pub expr: Expr,
	/// Sort direction (`ASC` or `DESC`).
	pub direction: Option<OrderDirection>,
	/// Null ordering (`NULLS FIRST` or `NULLS LAST`).
	pub nulls: Option<NullsOrder>,
	/// Source span.
	pub span: Span,
}

/// Units for a window frame specification (`ROWS`, `RANGE`, `GROUPS`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameUnits {
	/// Physical row count frame.
	Rows,
	/// Value offset frame.
	Range,
	/// Peer group offset frame.
	Groups,
}

/// A bound in a window frame specification.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
	/// `CURRENT ROW`.
	CurrentRow,
	/// `UNBOUNDED PRECEDING`.
	UnboundedPreceding,
	/// `UNBOUNDED FOLLOWING`.
	UnboundedFollowing,
	/// `<expr> PRECEDING`.
	Preceding(Box<Expr>),
	/// `<expr> FOLLOWING`.
	Following(Box<Expr>),
}

/// Exclusion clause in a window frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameExclusion {
	/// `EXCLUDE CURRENT ROW`.
	CurrentRow,
	/// `EXCLUDE GROUP`.
	Group,
	/// `EXCLUDE TIES`.
	Ties,
	/// `EXCLUDE NO OTHERS`.
	NoOthers,
}

/// Specification of a window frame (`ROWS BETWEEN ... AND ...`).
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
	/// Units (`ROWS`, `RANGE`, or `GROUPS`).
	pub units: WindowFrameUnits,
	/// Start bound.
	pub start: WindowFrameBound,
	/// End bound (optional when not using `BETWEEN`).
	pub end: Option<WindowFrameBound>,
	/// Optional frame exclusion.
	pub exclusion: Option<WindowFrameExclusion>,
	/// Source span.
	pub span: Span,
}

/// Specification of a window in an `OVER` clause or `WINDOW` definition.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
	/// Existing window name referenced: `OVER (existing_win ...)`.
	pub window_name: Option<String>,
	/// `PARTITION BY` expressions.
	pub partition_by: Vec<Expr>,
	/// `ORDER BY` specifications.
	pub order_by: Vec<OrderByItem>,
	/// Window frame specification.
	pub frame: Option<WindowFrame>,
	/// Source span.
	pub span: Span,
}

impl WindowSpec {
	/// Recursively set all spans in this window specification to zero for semantic equality
	/// comparisons.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		self.span = zero;
		for expr in &mut self.partition_by {
			expr.normalize_spans();
		}
		for item in &mut self.order_by {
			item.span = zero;
			item.expr.normalize_spans();
		}
		if let Some(frame) = &mut self.frame {
			frame.span = zero;
			match &mut frame.start {
				WindowFrameBound::Preceding(e) | WindowFrameBound::Following(e) => {
					e.normalize_spans();
				}
				_ => {}
			}
			if let Some(WindowFrameBound::Preceding(e) | WindowFrameBound::Following(e)) =
				&mut frame.end
			{
				e.normalize_spans();
			}
		}
	}
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
		/// Optional `OVER (...)` window specification.
		over: Option<WindowSpec>,
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

	/// Recursively set all spans in this expression to zero for semantic equality comparisons.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		match self {
			Self::Literal {
				span,
				..
			} => *span = zero,
			Self::ColumnRef(col) => col.span = zero,
			Self::Param {
				span,
				..
			} => *span = zero,
			Self::UnaryOp {
				expr,
				span,
				..
			} => {
				*span = zero;
				expr.normalize_spans();
			}
			Self::BinaryOp {
				left,
				right,
				span,
				..
			} => {
				*span = zero;
				left.normalize_spans();
				right.normalize_spans();
			}
			Self::TypeCast {
				expr,
				data_type,
				span,
			} => {
				*span = zero;
				data_type.span = zero;
				expr.normalize_spans();
			}
			Self::FunctionCall {
				name,
				args,
				over,
				span,
				..
			} => {
				*span = zero;
				name.span = zero;
				for arg in args {
					arg.normalize_spans();
				}
				if let Some(spec) = over {
					spec.normalize_spans();
				}
			}
			Self::Subscript {
				expr,
				index,
				span,
			} => {
				*span = zero;
				expr.normalize_spans();
				index.normalize_spans();
			}
			Self::Slice {
				expr,
				lower,
				upper,
				span,
			} => {
				*span = zero;
				expr.normalize_spans();
				if let Some(l) = lower {
					l.normalize_spans();
				}
				if let Some(u) = upper {
					u.normalize_spans();
				}
			}
			Self::Between {
				expr,
				low,
				high,
				span,
				..
			} => {
				*span = zero;
				expr.normalize_spans();
				low.normalize_spans();
				high.normalize_spans();
			}
			Self::InList {
				expr,
				list,
				span,
				..
			} => {
				*span = zero;
				expr.normalize_spans();
				for item in list {
					item.normalize_spans();
				}
			}
			Self::IsNull {
				expr,
				span,
				..
			}
			| Self::IsTrue {
				expr,
				span,
				..
			}
			| Self::IsFalse {
				expr,
				span,
				..
			}
			| Self::IsUnknown {
				expr,
				span,
				..
			} => {
				*span = zero;
				expr.normalize_spans();
			}
			Self::IsDistinctFrom {
				left,
				right,
				span,
				..
			} => {
				*span = zero;
				left.normalize_spans();
				right.normalize_spans();
			}
			Self::Case {
				operand,
				branches,
				else_expr,
				span,
			} => {
				*span = zero;
				if let Some(op) = operand {
					op.normalize_spans();
				}
				for branch in branches {
					branch.span = zero;
					branch.when.normalize_spans();
					branch.then.normalize_spans();
				}
				if let Some(el) = else_expr {
					el.normalize_spans();
				}
			}
			Self::Collate {
				expr,
				collation,
				span,
			} => {
				*span = zero;
				collation.span = zero;
				expr.normalize_spans();
			}
			Self::ArrayConstructor {
				elements,
				span,
			} => {
				*span = zero;
				for elem in elements {
					elem.normalize_spans();
				}
			}
			Self::Row {
				fields,
				span,
			} => {
				*span = zero;
				for field in fields {
					field.normalize_spans();
				}
			}
			Self::Grouping {
				expr,
				span,
			} => {
				*span = zero;
				expr.normalize_spans();
			}
		}
	}
}
