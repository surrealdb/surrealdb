//! Expression AST nodes for MySQL.

use crate::ast::literal::Literal;
use crate::ast::operator::{BinaryOperator, UnaryOperator};
use crate::ast::stmt::SelectStmt;
use crate::lexer::span::Span;

/// Truth value for `IS [NOT] TRUE / FALSE / UNKNOWN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruthValue {
	True,
	False,
	Unknown,
}

/// A qualified or unqualified column reference (`col`, `tbl.col`, `db.tbl.col`, `*`).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
	pub fields: Vec<String>,
	pub span: Span,
}

/// A single `WHEN ... THEN ...` branch in a `CASE` expression.
#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
	pub when: Expr,
	pub then: Expr,
	pub span: Span,
}

/// An expression in MySQL AST.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
	Literal(Literal, Span),
	ColumnRef(ColumnRef),
	UserVar(String, Span),
	SystemVar {
		name: String,
		is_global: bool,
		span: Span,
	},
	Param(usize, Span),
	UnaryOp {
		op: UnaryOperator,
		expr: Box<Expr>,
		span: Span,
	},
	BinaryOp {
		left: Box<Expr>,
		op: BinaryOperator,
		right: Box<Expr>,
		span: Span,
	},
	FunctionCall {
		name: String,
		distinct: bool,
		args: Vec<Expr>,
		span: Span,
	},
	Cast {
		expr: Box<Expr>,
		target_type: String,
		span: Span,
	},
	Case {
		operand: Option<Box<Expr>>,
		branches: Vec<WhenClause>,
		else_expr: Option<Box<Expr>>,
		span: Span,
	},
	InList {
		expr: Box<Expr>,
		negated: bool,
		list: Vec<Expr>,
		span: Span,
	},
	InSubquery {
		expr: Box<Expr>,
		negated: bool,
		subquery: Box<SelectStmt>,
		span: Span,
	},
	Between {
		expr: Box<Expr>,
		negated: bool,
		low: Box<Expr>,
		high: Box<Expr>,
		span: Span,
	},
	IsNull {
		expr: Box<Expr>,
		negated: bool,
		span: Span,
	},
	IsTruthValue {
		expr: Box<Expr>,
		negated: bool,
		value: TruthValue,
		span: Span,
	},
	Exists {
		subquery: Box<SelectStmt>,
		span: Span,
	},
	Subquery {
		subquery: Box<SelectStmt>,
		span: Span,
	},
	Collate {
		expr: Box<Expr>,
		collation: String,
		span: Span,
	},
	Interval {
		expr: Box<Expr>,
		unit: String,
		span: Span,
	},
}

impl Expr {
	/// Return source span covering this expression.
	pub fn span(&self) -> Span {
		match self {
			Self::Literal(_, s) => *s,
			Self::ColumnRef(c) => c.span,
			Self::UserVar(_, s) => *s,
			Self::SystemVar {
				span,
				..
			} => *span,
			Self::Param(_, s) => *s,
			Self::UnaryOp {
				span,
				..
			} => *span,
			Self::BinaryOp {
				span,
				..
			} => *span,
			Self::FunctionCall {
				span,
				..
			} => *span,
			Self::Cast {
				span,
				..
			} => *span,
			Self::Case {
				span,
				..
			} => *span,
			Self::InList {
				span,
				..
			} => *span,
			Self::InSubquery {
				span,
				..
			} => *span,
			Self::Between {
				span,
				..
			} => *span,
			Self::IsNull {
				span,
				..
			} => *span,
			Self::IsTruthValue {
				span,
				..
			} => *span,
			Self::Exists {
				span,
				..
			} => *span,
			Self::Subquery {
				span,
				..
			} => *span,
			Self::Collate {
				span,
				..
			} => *span,
			Self::Interval {
				span,
				..
			} => *span,
		}
	}

	/// Recursively set all spans to zero.
	pub fn normalize_spans(&mut self) {
		let zero = Span::empty(0);
		match self {
			Self::Literal(_, s) => *s = zero,
			Self::ColumnRef(c) => c.span = zero,
			Self::UserVar(_, s) => *s = zero,
			Self::SystemVar {
				span,
				..
			} => *span = zero,
			Self::Param(_, s) => *s = zero,
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
			Self::FunctionCall {
				args,
				span,
				..
			} => {
				*span = zero;
				for a in args {
					a.normalize_spans();
				}
			}
			Self::Cast {
				expr,
				span,
				..
			} => {
				*span = zero;
				expr.normalize_spans();
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
				for b in branches {
					b.span = zero;
					b.when.normalize_spans();
					b.then.normalize_spans();
				}
				if let Some(el) = else_expr {
					el.normalize_spans();
				}
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
			Self::InSubquery {
				expr,
				subquery,
				span,
				..
			} => {
				*span = zero;
				expr.normalize_spans();
				subquery.normalize_spans();
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
			Self::IsNull {
				expr,
				span,
				..
			} => {
				*span = zero;
				expr.normalize_spans();
			}
			Self::IsTruthValue {
				expr,
				span,
				..
			} => {
				*span = zero;
				expr.normalize_spans();
			}
			Self::Exists {
				subquery,
				span,
			} => {
				*span = zero;
				subquery.normalize_spans();
			}
			Self::Subquery {
				subquery,
				span,
			} => {
				*span = zero;
				subquery.normalize_spans();
			}
			Self::Collate {
				expr,
				span,
				..
			} => {
				*span = zero;
				expr.normalize_spans();
			}
			Self::Interval {
				expr,
				span,
				..
			} => {
				*span = zero;
				expr.normalize_spans();
			}
		}
	}
}
