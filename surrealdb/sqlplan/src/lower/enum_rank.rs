//! Declaration-order comparison of enum columns.
//!
//! A column that holds the labels of an enum stores them as strings, which compare by spelling.
//! SQL orders an enum by the position its label was declared at, so a comparison, a `BETWEEN`, an
//! `ORDER BY` and `MIN` and `MAX` over such a column compare `enum::rank(enum, value)` instead.
//! The operand opposite a column is ranked by the same enum, which refuses a label the enum does
//! not hold.

use surrealdb_expr::expr::Function;
use surrealdb_expr::expr::logical::{LogicalCall, LogicalExpr, LogicalLiteral};
use surrealdb_strand::Strand;

use crate::catalog::{EnumColumn, EnumRef, EnumShape};
use crate::ir::BoundExpr;

/// The enum a single-label column read by `expr` holds, where `expr` is a plain column reference.
pub(crate) fn scalar_enum(expr: &BoundExpr) -> Option<&EnumRef> {
	match expr {
		BoundExpr::Column(col) => match col.enum_column.as_deref() {
			Some(EnumColumn {
				reference,
				shape: EnumShape::Scalar,
			}) if col.id_table.is_none() => Some(reference),
			Some(_) | None => None,
		},
		BoundExpr::Literal(_)
		| BoundExpr::Access {
			..
		}
		| BoundExpr::Param {
			..
		}
		| BoundExpr::Binary {
			..
		}
		| BoundExpr::Unary {
			..
		}
		| BoundExpr::FunctionCall {
			..
		}
		| BoundExpr::Case {
			..
		}
		| BoundExpr::Between {
			..
		}
		| BoundExpr::InList {
			..
		}
		| BoundExpr::IsNull {
			..
		}
		| BoundExpr::Cast {
			..
		}
		| BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::InSubquery {
			..
		}
		| BoundExpr::ArraySubquery(_)
		| BoundExpr::WindowFunction {
			..
		}
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		} => None,
	}
}

/// Whether `expr` is a plain column reference to a column that holds a set of enum labels.
pub(crate) fn is_label_set(expr: &BoundExpr) -> bool {
	matches!(
		expr,
		BoundExpr::Column(col) if matches!(
			col.enum_column.as_deref(),
			Some(EnumColumn { shape: EnumShape::Set, .. })
		)
	)
}

/// The set of members a string literal spells, as MySQL reads a `SET` value: members separated by
/// commas, and the empty text as the empty set.
pub(crate) fn label_set_literal(text: &str) -> LogicalExpr {
	let members = if text.is_empty() {
		Vec::new()
	} else {
		text.split(',').map(string).collect()
	};
	LogicalExpr::Prefix {
		op: surrealdb_expr::expr::PrefixOperator::Cast(surrealdb_expr::expr::Kind::Set(
			Box::new(surrealdb_expr::expr::Kind::String),
			None,
		)),
		expr: Box::new(LogicalExpr::Array(members)),
	}
}

fn string(value: &str) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::String(Strand::from(value)))
}

/// The first argument of the enum functions: the enum's name, or an array of the labels of an
/// inline enum.
fn reference_arg(reference: &EnumRef) -> LogicalExpr {
	match reference {
		EnumRef::Named(name) => string(name),
		EnumRef::Inline(labels) => {
			LogicalExpr::Array(labels.iter().map(|label| string(label)).collect())
		}
	}
}

/// `enum::rank(reference, value)`: the position of the label `value` in the enum's declaration
/// order, `NONE` for no value.
pub(crate) fn rank(reference: &EnumRef, value: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal("enum::rank".to_owned()),
		args: vec![reference_arg(reference), value],
	}))
}

/// The label of the group's `values` that comes first (`last` false) or last in the enum's
/// declaration order, `NULL` for a group with no label. `values` is the column's value, which is
/// collected over the group.
pub(crate) fn extreme(reference: &EnumRef, values: LogicalExpr, last: bool) -> LogicalExpr {
	let collected = LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal("array::group".to_owned()),
		args: vec![values],
	}));
	LogicalExpr::Binary {
		left: Box::new(LogicalExpr::Call(Box::new(LogicalCall {
			receiver: Function::Normal(
				if last {
					"enum::max"
				} else {
					"enum::min"
				}
				.to_owned(),
			),
			args: vec![reference_arg(reference), collected],
		}))),
		op: surrealdb_expr::expr::BinaryOperator::NullCoalescing,
		right: Box::new(LogicalExpr::Literal(LogicalLiteral::Null)),
	}
}
