//! Record link columns: stored as record links, written and searched by key.
//!
//! A column the catalog says holds links to one table (`record<t>`) stores the record `t:<key>`,
//! while a SQL client reads, writes and searches it as the key. Reading it needs no conversion (the
//! wire layer presents a link as its key). Comparing it with a key names the record of that table
//! the key stands for, and compares the stored link with that record, plainly, so an index on the
//! column can serve the comparison. The comparison forms are those of the `id` column
//! ([`crate::lower::id_key`]) with the target table of the link in the place of the table the `id`
//! belongs to.
//!
//! A link column compared with a record link, another link column of the same target, or the `id`
//! of its target table needs no conversion: they are compared as stored.
//!
//! A link column that stands alone is the link as stored ([`lower_stored`]); as an operand of any
//! other expression it is its key, so that arithmetic, text and casts see an integer or a string.

use surrealdb_expr::expr::logical::LogicalExpr;
use surrealdb_expr::val::Number;

use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::ir::{BoundColumnRef, BoundExpr, BoundLiteral};
use crate::lower::expr::{lower_bound_expr, lower_column_raw};
use crate::lower::id_key::{
	contains_subquery, id_column, is_key_or_link, key_operand, key_read_as, record_of,
	record_of_operand, record_of_operand_typed, record_or_value,
};

/// The column of `expr` when it holds record links to a table, and that table.
pub(crate) fn link_column(expr: &BoundExpr) -> Option<(&BoundColumnRef, &str)> {
	match expr {
		BoundExpr::Column(col) => col.link_table.as_deref().map(|table| (col, table)),
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

/// Lower `expr` as the value it is stored as: a link column is the link itself.
///
/// This is how a value is lowered where it stands alone and the wire layer presents it (a projected
/// item, a group key, an ordering key, a null test), and where it is handed to
/// `type::record(<table>, <value>)`, which names the record by the key of a link. Everywhere else a
/// link column is its key, which is what [`lower_bound_expr`] lowers it to.
pub(crate) fn lower_stored(
	expr: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	match link_column(expr) {
		Some((col, _)) => Ok(lower_column_raw(col)),
		None => lower_bound_expr(expr, policy),
	}
}

/// The column of `expr` when it stores a record id of a table, and that table: the `id` of a base
/// table, or a link column.
fn stored_record_column(expr: &BoundExpr) -> Option<(&BoundColumnRef, &str)> {
	id_column(expr).or_else(|| link_column(expr))
}

/// The two columns `left` and `right` when they store record ids of the same table and one of them
/// is a link column: they are compared as stored. Two `id` columns of the same table are not a
/// pair of this kind; the `id` rules compare them.
pub(crate) fn same_table_pair<'a>(
	left: &'a BoundExpr,
	right: &'a BoundExpr,
) -> Option<(&'a BoundColumnRef, &'a BoundColumnRef)> {
	let (l, l_table) = stored_record_column(left)?;
	let (r, r_table) = stored_record_column(right)?;
	let has_link = link_column(left).is_some() || link_column(right).is_some();
	(l_table == r_table && has_link).then_some((l, r))
}

/// What a link column to a table is compared with.
pub(crate) enum LinkOperand {
	/// The record of the table the operand names, or the operand itself when it is already a stored
	/// record of the table.
	Record(LogicalExpr),
	/// An expression that holds a subquery, lowered as it is. It cannot be converted: the
	/// conversion would evaluate it more than once.
	Subquery(LogicalExpr),
	/// A value that names no record (a float, a boolean, `NULL`, the empty string), lowered as it
	/// is. Compared with a link it matches nothing.
	Plain(LogicalExpr),
}

/// Lower `other`, which is compared with a link column to `table`.
///
/// An integer or non-empty string literal is the record literal `table:<key>`, a parameter, a
/// column or an expression is the record `type::record(<table>, <value>)` names (a value that names
/// no record gives `NULL`), the `id` of `table` or a link column to `table` is compared as stored,
/// and the `id` of another table is compared by its key.
pub(crate) fn link_operand(
	table: &str,
	key_read: crate::ir::KeyRead,
	other: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<LinkOperand, PlanError> {
	link_operand_with(table, key_read, other, policy, false)
}

/// An operand converted without turning a non-null, non-key value into NULL.
pub(crate) fn link_comparison_operand(
	table: &str,
	key_read: crate::ir::KeyRead,
	other: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<LinkOperand, PlanError> {
	link_operand_with(table, key_read, other, policy, true)
}

fn link_operand_with(
	table: &str,
	key_read: crate::ir::KeyRead,
	other: &BoundExpr,
	policy: &DialectPolicy,
	preserve_value: bool,
) -> Result<LinkOperand, PlanError> {
	let record = |value| {
		let value = key_read_as(key_read, value);
		if preserve_value {
			record_or_value(table, value)
		} else {
			record_of(table, value)
		}
	};
	let integral = if let BoundExpr::Literal(BoundLiteral::Float(value)) = other {
		Number::Float(*value).as_int_lossless()
	} else if let BoundExpr::Literal(BoundLiteral::Numeric(text)) = other
		&& let Ok(crate::lower::expr::NumericValue::Decimal(value)) =
			crate::lower::expr::numeric_value(text)
	{
		Number::Decimal(value).as_int_lossless()
	} else {
		None
	};
	if let Some(value) = integral {
		return Ok(LinkOperand::Record(record_of_operand(
			table,
			&crate::lower::id_key::KeyOperand::Int(value),
		)));
	}
	if let Some(key) = key_operand(other) {
		return Ok(LinkOperand::Record(match key {
			crate::lower::id_key::KeyOperand::Param(_) if preserve_value => {
				record(lower_bound_expr(other, policy)?)
			}
			crate::lower::id_key::KeyOperand::Int(_)
			| crate::lower::id_key::KeyOperand::Str(_)
			| crate::lower::id_key::KeyOperand::Param(_) => record_of_operand_typed(table, &key, key_read),
		}));
	}
	if let Some((col, other_table)) = stored_record_column(other) {
		return Ok(LinkOperand::Record(if other_table == table {
			lower_column_raw(col)
		} else {
			record(lower_stored(other, policy)?)
		}));
	}
	if is_key_or_link(other) {
		return Ok(LinkOperand::Record(record(lower_stored(other, policy)?)));
	}
	let lowered = lower_bound_expr(other, policy)?;
	Ok(if contains_subquery(other) {
		LinkOperand::Subquery(lowered)
	} else {
		LinkOperand::Plain(lowered)
	})
}
