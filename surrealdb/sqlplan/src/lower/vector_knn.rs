//! The nearest-neighbour search that `ORDER BY column <-> query LIMIT k` asks for.
//!
//! The engine answers the operator `column <|k, ef|> query` from an HNSW index over the column,
//! and `column <|k, DISTANCE|> query` by a scan that keeps the `k` nearest. The search is asked
//! of an index when it is the one index over the column and ranks by the distance of the
//! `ORDER BY`, and of a scan otherwise. The select keeps its `ORDER BY`, which sorts the `k` rows
//! it is left with.

use surrealdb_expr::expr::BinaryOperator;
use surrealdb_expr::expr::logical::LogicalExpr;
use surrealdb_expr::expr::operator::NearestNeighbor;

use super::expr::{is_null_expr, lower_bound_expr};
use crate::dialect::{DialectPolicy, SqlDialect};
use crate::error::PlanError;
use crate::ir::{
	BoundBinaryOp, BoundExpr, BoundLiteral, BoundOrder, BoundOrderDirection, BoundSelect,
	BoundTableSource,
};
use crate::vector::VectorMetric;

/// The candidates an index search keeps at the least, which is the `hnsw.ef_search` of pgvector.
/// A search for more rows keeps as many candidates as it is asked for rows.
const MIN_EF_SEARCH: u32 = 40;

/// The nearest-neighbour predicate `select` asks for, or `None` when it asks for no such search.
///
/// A select asks for one when it reads one table, orders its rows first by the distance between a
/// column of the table and a query that reads no row, ascending, and keeps at most a literal
/// number of them. Only a select of rows qualifies: one that groups, aggregates, removes
/// duplicates or combines queries orders rows that are not those of the table.
///
/// The search covers `LIMIT + OFFSET` rows, the most that the select can answer. A row whose
/// column is NULL has no distance and is not found by it, as an index over the column does not
/// hold it.
pub(super) fn knn_predicate(
	select: &BoundSelect,
	policy: &DialectPolicy,
) -> Result<Option<LogicalExpr>, PlanError> {
	if policy.dialect != SqlDialect::Postgres
		|| select.set_op.is_some()
		|| select.distinct.is_some()
		|| select.having.is_some()
		|| !select.group_by.is_empty()
		|| select.targets.iter().any(|target| target.expr.contains_window())
		|| select
			.targets
			.iter()
			.any(|target| super::expr::bound_expr_contains_aggregate(&target.expr))
	{
		return Ok(None);
	}
	let Some(BoundTableSource::Table {
		..
	}) = &select.from
	else {
		return Ok(None);
	};
	let Some(first) = select.order_by.first() else {
		return Ok(None);
	};
	// Later keys only break ties, which a search that keeps the first `k` rows by distance would
	// break arbitrarily, and NULLs placed first are rows the search does not find.
	if select.order_by.len() != 1
		|| first.direction != BoundOrderDirection::Asc
		|| first.nulls_first
	{
		return Ok(None);
	}
	let Some((metric, a, b)) = distance_of(&first.expr) else {
		return Ok(None);
	};
	let (column, query) = match (is_row_column(a), is_row_column(b)) {
		(true, false) => (a, b),
		(false, true) => (b, a),
		(true, true) | (false, false) => return Ok(None),
	};
	if !reads_no_row(query) {
		return Ok(None);
	}
	let Some(k) = covered_rows(select) else {
		return Ok(None);
	};
	let indexed = if let BoundExpr::Column(column) = column {
		single_index_ranks_by(select, &column.column, metric)
	} else {
		false
	};
	let search = if indexed {
		NearestNeighbor::Approximate(k, k.max(MIN_EF_SEARCH))
	} else {
		NearestNeighbor::K(k, metric.index_distance())
	};
	Ok(Some(LogicalExpr::Binary {
		left: Box::new(lower_bound_expr(column, policy)?),
		op: BinaryOperator::NearestNeighbor(Box::new(search)),
		right: Box::new(lower_bound_expr(query, policy)?),
	}))
}

/// Whether the one HNSW index over `column` ranks by `metric`.
///
/// The operator that asks an index for its nearest neighbours names no distance, so the engine
/// answers it from the index it finds over the column. With several indexes over the column it
/// may be one that ranks by another distance, so the search is asked of an index only when no
/// other index could answer it.
fn single_index_ranks_by(select: &BoundSelect, column: &str, metric: VectorMetric) -> bool {
	let mut over_column = select.hnsw_indexes.iter().filter(|index| index.column == column);
	matches!((over_column.next(), over_column.next()), (Some(index), None) if index.metric == metric)
}

/// The test that tells the rows an `ORDER BY` of a distance leaves last, when it can be made
/// without computing the distance: the distance between a column and a query is NULL where the
/// column is, so the column says which rows have none.
pub(super) fn null_probe(
	order: &BoundOrder,
	policy: &DialectPolicy,
) -> Result<Option<LogicalExpr>, PlanError> {
	if policy.dialect != SqlDialect::Postgres {
		return Ok(None);
	}
	let Some((_, a, b)) = distance_of(&order.expr) else {
		return Ok(None);
	};
	let column = match (is_row_column(a), is_row_column(b)) {
		(true, false) if reads_no_row(b) => a,
		(false, true) if reads_no_row(a) => b,
		_ => return Ok(None),
	};
	Ok(Some(is_null_expr(lower_bound_expr(column, policy)?)))
}

/// The metric and the operands of `expr` when it is a distance: a distance operator, or a call of
/// a distance function.
fn distance_of(expr: &BoundExpr) -> Option<(VectorMetric, &BoundExpr, &BoundExpr)> {
	if let BoundExpr::Binary {
		left,
		op: BoundBinaryOp::VectorDistance(metric),
		right,
	} = expr
	{
		return Some((*metric, left, right));
	}
	if let BoundExpr::FunctionCall {
		name,
		args,
		..
	} = expr
		&& let (Some(metric), [a, b]) = (VectorMetric::from_function(name), args.as_slice())
	{
		return Some((metric, a, b));
	}
	None
}

/// Whether `expr` is a column of the table the select reads.
fn is_row_column(expr: &BoundExpr) -> bool {
	matches!(
		expr,
		BoundExpr::Column(column)
			if column.relation_index == 0
				&& column.correlation_depth == 0
				&& column.id_table.is_none()
				&& column.link_table.is_none()
				&& column.column != "*"
	)
}

/// Whether `expr` is a query that is known without reading a row: a parameter, or a text literal
/// that is the text form of a vector, cast to one.
fn reads_no_row(expr: &BoundExpr) -> bool {
	if matches!(expr, BoundExpr::Param { .. }) {
		return true;
	}
	if let BoundExpr::Cast {
		expr,
		target_type,
	} = expr
	{
		return target_type.starts_with("vector")
			&& matches!(
				expr.as_ref(),
				BoundExpr::Literal(BoundLiteral::String(_)) | BoundExpr::Param { .. }
			);
	}
	false
}

/// The number of rows a select answers at most, when `LIMIT` and `OFFSET` are literals.
fn covered_rows(select: &BoundSelect) -> Option<u32> {
	let Some(BoundExpr::Literal(BoundLiteral::Integer(limit))) = &select.limit else {
		return None;
	};
	let offset = match &select.offset {
		None => 0,
		Some(BoundExpr::Literal(BoundLiteral::Integer(offset))) => *offset,
		Some(_) => return None,
	};
	if *limit < 1 || offset < 0 {
		return None;
	}
	u32::try_from(limit.checked_add(offset)?).ok()
}
