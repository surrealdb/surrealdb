//! `INTERSECT` and `EXCEPT`: the rows of the first query that the second query has an equal row
//! for, or has none for, each answered once.
//!
//! Both are a semi or an anti join of the two queries on the equality of their columns, read under
//! the names the first query gives them. Two rows are equal when every pair of their columns is
//! equal in SurrealQL, which takes `NULL` as equal to `NULL`, as the set operations of SQL do. SQL
//! has no absent value besides `NULL`, so a column a row does not have is read as `NULL`.
//!
//! The join has one key, so that the engine runs it as one membership test against the second
//! query's answer. A query of several columns joins on the array of its columns: a join with
//! several keys reads each further key through a query run once per row of the first query, whose
//! cost is the product of the sizes of the two queries.
//!
//! Each query is a subquery of the join. The engine runs the join as a filter over the first
//! query's rows and reads the key of the second query by projecting its rows, and it can do neither
//! over the rows of a projection, which each query ends in.

use std::collections::HashSet;

use surrealdb_expr::expr::BinaryOperator;
use surrealdb_expr::expr::logical::{
	EvalFrame, JoinKind, LogicalDistinct, LogicalExpr, LogicalFields, LogicalJoin, LogicalLiteral,
	LogicalOp, LogicalProject, LogicalSubquery, RelationAlias,
};

use super::{column_name, has_wildcard};
use crate::error::PlanError;
use crate::ir::BoundTarget;

/// The rows of `left`, the first query of the set operation, that `right`, the second, has an
/// equal row for with a `Semi` `kind` (`INTERSECT`), or has none for with an `Anti` one
/// (`EXCEPT`).
///
/// `first` and `later` are the columns of the two queries; when neither holds a `*`, they are as
/// many, and `later` is named as `first` is, which the join reads the rows of `right` by.
///
/// Refuses what it cannot answer as SQL does rather than answering it differently: the `ALL`
/// forms, which count the copies of a row, a `*` in either query, whose columns are not known
/// here, a first query with a column that has no name or shares one, and a subquery that answers
/// a single value, whose rows are not objects with columns to read.
pub(super) fn lower(
	kind: JoinKind,
	all: bool,
	first: &[BoundTarget],
	later: &[BoundTarget],
	left: LogicalOp,
	right: LogicalOp,
) -> Result<LogicalOp, PlanError> {
	let keyword = if kind == JoinKind::Semi {
		"INTERSECT"
	} else {
		"EXCEPT"
	};
	if all {
		return Err(PlanError::unsupported(format!(
			"{keyword} ALL is not supported; use {keyword} to answer each row once"
		)));
	}
	let names = column_names(keyword, first, later)?;
	if !answers_objects(&left) {
		return Err(PlanError::unsupported(format!(
			"{keyword} is not supported in a subquery that answers a single value"
		)));
	}

	let alias = RelationAlias::new(0);
	let on = (
		key(names.iter().map(|name| null_if_absent(LogicalExpr::Field(name.as_str().into())))),
		key(names.iter().map(|name| null_if_absent(alias.access(name.as_str())))),
	);
	let join = LogicalOp::Join(LogicalJoin::new(
		subquery(left),
		subquery(right),
		kind,
		vec![on],
		None,
		alias,
		EvalFrame::caller(),
	));
	Ok(LogicalOp::Distinct(LogicalDistinct {
		input: Box::new(join),
	}))
}

/// The names of the columns the join compares: those of `first`, each of which has to be told
/// from the others.
fn column_names(
	keyword: &str,
	first: &[BoundTarget],
	later: &[BoundTarget],
) -> Result<Vec<String>, PlanError> {
	if has_wildcard(first) || has_wildcard(later) {
		return Err(PlanError::unsupported(format!(
			"{keyword} is not supported with * in either query; list the columns"
		)));
	}
	let mut seen = HashSet::with_capacity(first.len());
	let mut names = Vec::with_capacity(first.len());
	for target in first {
		let Some(name) = column_name(target) else {
			return Err(PlanError::unsupported(format!(
				"{keyword} needs a name for every column of its first query; give each computed \
				 column an alias with AS"
			)));
		};
		if !seen.insert(name.clone()) {
			return Err(PlanError::unsupported(format!(
				"{keyword} needs every column of its first query to have a different name; \
				 give the columns that share a name different aliases with AS"
			)));
		}
		names.push(name);
	}
	Ok(names)
}

/// Whether the rows `op` answers are objects, which a column is read from, and not bare values.
/// A `Distinct` keeps the rows it is given as they are.
fn answers_objects(mut op: &LogicalOp) -> bool {
	while let LogicalOp::Distinct(LogicalDistinct {
		input,
	}) = op
	{
		op = input;
	}
	matches!(
		op,
		LogicalOp::Project(LogicalProject {
			fields: LogicalFields::Select(_),
			..
		})
	)
}

/// The key of a row: its column when it has one, otherwise the array of its columns.
fn key(columns: impl Iterator<Item = LogicalExpr>) -> LogicalExpr {
	match <[LogicalExpr; 1]>::try_from(columns.collect::<Vec<_>>()) {
		Ok([column]) => column,
		Err(columns) => LogicalExpr::Array(columns),
	}
}

/// `column`, or `NULL` where the row has no such field.
fn null_if_absent(column: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(column),
		op: BinaryOperator::NullCoalescing,
		right: Box::new(LogicalExpr::Literal(LogicalLiteral::Null)),
	}
}

fn subquery(op: LogicalOp) -> LogicalOp {
	LogicalOp::Subquery(LogicalSubquery {
		input: Box::new(op),
	})
}

#[cfg(test)]
mod tests;
