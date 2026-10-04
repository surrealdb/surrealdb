//! Lowering from BoundSelect to SurrealDB `LogicalOp`.

use surrealdb_expr::expr::logical::{
	EvalFrame, JoinKind, LogicalAggregate, LogicalDistinct, LogicalExpr, LogicalFields,
	LogicalFilter, LogicalJoin, LogicalLimit, LogicalOp, LogicalOrdering, LogicalProject,
	LogicalSort, LogicalSource, LogicalSubquery, LogicalValues, RelationAlias, SourceRef,
};
use surrealdb_strand::TableName;

use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::ir::{
	BoundBinaryOp, BoundExpr, BoundJoinCondition, BoundJoinType, BoundSelect, BoundTableSource,
};
use crate::lower::expr::{
	bound_expr_contains_aggregate, lower_bound_expr, lower_bound_order, lower_bound_target,
};

/// Lower a fully bound SQL `BoundSelect` into a SurrealDB `LogicalOp` execution plan.
pub fn lower_bound_select(
	select: &BoundSelect,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	// 1. FROM clause
	let mut op = match &select.from {
		None | Some(BoundTableSource::Values) => LogicalOp::Values(LogicalValues {
			expr: LogicalExpr::Array(vec![LogicalExpr::Object(vec![])]),
			for_update: select.for_update,
		}),
		Some(source) => {
			let mut join_counter = 0;
			lower_table_source(source, select.for_update, &mut join_counter, policy)?
		}
	};

	// 2. WHERE clause
	if let Some(where_expr) = &select.where_clause {
		op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(op),
			predicate: lower_bound_expr(where_expr, policy)?,
		});
	}

	// 3. GROUP BY & Aggregation
	let fields = LogicalFields::Select(
		select
			.targets
			.iter()
			.map(|t| lower_bound_target(t, policy))
			.collect::<Result<Vec<_>, _>>()?,
	);

	let grouped = !select.group_by.is_empty();
	let has_aggregates = select.targets.iter().any(|t| bound_expr_contains_aggregate(&t.expr));
	let is_aggregate = grouped || has_aggregates;

	if is_aggregate {
		let group = select
			.group_by
			.iter()
			.map(|g| lower_bound_expr(g, policy))
			.collect::<Result<Vec<_>, _>>()?;
		op = LogicalOp::Aggregate(LogicalAggregate {
			input: Box::new(op),
			group,
			fields: fields.clone(),
		});
	}

	// 4. HAVING clause
	if let Some(having_expr) = &select.having {
		op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(op),
			predicate: lower_bound_expr(having_expr, policy)?,
		});
	}

	// 5. DISTINCT clause
	if select.distinct.is_some() {
		op = LogicalOp::Distinct(LogicalDistinct {
			input: Box::new(op),
		});
	}

	// 6. ORDER BY clause
	if !select.order_by.is_empty() {
		let mut orders = Vec::new();
		for o in &select.order_by {
			orders.extend(lower_bound_order(o, policy)?);
		}
		op = LogicalOp::Sort(LogicalSort {
			input: Box::new(op),
			ordering: LogicalOrdering::Order(orders),
			tempfiles: false,
		});
	}

	// 7. LIMIT & OFFSET clause
	if select.limit.is_some() || select.offset.is_some() {
		let limit = select.limit.as_ref().map(|l| lower_bound_expr(l, policy)).transpose()?;
		let start = select.offset.as_ref().map(|o| lower_bound_expr(o, policy)).transpose()?;
		op = LogicalOp::Limit(LogicalLimit {
			input: Box::new(op),
			start,
			limit,
		});
	}

	// 8. Projection (for non-aggregate queries)
	if !is_aggregate {
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields,
			omit: vec![],
		});
	} else {
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::all(),
			omit: vec![],
		});
	}

	Ok(op)
}

fn lower_table_source(
	source: &BoundTableSource,
	for_update: bool,
	join_counter: &mut usize,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	match source {
		BoundTableSource::Values => Ok(LogicalOp::Values(LogicalValues {
			expr: LogicalExpr::Array(vec![LogicalExpr::Object(vec![])]),
			for_update,
		})),
		BoundTableSource::Table {
			name,
			..
		} => Ok(LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(TableName::from(name.as_str())),
			schema: None,
			with: None,
			for_update,
		})),
		BoundTableSource::Subquery {
			query,
			..
		} => {
			let inner = lower_bound_select(query, policy)?;
			Ok(LogicalOp::Subquery(LogicalSubquery {
				input: Box::new(inner),
			}))
		}
		BoundTableSource::Join {
			left,
			join_type,
			right,
			condition,
		} => {
			let left_op = lower_table_source(left, for_update, join_counter, policy)?;
			let right_op = lower_table_source(right, for_update, join_counter, policy)?;

			let kind = match join_type {
				BoundJoinType::Inner | BoundJoinType::Cross => JoinKind::Inner,
				BoundJoinType::Left => JoinKind::Left,
				_ => {
					return Err(PlanError::UnsupportedStatement(format!(
						"Join type {join_type:?} not supported directly in streaming engine"
					)));
				}
			};

			let mut on = Vec::new();
			let mut filter = None;

			if let Some(cond) = condition {
				match cond {
					BoundJoinCondition::On(expr) => {
						if let BoundExpr::Binary {
							left,
							op: BoundBinaryOp::Equal,
							right,
						} = expr
						{
							let l = lower_bound_expr(left, policy)?;
							let r = lower_bound_expr(right, policy)?;
							on.push((l, r));
						} else {
							let cond_expr = lower_bound_expr(expr, policy)?;
							filter = Some(cond_expr);
						}
					}
					BoundJoinCondition::Using(_) => {
						return Err(PlanError::UnsupportedStatement(
							"USING join condition not supported yet".into(),
						));
					}
				}
			}

			let alias_idx = *join_counter;
			*join_counter += 1;

			Ok(LogicalOp::Join(LogicalJoin::new(
				left_op,
				right_op,
				kind,
				on,
				filter,
				RelationAlias::new(alias_idx),
				EvalFrame::caller(),
			)))
		}
	}
}
