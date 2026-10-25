//! Lowering from BoundSelect to SurrealDB `LogicalOp`.

use std::borrow::Cow;

use surrealdb_expr::expr::BinaryOperator;
use surrealdb_expr::expr::logical::{
	EvalFrame, JoinKind, LogicalAggregate, LogicalDistinct, LogicalExpr, LogicalField,
	LogicalFields, LogicalFilter, LogicalJoin, LogicalLimit, LogicalOp, LogicalOrdering,
	LogicalProject, LogicalSort, LogicalSource, LogicalSubquery, LogicalUnionAll, LogicalValues,
	OutputPath, RelationAlias, SourceRef,
};
use surrealdb_strand::TableName;

use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::ir::{
	BoundBinaryOp, BoundColumnRef, BoundExpr, BoundJoinCondition, BoundJoinType, BoundOrder,
	BoundSelect, BoundSetOpKind, BoundSetOperation, BoundTableSource, BoundTarget,
};
use crate::lower::expr::{
	bound_expr_contains_aggregate, lower_bound_expr, lower_bound_order, lower_bound_target,
	lower_column_raw, lower_lookup_predicate,
};
use crate::lower::id_key::{id_column, is_key_or_link, key_of, record_of};
use crate::lower::link::{LinkOperand, link_column, link_operand, lower_stored, same_table_pair};
use crate::output_keys::output_keys;

/// Lower a fully bound SQL `BoundSelect` into a SurrealDB `LogicalOp` execution plan.
///
/// The plan answers the select to a client, which reads each column by position. When the select's
/// columns are all listed, every row holds each column under its own field, the one the
/// [`ResultColumn::key`](crate::ResultColumn::key) of the select's descriptor names.
pub fn lower_bound_select(
	select: &BoundSelect,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	lower_bound_select_internal(select, false, true, policy)
}

/// Lower a subquery `BoundSelect` where single-column rows should be emitted as bare values.
pub fn lower_bound_subquery(
	select: &BoundSelect,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	lower_bound_select_internal(select, true, false, policy)
}

/// The targets as a select's rows hold them: with `keys`, each under its field, which the target
/// then names as its alias. Without, the rows are read by the names the engine gives their fields:
/// a derived table is read by the name its enclosing query uses for a column, and a select with a
/// wildcard has columns that are known only from its rows.
fn keyed_targets(targets: &[BoundTarget], keys: Option<Vec<String>>) -> Cow<'_, [BoundTarget]> {
	let Some(keys) = keys else {
		return Cow::Borrowed(targets);
	};
	Cow::Owned(
		targets
			.iter()
			.zip(keys)
			.map(|(target, key)| BoundTarget {
				alias: Some(key),
				..target.clone()
			})
			.collect(),
	)
}

fn lower_bound_select_internal(
	select: &BoundSelect,
	as_value: bool,
	keyed: bool,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	if !select.windows.is_empty() {
		crate::observability::global_metrics().record_unsupported_feature("WINDOW");
		return Err(PlanError::UnsupportedExpression(
			"window functions and WINDOW clauses are not supported in streaming execution engine"
				.into(),
		));
	}

	// 1. FROM clause
	let mut op = match &select.from {
		None | Some(BoundTableSource::Values) => LogicalOp::Values(LogicalValues {
			expr: LogicalExpr::Array(vec![LogicalExpr::Object(vec![])]),
			for_update: select.for_update,
			written: false,
		}),
		Some(source) => {
			let mut join_counter = 0;
			lower_table_source(
				source,
				select.for_update,
				&mut join_counter,
				policy,
				&select.user_tables,
			)?
		}
	};

	// 2. WHERE clause
	if let Some(where_expr) = &select.where_clause {
		op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(op),
			predicate: lower_where(where_expr, policy)?,
		});
	}

	// 3. GROUP BY & Aggregation
	if select.group_by.iter().any(bound_expr_contains_aggregate) {
		return Err(PlanError::AggregateNotAllowed("GROUP BY".into()));
	}
	let grouped = !select.group_by.is_empty();
	let has_aggregates = select.targets.iter().any(|t| bound_expr_contains_aggregate(&t.expr));
	let has_having_agg = select.having.as_ref().is_some_and(bound_expr_contains_aggregate);
	let has_order_agg = select.order_by.iter().any(|o| bound_expr_contains_aggregate(&o.expr));
	let is_aggregate =
		grouped || has_aggregates || has_having_agg || has_order_agg || select.having.is_some();

	// The select is read by position only if it is asked to be and its columns are all listed;
	// the queries of its set operations are read the same way.
	let keys = keyed.then(|| output_keys(&select.targets)).flatten();
	let keyed = keys.is_some();
	let targets = keyed_targets(&select.targets, keys);
	let mut aggregate_targets = targets.to_vec();

	// The engine recognises a projected field as a grouping key only when it is the stored path the
	// aggregation groups by. Grouping by the stored record ids of a table groups by its keys, so
	// an `id` grouped by is grouped and projected as stored, and read as its key afterwards.
	let stored_keys: Vec<&BoundExpr> = if is_aggregate {
		select
			.group_by
			.iter()
			.filter(|g| id_column(g).is_some_and(|(col, _)| col.correlation_depth == 0))
			.collect()
	} else {
		Vec::new()
	};
	let mut key_outputs: Vec<String> = Vec::new();
	for (target, original) in aggregate_targets.iter_mut().zip(targets.iter()) {
		if stored_keys.contains(&&original.expr) {
			let name = target_output_name(original);
			target.expr = as_stored(&original.expr);
			target.alias = Some(name.clone());
			key_outputs.push(name);
		}
	}

	let mut agg_mapping: Vec<(BoundExpr, String)> = Vec::new();
	for t in targets.iter() {
		let col_name = target_output_name(t);
		agg_mapping.push((t.expr.clone(), col_name));
	}

	for (index, group) in select.group_by.iter().enumerate() {
		if !agg_mapping.iter().any(|(expr, _)| expr == group) {
			let name = format!("__sql_group_{index}");
			aggregate_targets.push(BoundTarget {
				expr: if stored_keys.contains(&group) {
					as_stored(group)
				} else {
					group.clone()
				},
				alias: Some(name.clone()),
				is_wildcard: false,
				table_wildcard: None,
			});
			agg_mapping.push((group.clone(), name));
		}
	}

	let mut synthetic_idx = 0;
	if let Some(having_expr) = &select.having {
		let mut having_aggs = Vec::new();
		collect_aggregate_exprs(having_expr, &mut having_aggs);
		for agg in having_aggs {
			if !agg_mapping.iter().any(|(e, _)| e == &agg) {
				let syn_name = format!("__sql_having_{}", synthetic_idx);
				synthetic_idx += 1;
				aggregate_targets.push(BoundTarget {
					expr: agg.clone(),
					alias: Some(syn_name.clone()),
					is_wildcard: false,
					table_wildcard: None,
				});
				agg_mapping.push((agg, syn_name));
			}
		}
	}

	let mut order_syn_idx = 0;
	for o in &select.order_by {
		let mut order_aggs = Vec::new();
		collect_aggregate_exprs(&o.expr, &mut order_aggs);
		for agg in order_aggs {
			if !agg_mapping.iter().any(|(e, _)| e == &agg) {
				let syn_name = format!("__sql_order_{}", order_syn_idx);
				order_syn_idx += 1;
				aggregate_targets.push(BoundTarget {
					expr: agg.clone(),
					alias: Some(syn_name.clone()),
					is_wildcard: false,
					table_wildcard: None,
				});
				agg_mapping.push((agg, syn_name));
			}
		}
	}

	// The projection after an aggregate that keeps its rows as objects reads each selected column
	// back by `target_output_name`. A target that lowering leaves unnamed, a computed expression,
	// is given that name so the column is found.
	let reads_columns_by_name = is_aggregate
		&& (aggregate_targets.len() > select.targets.len()
			|| (as_value
				&& select.targets.len() == 1
				&& (select.having.is_some() || !select.order_by.is_empty())));
	if reads_columns_by_name {
		for t in &mut aggregate_targets {
			if !t.is_wildcard && t.alias.is_none() && !target_names_itself(t) {
				t.alias = Some(target_output_name(t));
			}
		}
	}

	// A subquery that selects one column answers with bare values. An aggregate with a HAVING or
	// an ORDER BY keeps its rows as objects, so that those clauses read their columns, and takes
	// the values from the one column it selects after them.
	let value_column = as_value && select.targets.len() == 1 && !select.targets[0].is_wildcard;
	let as_value =
		as_value && (!is_aggregate || (select.having.is_none() && select.order_by.is_empty()));
	let fields = if as_value && select.targets.len() == 1 && !select.targets[0].is_wildcard {
		let target = &select.targets[0].expr;
		let expr = lower_bound_expr(target, policy)?;
		let expr = if matches!(target, BoundExpr::Column(col) if col.id_table.is_none() && col.link_table.is_none())
		{
			key_of(expr, true)
		} else {
			expr
		};
		let alias = select.targets[0].alias.as_ref().map(|a| OutputPath::field(a.clone()));
		LogicalFields::Value {
			expr,
			alias,
		}
	} else {
		let mut lowered = aggregate_targets
			.iter()
			.map(|t| lower_bound_target(t, policy))
			.collect::<Result<Vec<_>, _>>()?;
		if !is_aggregate && sole_base_table(select).is_some() {
			let last_wildcard =
				aggregate_targets.iter().rposition(BoundTarget::is_unlisted_wildcard);
			read_wildcard_id_as_key(&mut lowered, last_wildcard);
		}
		LogicalFields::Select(lowered)
	};

	// A clause over the rows of an aggregation reads its stored `id` columns; one over the rows of
	// a set operation reads the keys the operands projected; any other reads the rows the query
	// reads.
	let rewritten_having =
		select.having.as_ref().map(|h| rewrite_aggregates_to_columns(h, &agg_mapping, true));

	let mut rewritten_order_by = Vec::new();
	for o in &select.order_by {
		let expr = if select.set_op.is_some() {
			rewrite_aggregates_to_columns(&o.expr, &agg_mapping, false)
		} else if is_aggregate {
			rewrite_aggregates_to_columns(&o.expr, &agg_mapping, true)
		} else {
			o.expr.clone()
		};
		rewritten_order_by.push(BoundOrder {
			expr,
			direction: o.direction,
			nulls_first: o.nulls_first,
		});
	}

	if is_aggregate {
		let group = select
			.group_by
			.iter()
			.map(|g| {
				if stored_keys.contains(&g) {
					lower_bound_expr(&as_stored(g), policy)
				} else {
					lower_stored(g, policy)
				}
			})
			.collect::<Result<Vec<_>, _>>()?;
		op = LogicalOp::Aggregate(LogicalAggregate {
			input: Box::new(op),
			group,
			fields: fields.clone(),
		});
	}

	// 4. HAVING clause
	if let Some(having_expr) = &rewritten_having {
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

	// 6. ORDER BY clause (if not deferred to set_op)
	if select.set_op.is_none() && !rewritten_order_by.is_empty() {
		let mut orders = Vec::new();
		for o in &rewritten_order_by {
			orders.extend(lower_bound_order(o, policy)?);
		}
		op = LogicalOp::Sort(LogicalSort {
			input: Box::new(op),
			ordering: LogicalOrdering::Order(orders),
			tempfiles: false,
		});
	}

	// 7. LIMIT & OFFSET clause (if not deferred to set_op)
	if select.set_op.is_none() && (select.limit.is_some() || select.offset.is_some()) {
		let limit = select.limit.as_ref().map(|l| lower_bound_expr(l, policy)).transpose()?;
		let start = select.offset.as_ref().map(|o| lower_bound_expr(o, policy)).transpose()?;
		op = LogicalOp::Limit(LogicalLimit {
			input: Box::new(op),
			start,
			limit,
		});
	}

	// 8. Projection
	if !is_aggregate {
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields,
			omit: vec![],
		});
	} else if !matches!(fields, LogicalFields::Value { .. }) && value_column {
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::Value {
				expr: LogicalExpr::Field(target_output_name(&targets[0]).into()),
				alias: None,
			},
			omit: vec![],
		});
	} else if matches!(fields, LogicalFields::Value { .. }) {
		// The aggregate already answers with the value of the one column.
	} else if aggregate_targets.len() > select.targets.len() {
		let project_fields = targets
			.iter()
			.map(|t| {
				let col_name = target_output_name(t);
				if key_outputs.contains(&col_name) {
					return key_output_field(col_name);
				}
				let alias = t.alias.as_ref().map(|a| OutputPath::field(a.clone()));
				LogicalField::user(LogicalExpr::Field(col_name.into()), alias)
			})
			.collect::<Vec<_>>();
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::Select(project_fields),
			omit: vec![],
		});
	} else if key_outputs.is_empty() {
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::all(),
			omit: vec![],
		});
	} else {
		let mut fields = vec![LogicalField::All];
		fields.extend(key_outputs.into_iter().map(key_output_field));
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::Select(fields),
			omit: vec![],
		});
	}

	// 9. Set Operations (UNION, INTERSECT, EXCEPT)
	if let Some(first) = &select.set_op {
		op = lower_set_operations(&targets, first, op, keyed, policy)?;

		// Apply outer ORDER BY after set operation
		if !rewritten_order_by.is_empty() {
			let mut orders = Vec::new();
			for o in &rewritten_order_by {
				orders.extend(lower_bound_order(o, policy)?);
			}
			op = LogicalOp::Sort(LogicalSort {
				input: Box::new(op),
				ordering: LogicalOrdering::Order(orders),
				tempfiles: false,
			});
		}

		// Apply outer LIMIT & OFFSET after set operation
		if select.limit.is_some() || select.offset.is_some() {
			let limit = select.limit.as_ref().map(|l| lower_bound_expr(l, policy)).transpose()?;
			let start = select.offset.as_ref().map(|o| lower_bound_expr(o, policy)).transpose()?;
			op = LogicalOp::Limit(LogicalLimit {
				input: Box::new(op),
				start,
				limit,
			});
		}
	}

	Ok(op)
}

/// Combine `left`, the lowered first query of a set operation, with the queries chained after it.
///
/// A chain `a op b op c` is held as nested right operands with no record of which operands were
/// parenthesised, so `a UNION ALL b UNION c` and `a UNION ALL (b UNION c)` are the same chain.
/// Only a chain made of `UNION` links alone, or of `UNION ALL` links alone, reads alike under every
/// grouping, so a chain of more than two queries is lowered only when it is one of those, and is
/// refused otherwise: a chain with an `INTERSECT` or `EXCEPT` link as a chain, a mix of `UNION`
/// and `UNION ALL` with advice to group it. An operand followed by further links cannot carry its
/// own ORDER BY, LIMIT or OFFSET for the same reason: only a parenthesised group can hold them.
///
/// A set operation answers rows under the columns of its first query, and each later query is
/// lowered under the same fields. When `keyed`, those are the keys of the first query.
fn lower_set_operations(
	first_targets: &[BoundTarget],
	first: &BoundSetOperation,
	left: LogicalOp,
	keyed: bool,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	if first.right.set_op.is_some() {
		let links = || std::iter::successors(Some(first), |l| l.right.set_op.as_ref());
		if links().any(|l| l.op != BoundSetOpKind::Union) {
			return Err(PlanError::unsupported(
				"a chain of more than two queries is supported only with UNION or UNION ALL",
			));
		}
		if links().any(|l| l.all != first.all) {
			return Err(PlanError::unsupported(
				"a chain of more than two queries must join them all with UNION or all with \
				 UNION ALL; group a mix with subqueries",
			));
		}
	}

	// The chain is cloned once and taken apart link by link, so each query is lowered without the
	// rest of the chain and the work stays linear in its length.
	let mut branches = Vec::new();
	let mut next = Some(first.clone());
	while let Some(link) = next.take() {
		let mut branch = *link.right;
		next = branch.set_op.take();
		if next.is_some()
			&& (!branch.order_by.is_empty() || branch.limit.is_some() || branch.offset.is_some())
		{
			return Err(PlanError::unsupported(
				"ORDER BY, LIMIT or OFFSET on a query that is followed by another set operation",
			));
		}
		if !has_wildcard(first_targets)
			&& !has_wildcard(&branch.targets)
			&& first_targets.len() != branch.targets.len()
		{
			return Err(PlanError::Custom(
				"each query of a set operation must have the same number of columns".into(),
			));
		}
		name_as_first_query(first_targets, &mut branch);
		branches
			.push((lower_bound_select_internal(&branch, false, keyed, policy)?, branch.targets));
	}

	let all = first.all;
	match first.op {
		BoundSetOpKind::Union => {
			let mut inputs = vec![left];
			inputs.extend(branches.into_iter().map(|(branch_op, _)| branch_op));
			let union_all = LogicalOp::UnionAll(LogicalUnionAll {
				inputs,
			});
			Ok(if all {
				union_all
			} else {
				LogicalOp::Distinct(LogicalDistinct {
					input: Box::new(union_all),
				})
			})
		}
		BoundSetOpKind::Intersect | BoundSetOpKind::Except => {
			// A chain of these is refused above, so there is exactly one right query.
			let Some((right_op, right_targets)) = branches.pop() else {
				return Ok(left);
			};
			let mut on = Vec::new();
			for (i, t) in first_targets.iter().enumerate() {
				let left_name = target_output_name(t);
				let right_name = if i < right_targets.len() {
					target_output_name(&right_targets[i])
				} else {
					left_name.clone()
				};
				let alias = RelationAlias::new(0);
				on.push((LogicalExpr::Field(left_name.into()), alias.access(right_name)));
			}
			let kind = if first.op == BoundSetOpKind::Intersect {
				JoinKind::Semi
			} else {
				JoinKind::Anti
			};
			let join = LogicalOp::Join(LogicalJoin::new(
				left,
				right_op,
				kind,
				on,
				None,
				RelationAlias::new(0),
				EvalFrame::caller(),
			));
			Ok(if all {
				join
			} else {
				LogicalOp::Distinct(LogicalDistinct {
					input: Box::new(join),
				})
			})
		}
	}
}

/// Whether any of `targets` is a wildcard, whose columns are not known at plan time.
fn has_wildcard(targets: &[BoundTarget]) -> bool {
	targets.iter().any(|t| t.is_wildcard || t.table_wildcard.is_some())
}

/// The name a target gives its column in the rows it produces: its alias, the column it reads or
/// the function it calls. `None` where the name is not one of those or the target is a wildcard.
fn column_name(target: &BoundTarget) -> Option<String> {
	if target.is_wildcard || target.table_wildcard.is_some() {
		return None;
	}
	match (&target.alias, &target.expr) {
		(Some(alias), _) => Some(alias.clone()),
		(
			None,
			BoundExpr::Column(BoundColumnRef {
				column,
				..
			}),
		) if column != "*" => Some(column.clone()),
		(
			None,
			BoundExpr::FunctionCall {
				name,
				..
			},
		) => Some(name.clone()),
		(None, _) => None,
	}
}

/// Name the columns of `branch`, a later query of a set operation, as the columns of the first
/// query are.
///
/// A set operation answers rows under the names its first query gives its columns, whatever the
/// later queries call theirs, and a query over a UNION reads them under those names. A column is
/// left as it is where the first query's name for it cannot be told, where the later query's
/// column is a wildcard, or where the two queries do not have the same number of columns.
fn name_as_first_query(first: &[BoundTarget], branch: &mut BoundSelect) {
	if first.len() != branch.targets.len() {
		return;
	}
	for (first, later) in first.iter().zip(&mut branch.targets) {
		let Some(name) = column_name(first) else {
			continue;
		};
		if !later.is_wildcard
			&& later.table_wildcard.is_none()
			&& column_name(later).as_deref() != Some(name.as_str())
		{
			later.alias = Some(name);
		}
	}
}

fn target_output_name(target: &BoundTarget) -> String {
	match &target.alias {
		Some(a) => a.clone(),
		None => match &target.expr {
			BoundExpr::Column(col) if col.column != "*" => col.column.clone(),
			BoundExpr::FunctionCall {
				name,
				..
			} => name.clone(),
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
			}
			| BoundExpr::Column(_) => "col".to_string(),
		},
	}
}

/// Whether `lower_bound_target` names the output of `target` when it has no alias.
fn target_names_itself(target: &BoundTarget) -> bool {
	matches!(&target.expr, BoundExpr::Column(col) if col.column != "*")
		|| matches!(
			&target.expr,
			BoundExpr::FunctionCall { .. } | BoundExpr::UserVar(_) | BoundExpr::SystemVar { .. }
		)
}

fn collect_aggregate_exprs(expr: &BoundExpr, acc: &mut Vec<BoundExpr>) {
	match expr {
		BoundExpr::FunctionCall {
			name,
			args,
			..
		} => {
			// A call that holds an aggregate is one expression of the aggregation, so that its
			// result is named once and read by a HAVING or an ORDER BY as that column.
			let aggregates = crate::functions::FunctionRegistry::is_bound_aggregate(name)
				|| args.iter().any(bound_expr_contains_aggregate);
			if aggregates && !acc.contains(expr) {
				acc.push(expr.clone());
			}
		}
		BoundExpr::Binary {
			left,
			right,
			..
		} => {
			collect_aggregate_exprs(left, acc);
			collect_aggregate_exprs(right, acc);
		}
		BoundExpr::Unary {
			expr,
			..
		} => {
			collect_aggregate_exprs(expr, acc);
		}
		BoundExpr::Access {
			base,
			..
		} => {
			collect_aggregate_exprs(base, acc);
		}
		BoundExpr::Between {
			expr,
			low,
			high,
			..
		} => {
			collect_aggregate_exprs(expr, acc);
			collect_aggregate_exprs(low, acc);
			collect_aggregate_exprs(high, acc);
		}
		BoundExpr::InList {
			expr,
			list,
			..
		} => {
			collect_aggregate_exprs(expr, acc);
			for item in list {
				collect_aggregate_exprs(item, acc);
			}
		}
		BoundExpr::IsNull {
			expr,
			..
		} => {
			collect_aggregate_exprs(expr, acc);
		}
		BoundExpr::Cast {
			expr,
			..
		} => {
			collect_aggregate_exprs(expr, acc);
		}
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => {
			if let Some(op) = operand {
				collect_aggregate_exprs(op, acc);
			}
			for (c, v) in branches {
				collect_aggregate_exprs(c, acc);
				collect_aggregate_exprs(v, acc);
			}
			if let Some(el) = else_expr {
				collect_aggregate_exprs(el, acc);
			}
		}
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Param {
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
		} => {}
	}
}

/// Rewrite the sub-expressions of `expr` that are projected targets to references to the output
/// column of the target, for a clause evaluated over the rows a projection or aggregation
/// produced. A reference to the key of an `id` keeps reading the key when `keep_id` says the
/// output column holds the stored record id.
fn rewrite_aggregates_to_columns(
	expr: &BoundExpr,
	mapping: &[(BoundExpr, String)],
	keep_id: bool,
) -> BoundExpr {
	for (agg, col_name) in mapping {
		if expr == agg {
			return BoundExpr::Column(BoundColumnRef {
				relation_index: 0,
				relation_name: String::new(),
				column: col_name.clone(),
				correlation_depth: 0,
				id_table: id_column(agg).filter(|_| keep_id).map(|(_, table)| table.to_string()),
				link_table: link_column(agg).map(|(_, table)| table.to_string()),
				aggregate_output: keep_id,
				declared_type: match agg {
					BoundExpr::Column(col) => col.declared_type,
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
					} => crate::SqlType::Unknown,
				},
			});
		}
	}
	match expr {
		BoundExpr::Binary {
			left,
			op,
			right,
		} => BoundExpr::Binary {
			left: Box::new(rewrite_aggregates_to_columns(left, mapping, keep_id)),
			op: *op,
			right: Box::new(rewrite_aggregates_to_columns(right, mapping, keep_id)),
		},
		BoundExpr::FunctionCall {
			name,
			args,
			distinct,
		} => BoundExpr::FunctionCall {
			name: name.clone(),
			args: args
				.iter()
				.map(|arg| rewrite_aggregates_to_columns(arg, mapping, keep_id))
				.collect(),
			distinct: *distinct,
		},
		BoundExpr::InSubquery {
			expr,
			subquery,
			negated,
		} => BoundExpr::InSubquery {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping, keep_id)),
			subquery: subquery.clone(),
			negated: *negated,
		},
		BoundExpr::Unary {
			op,
			expr,
		} => BoundExpr::Unary {
			op: *op,
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping, keep_id)),
		},
		BoundExpr::Access {
			base,
			field,
		} => BoundExpr::Access {
			base: Box::new(rewrite_aggregates_to_columns(base, mapping, keep_id)),
			field: field.clone(),
		},
		BoundExpr::Between {
			expr,
			low,
			high,
			negated,
		} => BoundExpr::Between {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping, keep_id)),
			low: Box::new(rewrite_aggregates_to_columns(low, mapping, keep_id)),
			high: Box::new(rewrite_aggregates_to_columns(high, mapping, keep_id)),
			negated: *negated,
		},
		BoundExpr::InList {
			expr,
			list,
			negated,
		} => BoundExpr::InList {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping, keep_id)),
			list: list
				.iter()
				.map(|item| rewrite_aggregates_to_columns(item, mapping, keep_id))
				.collect(),
			negated: *negated,
		},
		BoundExpr::IsNull {
			expr,
			negated,
		} => BoundExpr::IsNull {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping, keep_id)),
			negated: *negated,
		},
		BoundExpr::Cast {
			expr,
			target_type,
		} => BoundExpr::Cast {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping, keep_id)),
			target_type: target_type.clone(),
		},
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => BoundExpr::Case {
			operand: operand
				.as_ref()
				.map(|op| Box::new(rewrite_aggregates_to_columns(op, mapping, keep_id))),
			branches: branches
				.iter()
				.map(|(c, v)| {
					(
						rewrite_aggregates_to_columns(c, mapping, keep_id),
						rewrite_aggregates_to_columns(v, mapping, keep_id),
					)
				})
				.collect(),
			else_expr: else_expr
				.as_ref()
				.map(|el| Box::new(rewrite_aggregates_to_columns(el, mapping, keep_id))),
		},
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Param {
			..
		}
		| BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::ArraySubquery(_)
		| BoundExpr::WindowFunction {
			..
		}
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		} => expr.clone(),
	}
}

fn lower_table_source(
	source: &BoundTableSource,
	for_update: bool,
	join_counter: &mut usize,
	policy: &DialectPolicy,
	user_tables: &[crate::catalog::TableSchema],
) -> Result<LogicalOp, PlanError> {
	match source {
		BoundTableSource::Values => Ok(LogicalOp::Values(LogicalValues {
			expr: LogicalExpr::Array(vec![LogicalExpr::Object(vec![])]),
			for_update,
			written: false,
		})),
		BoundTableSource::Table {
			name,
			schema,
			..
		} => {
			if let Some(rows) =
				crate::catalog::system::get_system_table_rows(schema.as_deref(), name, user_tables)
			{
				return Ok(LogicalOp::Values(LogicalValues {
					expr: LogicalExpr::Array(rows),
					for_update,
					written: false,
				}));
			}
			Ok(LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(TableName::from(name.as_str())),
				schema: None,
				with: None,
				for_update,
			}))
		}
		BoundTableSource::Subquery {
			query,
			..
		} => {
			let inner = lower_bound_select_internal(query, false, false, policy)?;
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
			let left_op = lower_table_source(left, for_update, join_counter, policy, user_tables)?;
			let right_op =
				lower_table_source(right, for_update, join_counter, policy, user_tables)?;

			let kind = match join_type {
				BoundJoinType::Inner | BoundJoinType::Cross => JoinKind::Inner,
				BoundJoinType::Left => JoinKind::Left,
				BoundJoinType::Right | BoundJoinType::Full => {
					return Err(PlanError::UnsupportedStatement(format!(
						"Join type {join_type:?} not supported directly in streaming engine"
					)));
				}
			};

			let alias_idx = *join_counter;
			*join_counter += 1;

			let mut on = Vec::new();
			let mut filter = None;

			if let Some(cond) = condition {
				match cond {
					BoundJoinCondition::On(expr) => {
						let mut equi_pairs = Vec::new();
						let mut residual = Vec::new();
						extract_equi_conditions(expr, &mut equi_pairs, &mut residual);

						for (l, r) in equi_pairs {
							let (probe, build) =
								if expr_max_relation_index(&l) > expr_max_relation_index(&r) {
									(r, l)
								} else {
									(l, r)
								};
							on.push(lower_join_key(&probe, &build, policy)?);
						}

						if !residual.is_empty() {
							let mut combined: Option<LogicalExpr> = None;
							for res in residual {
								let lowered = lower_bound_expr(&res, policy)?;
								combined = match combined {
									Some(c) => Some(LogicalExpr::Binary {
										left: Box::new(c),
										op: BinaryOperator::And,
										right: Box::new(lowered),
									}),
									None => Some(lowered),
								};
							}
							filter = combined;
						}
					}
					BoundJoinCondition::Using(cols) => {
						for col in cols {
							let left_key = LogicalExpr::Field(col.clone().into());
							let right_key = RelationAlias::new(alias_idx).access(col.clone());
							on.push(if col == "id" {
								lower_using_id_key(left, right, left_key, right_key)
							} else {
								(left_key, right_key)
							});
						}
					}
				}
			}

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

/// `expr` with the key read of an `id` column undone: the stored field itself.
fn as_stored(expr: &BoundExpr) -> BoundExpr {
	match expr {
		BoundExpr::Column(col) => BoundExpr::Column(BoundColumnRef {
			id_table: None,
			..col.clone()
		}),
		other @ BoundExpr::Literal(_)
		| other @ BoundExpr::Access {
			..
		}
		| other @ BoundExpr::Param {
			..
		}
		| other @ BoundExpr::Binary {
			..
		}
		| other @ BoundExpr::Unary {
			..
		}
		| other @ BoundExpr::FunctionCall {
			..
		}
		| other @ BoundExpr::Case {
			..
		}
		| other @ BoundExpr::Between {
			..
		}
		| other @ BoundExpr::InList {
			..
		}
		| other @ BoundExpr::IsNull {
			..
		}
		| other @ BoundExpr::Cast {
			..
		}
		| other @ BoundExpr::ScalarSubquery(_)
		| other @ BoundExpr::Exists(_)
		| other @ BoundExpr::InSubquery {
			..
		}
		| other @ BoundExpr::ArraySubquery(_)
		| other @ BoundExpr::WindowFunction {
			..
		}
		| other @ BoundExpr::UserVar(_)
		| other @ BoundExpr::SystemVar {
			..
		} => other.clone(),
	}
}

/// The field that reads the stored record id an aggregation produced as the output `name` as its
/// key, under the same name. The group of the absent id an outer join leaves reads as absent.
fn key_output_field(name: String) -> LogicalField {
	LogicalField::user(
		key_of(LogicalExpr::Field(name.clone().into()), true),
		Some(OutputPath::field(name)),
	)
}

/// Lower a `WHERE` predicate.
///
/// A predicate is judged for truth alone. A top-level conjunct that compares the `id` of a base
/// table with keys is lowered as the lookup of the records the keys name, and its conjuncts are
/// joined by the engine's own `AND` so the engine finds the lookup among them. A predicate without
/// such a conjunct keeps the three-valued `AND`.
fn lower_where(expr: &BoundExpr, policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let mut conjuncts = Vec::new();
	collect_conjuncts(expr, &mut conjuncts);
	let lookups = conjuncts
		.iter()
		.map(|conjunct| lower_lookup_predicate(conjunct, policy))
		.collect::<Result<Vec<_>, _>>()?;
	if lookups.iter().all(Option::is_none) {
		return lower_bound_expr(expr, policy);
	}
	let mut lowered = conjuncts.into_iter().zip(lookups).map(|(conjunct, lookup)| match lookup {
		Some(lookup) => Ok(lookup),
		None => lower_bound_expr(conjunct, policy),
	});
	let Some(first) = lowered.next() else {
		return lower_bound_expr(expr, policy);
	};
	lowered.try_fold(first?, |acc, next| {
		Ok(LogicalExpr::Binary {
			left: Box::new(acc),
			op: BinaryOperator::And,
			right: Box::new(next?),
		})
	})
}

/// The operands of the `AND` chain `expr`, left to right.
fn collect_conjuncts<'a>(expr: &'a BoundExpr, out: &mut Vec<&'a BoundExpr>) {
	match expr {
		BoundExpr::Binary {
			left,
			op: BoundBinaryOp::And,
			right,
		} => {
			collect_conjuncts(left, out);
			collect_conjuncts(right, out);
		}
		other @ BoundExpr::Literal(_)
		| other @ BoundExpr::Column(_)
		| other @ BoundExpr::Access {
			..
		}
		| other @ BoundExpr::Param {
			..
		}
		| other @ BoundExpr::Binary {
			..
		}
		| other @ BoundExpr::Unary {
			..
		}
		| other @ BoundExpr::FunctionCall {
			..
		}
		| other @ BoundExpr::Case {
			..
		}
		| other @ BoundExpr::Between {
			..
		}
		| other @ BoundExpr::InList {
			..
		}
		| other @ BoundExpr::IsNull {
			..
		}
		| other @ BoundExpr::Cast {
			..
		}
		| other @ BoundExpr::ScalarSubquery(_)
		| other @ BoundExpr::Exists(_)
		| other @ BoundExpr::InSubquery {
			..
		}
		| other @ BoundExpr::ArraySubquery(_)
		| other @ BoundExpr::WindowFunction {
			..
		}
		| other @ BoundExpr::UserVar(_)
		| other @ BoundExpr::SystemVar {
			..
		} => out.push(other),
	}
}

/// The base table `source` reads when it is a single base table.
fn base_table_of(source: &BoundTableSource) -> Option<&str> {
	match source {
		BoundTableSource::Table {
			name,
			schema,
			..
		} if !crate::catalog::system::is_system_catalog_table(schema.as_deref(), name) => {
			Some(name.as_str())
		}
		BoundTableSource::Table {
			..
		}
		| BoundTableSource::Subquery {
			..
		}
		| BoundTableSource::Join {
			..
		}
		| BoundTableSource::Values => None,
	}
}

/// The base table `select` reads, when its only source is a single base table.
fn sole_base_table(select: &BoundSelect) -> Option<&str> {
	select.from.as_ref().and_then(base_table_of)
}

/// The base table whose row is the left row of a join over `source`: its leftmost relation.
fn leftmost_base_table(source: &BoundTableSource) -> Option<&str> {
	match source {
		BoundTableSource::Join {
			left,
			..
		} => leftmost_base_table(left),
		other @ BoundTableSource::Table {
			..
		}
		| other @ BoundTableSource::Subquery {
			..
		}
		| other @ BoundTableSource::Values => base_table_of(other),
	}
}

/// Make a wildcard over a base table yield the key of the record id as its `id`, as an explicit
/// `id` column does: the wildcard, `*` or `t.*`, is followed by the key as `id`, which replaces the
/// stored record id the wildcard yields. `last_wildcard` is the position of the last wildcard of
/// the projection, if there is one.
fn read_wildcard_id_as_key(fields: &mut Vec<LogicalField>, last_wildcard: Option<usize>) {
	if let Some(last_all) = last_wildcard {
		fields.insert(
			last_all + 1,
			LogicalField::user(
				key_of(LogicalExpr::Field("id".into()), false),
				Some(OutputPath::field("id".to_string())),
			),
		);
	}
}

/// Lower an equality join key `probe = build`.
///
/// When the build side is the `id` of a base table, it keeps the shape of the stored column and
/// the probe side is turned into the record of that table its value names (a key, or a record link
/// to the row), unless the probe is the `id` of the same table, whose stored record id is compared
/// as it is. When the probe side is the `id` of a base table and the build side a column or an
/// expression, the stored id is compared with the record of its table the build value names, which
/// leaves a build key that is no longer a bare column for the engine to read the build table by an
/// index.
///
/// A link column is joined as stored with the `id` of its target table and with a link column to
/// the same table. A link column on either side of anything else is joined with the record of its
/// target table the other side names. Otherwise the keys are lowered as they are.
fn lower_join_key(
	probe: &BoundExpr,
	build: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<(LogicalExpr, LogicalExpr), PlanError> {
	if let Some((probe_col, build_col)) = same_table_pair(probe, build) {
		return Ok((lower_column_raw(probe_col), lower_column_raw(build_col)));
	}
	if let Some((build_col, build_table)) = id_column(build) {
		if let Some((probe_col, probe_table)) = id_column(probe)
			&& probe_table == build_table
		{
			return Ok((lower_column_raw(probe_col), lower_column_raw(build_col)));
		}
		return Ok((
			record_of(build_table, lower_stored(probe, policy)?),
			lower_column_raw(build_col),
		));
	}
	if let Some((probe_col, probe_table)) = id_column(probe)
		&& is_key_or_link(build)
	{
		return Ok((
			lower_column_raw(probe_col),
			record_of(probe_table, lower_stored(build, policy)?),
		));
	}
	if let Some((build_col, build_table)) = link_column(build)
		&& let LinkOperand::Record(record) = link_operand(build_table, probe, policy)?
	{
		return Ok((record, lower_column_raw(build_col)));
	}
	if let Some((probe_col, probe_table)) = link_column(probe)
		&& let LinkOperand::Record(record) = link_operand(probe_table, build, policy)?
	{
		return Ok((lower_column_raw(probe_col), record));
	}
	Ok((lower_stored(probe, policy)?, lower_stored(build, policy)?))
}

/// Lower the keys of `USING (id)` between `left` and `right`, whose row is read through
/// `right_key`. The key shapes follow the rules of [`lower_join_key`].
fn lower_using_id_key(
	left: &BoundTableSource,
	right: &BoundTableSource,
	left_key: LogicalExpr,
	right_key: LogicalExpr,
) -> (LogicalExpr, LogicalExpr) {
	match (leftmost_base_table(left), base_table_of(right)) {
		(Some(left_table), Some(right_table)) if left_table == right_table => (left_key, right_key),
		(Some(_), Some(right_table)) => {
			(record_of(right_table, key_of(left_key, false)), right_key)
		}
		(None, Some(right_table)) => (record_of(right_table, left_key), right_key),
		(Some(_), None) => (key_of(left_key, false), right_key),
		(None, None) => (left_key, right_key),
	}
}

fn extract_equi_conditions(
	expr: &BoundExpr,
	on: &mut Vec<(BoundExpr, BoundExpr)>,
	residual: &mut Vec<BoundExpr>,
) {
	match expr {
		BoundExpr::Binary {
			left,
			op: BoundBinaryOp::And,
			right,
		} => {
			extract_equi_conditions(left, on, residual);
			extract_equi_conditions(right, on, residual);
		}
		BoundExpr::Binary {
			left,
			op: BoundBinaryOp::Equal,
			right,
		} => {
			on.push((*left.clone(), *right.clone()));
		}
		other @ (BoundExpr::Literal(_)
		| BoundExpr::Column(_)
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
		| BoundExpr::FunctionCall {
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
		}) => {
			residual.push(other.clone());
		}
	}
}

fn expr_max_relation_index(expr: &BoundExpr) -> usize {
	match expr {
		BoundExpr::Column(col) => col.relation_index,
		BoundExpr::Access {
			base,
			..
		} => expr_max_relation_index(base),
		BoundExpr::Binary {
			left,
			right,
			..
		} => expr_max_relation_index(left).max(expr_max_relation_index(right)),
		BoundExpr::Unary {
			expr,
			..
		} => expr_max_relation_index(expr),
		BoundExpr::Literal(_)
		| BoundExpr::Param {
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
		} => 0,
	}
}
