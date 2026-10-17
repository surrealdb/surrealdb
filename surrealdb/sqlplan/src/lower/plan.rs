//! Lowering from BoundSelect to SurrealDB `LogicalOp`.

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
	BoundSelect, BoundSetOpKind, BoundTableSource, BoundTarget,
};
use crate::lower::expr::{
	bound_expr_contains_aggregate, lower_bound_expr, lower_bound_order, lower_bound_target,
};

/// Lower a fully bound SQL `BoundSelect` into a SurrealDB `LogicalOp` execution plan.
pub fn lower_bound_select(
	select: &BoundSelect,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	lower_bound_select_internal(select, false, policy)
}

/// Lower a subquery `BoundSelect` where single-column rows should be emitted as bare values.
pub fn lower_bound_subquery(
	select: &BoundSelect,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	lower_bound_select_internal(select, true, policy)
}

fn lower_bound_select_internal(
	select: &BoundSelect,
	as_value: bool,
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
			predicate: lower_bound_expr(where_expr, policy)?,
		});
	}

	// 3. GROUP BY & Aggregation
	let grouped = !select.group_by.is_empty();
	let has_aggregates = select.targets.iter().any(|t| bound_expr_contains_aggregate(&t.expr));
	let has_having_agg = select.having.as_ref().is_some_and(bound_expr_contains_aggregate);
	let has_order_agg = select.order_by.iter().any(|o| bound_expr_contains_aggregate(&o.expr));
	let is_aggregate =
		grouped || has_aggregates || has_having_agg || has_order_agg || select.having.is_some();

	let mut aggregate_targets = select.targets.clone();
	let mut agg_mapping: Vec<(BoundExpr, String)> = Vec::new();
	for t in &select.targets {
		let col_name = target_output_name(t);
		agg_mapping.push((t.expr.clone(), col_name));
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

	let fields = if as_value && select.targets.len() == 1 && !select.targets[0].is_wildcard {
		let expr = lower_bound_expr(&select.targets[0].expr, policy)?;
		let alias = select.targets[0].alias.as_ref().map(|a| OutputPath::field(a.clone()));
		LogicalFields::Value {
			expr,
			alias,
		}
	} else {
		LogicalFields::Select(
			aggregate_targets
				.iter()
				.map(|t| lower_bound_target(t, policy))
				.collect::<Result<Vec<_>, _>>()?,
		)
	};

	let rewritten_having =
		select.having.as_ref().map(|h| rewrite_aggregates_to_columns(h, &agg_mapping));

	let mut rewritten_order_by = Vec::new();
	for o in &select.order_by {
		rewritten_order_by.push(BoundOrder {
			expr: rewrite_aggregates_to_columns(&o.expr, &agg_mapping),
			direction: o.direction,
			nulls_first: o.nulls_first,
		});
	}

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
	if matches!(fields, LogicalFields::Value { .. }) || !is_aggregate {
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields,
			omit: vec![],
		});
	} else if aggregate_targets.len() > select.targets.len() {
		let project_fields = select
			.targets
			.iter()
			.map(|t| {
				let col_name = target_output_name(t);
				let alias = t.alias.as_ref().map(|a| OutputPath::field(a.clone()));
				LogicalField::user(LogicalExpr::Field(col_name.into()), alias)
			})
			.collect::<Vec<_>>();
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::Select(project_fields),
			omit: vec![],
		});
	} else {
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::all(),
			omit: vec![],
		});
	}

	// 9. Set Operation (UNION, INTERSECT, EXCEPT)
	if let Some(set_op) = &select.set_op {
		let right_op = lower_bound_select(&set_op.right, policy)?;
		match set_op.op {
			BoundSetOpKind::Union => {
				let union_all = LogicalOp::UnionAll(LogicalUnionAll {
					inputs: vec![op, right_op],
				});
				op = if set_op.all {
					union_all
				} else {
					LogicalOp::Distinct(LogicalDistinct {
						input: Box::new(union_all),
					})
				};
			}
			BoundSetOpKind::Intersect => {
				let mut on = Vec::new();
				for (i, t) in select.targets.iter().enumerate() {
					let left_name = target_output_name(t);
					let right_name = if i < set_op.right.targets.len() {
						target_output_name(&set_op.right.targets[i])
					} else {
						left_name.clone()
					};
					let alias = RelationAlias::new(0);
					on.push((LogicalExpr::Field(left_name.into()), alias.access(right_name)));
				}
				let semi_join = LogicalOp::Join(LogicalJoin::new(
					op,
					right_op,
					JoinKind::Semi,
					on,
					None,
					RelationAlias::new(0),
					EvalFrame::caller(),
				));
				op = if set_op.all {
					semi_join
				} else {
					LogicalOp::Distinct(LogicalDistinct {
						input: Box::new(semi_join),
					})
				};
			}
			BoundSetOpKind::Except => {
				let mut on = Vec::new();
				for (i, t) in select.targets.iter().enumerate() {
					let left_name = target_output_name(t);
					let right_name = if i < set_op.right.targets.len() {
						target_output_name(&set_op.right.targets[i])
					} else {
						left_name.clone()
					};
					let alias = RelationAlias::new(0);
					on.push((LogicalExpr::Field(left_name.into()), alias.access(right_name)));
				}
				let anti_join = LogicalOp::Join(LogicalJoin::new(
					op,
					right_op,
					JoinKind::Anti,
					on,
					None,
					RelationAlias::new(0),
					EvalFrame::caller(),
				));
				op = if set_op.all {
					anti_join
				} else {
					LogicalOp::Distinct(LogicalDistinct {
						input: Box::new(anti_join),
					})
				};
			}
		}

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

fn target_output_name(target: &BoundTarget) -> String {
	match &target.alias {
		Some(a) => a.clone(),
		None => match &target.expr {
			BoundExpr::Column(col) if col.column != "*" => col.column.clone(),
			BoundExpr::FunctionCall {
				name,
				..
			} => name.clone(),
			_ => "col".to_string(),
		},
	}
}

fn collect_aggregate_exprs(expr: &BoundExpr, acc: &mut Vec<BoundExpr>) {
	match expr {
		BoundExpr::FunctionCall {
			name,
			..
		} if matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max")
			&& !acc.contains(expr) =>
		{
			acc.push(expr.clone());
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
		_ => {}
	}
}

fn rewrite_aggregates_to_columns(expr: &BoundExpr, mapping: &[(BoundExpr, String)]) -> BoundExpr {
	for (agg, col_name) in mapping {
		if expr == agg {
			return BoundExpr::Column(BoundColumnRef {
				relation_index: 0,
				relation_name: String::new(),
				column: col_name.clone(),
				correlation_depth: 0,
			});
		}
	}
	match expr {
		BoundExpr::Binary {
			left,
			op,
			right,
		} => BoundExpr::Binary {
			left: Box::new(rewrite_aggregates_to_columns(left, mapping)),
			op: *op,
			right: Box::new(rewrite_aggregates_to_columns(right, mapping)),
		},
		BoundExpr::Unary {
			op,
			expr,
		} => BoundExpr::Unary {
			op: *op,
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping)),
		},
		BoundExpr::Access {
			base,
			field,
		} => BoundExpr::Access {
			base: Box::new(rewrite_aggregates_to_columns(base, mapping)),
			field: field.clone(),
		},
		BoundExpr::Between {
			expr,
			low,
			high,
			negated,
		} => BoundExpr::Between {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping)),
			low: Box::new(rewrite_aggregates_to_columns(low, mapping)),
			high: Box::new(rewrite_aggregates_to_columns(high, mapping)),
			negated: *negated,
		},
		BoundExpr::InList {
			expr,
			list,
			negated,
		} => BoundExpr::InList {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping)),
			list: list.iter().map(|item| rewrite_aggregates_to_columns(item, mapping)).collect(),
			negated: *negated,
		},
		BoundExpr::IsNull {
			expr,
			negated,
		} => BoundExpr::IsNull {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping)),
			negated: *negated,
		},
		BoundExpr::Cast {
			expr,
			target_type,
		} => BoundExpr::Cast {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping)),
			target_type: target_type.clone(),
		},
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => BoundExpr::Case {
			operand: operand
				.as_ref()
				.map(|op| Box::new(rewrite_aggregates_to_columns(op, mapping))),
			branches: branches
				.iter()
				.map(|(c, v)| {
					(
						rewrite_aggregates_to_columns(c, mapping),
						rewrite_aggregates_to_columns(v, mapping),
					)
				})
				.collect(),
			else_expr: else_expr
				.as_ref()
				.map(|el| Box::new(rewrite_aggregates_to_columns(el, mapping))),
		},
		_ => expr.clone(),
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
			let left_op = lower_table_source(left, for_update, join_counter, policy, user_tables)?;
			let right_op =
				lower_table_source(right, for_update, join_counter, policy, user_tables)?;

			let kind = match join_type {
				BoundJoinType::Inner | BoundJoinType::Cross => JoinKind::Inner,
				BoundJoinType::Left => JoinKind::Left,
				_ => {
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
							let probe_expr = lower_bound_expr(&probe, policy)?;
							let build_expr = lower_bound_expr(&build, policy)?;
							on.push((probe_expr, build_expr));
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
							on.push((left_key, right_key));
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
		other => {
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
		_ => 0,
	}
}
