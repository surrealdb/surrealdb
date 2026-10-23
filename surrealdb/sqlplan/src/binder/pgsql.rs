//! PostgreSQL AST binder implementation.

use std::sync::Arc;

use surrealdb_pgsql::ast::expr::{Expr, NullsOrder, OrderDirection, QualifiedName};
use surrealdb_pgsql::ast::literal::Literal as PgLiteral;
use surrealdb_pgsql::ast::stmt::{
	DistinctClause, JoinCondition, JoinType, SelectStmt, SetOpKind, TableRef,
};

use crate::catalog::BinderCatalog;
use crate::dialect::{DialectPolicy, SqlDialect};
use crate::error::PlanError;
use crate::functions::{FunctionRegistry, SqlFunctionKind};
use crate::ir::{
	BoundBinaryOp, BoundColumnRef, BoundCte, BoundDistinct, BoundExpr, BoundJoinCondition,
	BoundJoinType, BoundLiteral, BoundOrder, BoundOrderDirection, BoundSelect, BoundSetOpKind,
	BoundSetOperation, BoundTableSource, BoundTarget, BoundUnaryOp, BoundWindowDef,
	BoundWindowFrame, BoundWindowFrameBound, BoundWindowFrameExclusion, BoundWindowFrameUnits,
	BoundWindowSpec, BoundWithClause,
};
use crate::scope::QueryScope;

/// Bind a PostgreSQL `SelectStmt` into a `BoundSelect` using the given catalog and policy.
pub fn bind_select(
	select: &SelectStmt,
	catalog: &dyn BinderCatalog,
	policy: &DialectPolicy,
	parent_scope: Option<Arc<QueryScope>>,
) -> Result<BoundSelect, PlanError> {
	let mut scope = if let Some(parent) = parent_scope {
		QueryScope::with_parent(parent)
	} else {
		QueryScope::new()
	};

	// 0. Bind WITH clause (CTEs) if present
	let bound_with = if let Some(with_clause) = &select.with {
		let mut bound_ctes = Vec::new();
		for cte in &with_clause.ctes {
			let surrealdb_pgsql::ast::stmt::Statement::Select(sub_select) = cte.query.as_ref()
			else {
				return Err(PlanError::UnsupportedStatement(
					"Only SELECT queries are supported inside CTE definitions".into(),
				));
			};

			if with_clause.recursive && select_contains_table_reference(sub_select, &cte.name) {
				return Err(PlanError::UnsupportedStatement(
					"Recursive CTE self-reference is not supported in streaming engine".into(),
				));
			}

			let mut bound_query =
				bind_select(sub_select, catalog, policy, Some(Arc::new(scope.clone())))?;

			if !cte.columns.is_empty() {
				if cte.columns.len() != bound_query.targets.len() {
					return Err(PlanError::Custom(format!(
						"CTE '{}' specifies {} column aliases, but query returns {} columns",
						cte.name,
						cte.columns.len(),
						bound_query.targets.len()
					)));
				}
				for (i, col_name) in cte.columns.iter().enumerate() {
					bound_query.targets[i].alias = Some(col_name.clone());
				}
			} else {
				for (i, t) in bound_query.targets.iter_mut().enumerate() {
					if t.alias.is_none() {
						let name = if let BoundExpr::Column(c) = &t.expr {
							c.column.clone()
						} else {
							format!("col_{}", i + 1)
						};
						t.alias = Some(name);
					}
				}
			}

			let bound_cte = BoundCte {
				name: cte.name.clone(),
				columns: cte.columns.clone(),
				materialized: cte.materialized,
				query: Box::new(bound_query),
			};

			scope.register_cte(bound_cte.clone(), policy);
			bound_ctes.push(bound_cte);
		}

		Some(BoundWithClause {
			recursive: with_clause.recursive,
			ctes: bound_ctes,
		})
	} else {
		None
	};

	// 1. Bind FROM clause and register tables in scope
	let from_source = if select.from.is_empty() {
		Some(BoundTableSource::Values)
	} else if select.from.len() == 1 {
		Some(bind_table_ref(&select.from[0], &mut scope, catalog, policy)?)
	} else {
		// Implicit comma-separated joins: t1, t2 -> t1 CROSS JOIN t2
		let mut current = bind_table_ref(&select.from[0], &mut scope, catalog, policy)?;
		for table in &select.from[1..] {
			let right = bind_table_ref(table, &mut scope, catalog, policy)?;
			current = BoundTableSource::Join {
				left: Box::new(current),
				join_type: BoundJoinType::Cross,
				right: Box::new(right),
				condition: None,
			};
		}
		Some(current)
	};

	// 2. Bind WINDOW clause definitions and register in scope
	let mut bound_windows = Vec::new();
	for win in &select.window {
		let spec = bind_window_spec(&win.spec, &scope, catalog, policy)?;
		scope.register_window(&win.name, spec.clone(), policy);
		bound_windows.push(BoundWindowDef {
			name: win.name.clone(),
			spec,
		});
	}

	// 3. Bind projection target list
	let mut targets = Vec::new();
	for target in &select.targets {
		if let Expr::ColumnRef(col) = &target.expr {
			if col.fields.len() == 1 && col.fields[0] == "*" {
				targets.extend(scope.expand_wildcard(catalog));
				continue;
			} else if col.fields.len() == 2 && col.fields[1] == "*" {
				targets.extend(scope.expand_table_wildcard(&col.fields[0], policy, catalog)?);
				continue;
			}
		}

		let bound_expr = bind_expr(&target.expr, &scope, catalog, policy)?;
		let alias = target.alias.clone().map(|a| policy.fold_identifier(&a).into_owned());
		targets.push(BoundTarget {
			expr: bound_expr,
			alias,
			is_wildcard: false,
			table_wildcard: None,
		});
	}

	// Register targets in scope for positional GROUP BY / ORDER BY resolution
	scope.targets.clone_from(&targets);

	// 4. Bind WHERE clause (window functions forbidden)
	let where_clause = select
		.where_clause
		.as_ref()
		.map(|e| {
			let bound = bind_expr(e, &scope, catalog, policy)?;
			if bound.contains_window() {
				return Err(PlanError::WindowNotAllowed("WHERE clause".into()));
			}
			Ok(bound)
		})
		.transpose()?;

	// 5. Bind GROUP BY (window functions forbidden)
	let mut group_by = Vec::new();
	for g in &select.group_by {
		let bound = if let Expr::Literal {
			lit: PgLiteral::Integer(pos),
			..
		} = g
		{
			if policy.group_by_ordinal {
				scope.resolve_group_by_ordinal(*pos as usize)?
			} else {
				return Err(PlanError::Custom("GROUP BY ordinal not allowed".into()));
			}
		} else if let Expr::ColumnRef(col) = g
			&& col.fields.len() == 1
			&& policy.group_by_alias
			&& let Some(target_expr) = scope.find_target_by_alias(&col.fields[0], policy)
		{
			target_expr
		} else {
			bind_expr(g, &scope, catalog, policy)?
		};
		if bound.contains_window() {
			return Err(PlanError::WindowNotAllowed("GROUP BY clause".into()));
		}
		group_by.push(bound);
	}

	// 6. Bind HAVING (window functions forbidden)
	let having = select
		.having
		.as_ref()
		.map(|e| {
			let bound = bind_expr(e, &scope, catalog, policy)?;
			if bound.contains_window() {
				return Err(PlanError::WindowNotAllowed("HAVING clause".into()));
			}
			Ok(bound)
		})
		.transpose()?;

	// 7. Bind ORDER BY
	let mut order_by = Vec::new();
	for item in &select.order_by {
		let expr = if let Expr::Literal {
			lit: PgLiteral::Integer(pos),
			..
		} = &item.expr
		{
			if policy.order_by_ordinal {
				scope.resolve_order_by_ordinal(*pos as usize)?
			} else {
				return Err(PlanError::Custom("ORDER BY ordinal not allowed".into()));
			}
		} else if let Expr::ColumnRef(col) = &item.expr
			&& col.fields.len() == 1
			&& let Some(target_expr) = scope.find_target_by_alias(&col.fields[0], policy)
		{
			target_expr
		} else {
			bind_expr(&item.expr, &scope, catalog, policy)?
		};

		let direction = match item.direction {
			Some(OrderDirection::Desc) => BoundOrderDirection::Desc,
			_ => BoundOrderDirection::Asc,
		};

		let nulls_first = match item.nulls {
			Some(NullsOrder::First) => true,
			Some(NullsOrder::Last) => false,
			None => match direction {
				BoundOrderDirection::Asc => false, // PG ASC NULLS LAST
				BoundOrderDirection::Desc => true, // PG DESC NULLS FIRST
			},
		};

		order_by.push(BoundOrder {
			expr,
			direction,
			nulls_first,
		});
	}

	// 7. Bind LIMIT and OFFSET
	let limit = select.limit.as_ref().map(|e| bind_expr(e, &scope, catalog, policy)).transpose()?;
	let offset =
		select.offset.as_ref().map(|e| bind_expr(e, &scope, catalog, policy)).transpose()?;

	// 8. Bind DISTINCT
	let distinct = select
		.distinct
		.as_ref()
		.map(|d| match d {
			DistinctClause::All => Ok(BoundDistinct::All),
			DistinctClause::On(exprs) => {
				let bound_exprs = exprs
					.iter()
					.map(|e| bind_expr(e, &scope, catalog, policy))
					.collect::<Result<Vec<_>, _>>()?;
				Ok(BoundDistinct::On(bound_exprs))
			}
		})
		.transpose()?;

	let for_update = !select.locking.is_empty();

	let set_op = match &select.set_op {
		Some(so) => {
			let bound_right = bind_select(&so.right, catalog, policy, None)?;
			let op = match so.op {
				SetOpKind::Union => BoundSetOpKind::Union,
				SetOpKind::Intersect => BoundSetOpKind::Intersect,
				SetOpKind::Except => BoundSetOpKind::Except,
			};
			Some(BoundSetOperation {
				op,
				all: so.all,
				right: Box::new(bound_right),
			})
		}
		None => None,
	};

	Ok(BoundSelect {
		with: bound_with,
		from: from_source,
		where_clause,
		targets,
		group_by,
		having,
		windows: bound_windows,
		order_by,
		limit,
		offset,
		distinct,
		set_op,
		for_update,
		user_tables: catalog.all_tables(),
	})
}

/// The name `target` gives its column: its alias, or the column it reads.
fn output_name(target: &BoundTarget) -> Option<&str> {
	match (&target.alias, &target.expr) {
		(Some(alias), _) => Some(alias),
		(
			None,
			BoundExpr::Column(BoundColumnRef {
				column,
				..
			}),
		) if column != "*" => Some(column),
		_ => None,
	}
}

/// Names the columns of a derived table as its alias lists them, `(SELECT ...) AS v(a, b)`: the
/// first columns are renamed in order, and a column the list does not reach keeps its name.
///
/// Fails where the list cannot be applied unambiguously: the query selects `*`, the list names
/// more columns than the query has, or a name is used by two columns.
fn name_derived_columns(
	query: &mut BoundSelect,
	table: &str,
	columns: &[String],
	policy: &DialectPolicy,
) -> Result<(), PlanError> {
	if columns.is_empty() {
		return Ok(());
	}
	if query.targets.iter().any(|t| t.is_wildcard || t.table_wildcard.is_some()) {
		return Err(PlanError::UnsupportedStatement(format!(
			"a column alias list on \"{table}\", whose query selects `*`"
		)));
	}
	if columns.len() > query.targets.len() {
		return Err(PlanError::Custom(format!(
			"table \"{table}\" has {} columns available but {} columns specified",
			query.targets.len(),
			columns.len()
		)));
	}
	let names: Vec<String> =
		columns.iter().map(|name| policy.fold_identifier(name).into_owned()).collect();
	let mut seen = std::collections::HashSet::new();
	let unreached = query.targets[names.len()..].iter().filter_map(output_name);
	for name in names.iter().map(String::as_str).chain(unreached) {
		if !seen.insert(name) {
			return Err(PlanError::Custom(format!(
				"column name \"{name}\" is used by more than one column of table \"{table}\""
			)));
		}
	}
	for (target, name) in query.targets.iter_mut().zip(&names) {
		target.alias = Some(name.clone());
	}
	Ok(())
}

/// The names of the columns `query` answers, where each is known.
fn derived_column_names(query: &BoundSelect) -> Option<Vec<String>> {
	query.targets.iter().map(|t| output_name(t).map(str::to_owned)).collect()
}

fn bind_table_ref(
	table: &TableRef,
	scope: &mut QueryScope,
	catalog: &dyn BinderCatalog,
	policy: &DialectPolicy,
) -> Result<BoundTableSource, PlanError> {
	match table {
		TableRef::Table {
			name,
			alias,
			..
		} => {
			let raw_name = name.parts.last().map(|s| s.as_str()).unwrap_or_default();
			if name.parts.len() == 1
				&& let Some(cte) = scope.get_cte(raw_name, policy).cloned()
			{
				let exposed_name =
					alias.as_ref().map(|a| a.name.clone()).unwrap_or_else(|| cte.name.clone());

				let mut cte_query = cte.query;
				let alias_list = alias.as_ref().map(|a| a.columns.as_slice()).unwrap_or_default();
				name_derived_columns(&mut cte_query, &exposed_name, alias_list, policy)?;

				let columns =
					if cte_query.targets.iter().any(crate::ir::BoundTarget::is_unlisted_wildcard) {
						None
					} else if !alias_list.is_empty() {
						derived_column_names(&cte_query)
					} else if !cte.columns.is_empty() {
						Some(cte.columns)
					} else {
						Some(
							cte_query
								.targets
								.iter()
								.enumerate()
								.map(|(i, t)| {
									t.alias.clone().unwrap_or_else(|| {
										if let BoundExpr::Column(c) = &t.expr {
											c.column.clone()
										} else {
											format!("col_{}", i + 1)
										}
									})
								})
								.collect(),
						)
					};

				let relation_index =
					scope.register_subquery_with_columns(&exposed_name, columns, policy);
				scope.relations[relation_index].source_query = Some(Arc::from(cte_query.clone()));

				return Ok(BoundTableSource::Subquery {
					query: cte_query,
					relation_index,
					alias: exposed_name,
				});
			}
			if alias.as_ref().is_some_and(|a| !a.columns.is_empty()) {
				return Err(PlanError::UnsupportedStatement(
					"a column alias list on a table: the table's columns have no declared order"
						.into(),
				));
			}

			let (schema, table_name) = resolve_table_qualified_name(name, policy, Some(catalog))?;
			let alias_name = alias.as_ref().map(|a| a.name.clone());

			let relation_index = scope.register_relation(
				schema.clone(),
				&table_name,
				alias_name.clone(),
				catalog,
				policy,
			);

			let exposed_name = alias_name.unwrap_or_else(|| table_name.clone());

			Ok(BoundTableSource::Table {
				schema,
				name: table_name.clone(),
				relation_index,
				alias: exposed_name,
			})
		}
		TableRef::Function {
			function,
			alias,
			with_ordinality,
			..
		} => {
			let Expr::FunctionCall {
				name,
				distinct: false,
				filter: None,
				over: None,
				..
			} = function.as_ref()
			else {
				return Err(PlanError::UnsupportedExpression(
					"FROM requires a scalar function without aggregate or window decorations"
						.into(),
				));
			};
			let bound = bind_expr(function, &QueryScope::new(), catalog, policy)?;
			let BoundExpr::FunctionCall {
				name: function_name,
				..
			} = &bound
			else {
				return Err(PlanError::UnsupportedExpression("non-scalar function in FROM".into()));
			};
			let definition = FunctionRegistry::lookup(function_name, policy.dialect)
				.ok_or_else(|| PlanError::UndefinedFunction(function_name.clone()))?;
			if !definition.is_scalar() || function_name == "generate_series" {
				return Err(PlanError::UnsupportedStatement(
					"aggregate and set-returning functions in FROM are unsupported".into(),
				));
			}
			let default_name = name
				.parts
				.last()
				.cloned()
				.ok_or_else(|| PlanError::UndefinedFunction(String::new()))?;
			let exposed = alias
				.as_ref()
				.map(|alias| alias.name.clone())
				.unwrap_or_else(|| default_name.clone());
			let count = if *with_ordinality {
				2
			} else {
				1
			};
			let aliases = alias.as_ref().map(|alias| alias.columns.as_slice()).unwrap_or(&[]);
			if aliases.len() > count {
				return Err(PlanError::UnsupportedExpression(
					"too many column aliases for scalar function in FROM".into(),
				));
			}
			let first_name = aliases.first().cloned().unwrap_or_else(|| exposed.clone());
			let mut targets = vec![BoundTarget {
				expr: bound,
				alias: Some(first_name.clone()),
				is_wildcard: false,
				table_wildcard: None,
			}];
			let mut columns = vec![first_name];
			if *with_ordinality {
				let name = aliases.get(1).cloned().unwrap_or_else(|| "ordinality".into());
				if columns.contains(&name) {
					return Err(PlanError::UnsupportedExpression(
						"duplicate function output names; use distinct aliases".into(),
					));
				}
				columns.push(name.clone());
				targets.push(BoundTarget {
					expr: BoundExpr::Literal(BoundLiteral::Integer(1)),
					alias: Some(name),
					is_wildcard: false,
					table_wildcard: None,
				});
			}
			let query = BoundSelect {
				with: None,
				from: None,
				where_clause: None,
				targets,
				group_by: Vec::new(),
				having: None,
				windows: Vec::new(),
				order_by: Vec::new(),
				limit: None,
				offset: None,
				distinct: None,
				set_op: None,
				for_update: false,
				user_tables: Vec::new(),
			};
			let relation_index =
				scope.register_subquery_with_columns(&exposed, Some(columns), policy);
			scope.relations[relation_index].source_query = Some(Arc::new(query.clone()));
			Ok(BoundTableSource::Subquery {
				query: Box::new(query),
				relation_index,
				alias: exposed,
			})
		}
		TableRef::Subquery {
			query,
			alias,
			..
		} => {
			let sub_alias = alias.name.clone();
			let mut bound_sub = bind_select(query, catalog, policy, scope.parent.clone())?;
			name_derived_columns(&mut bound_sub, &sub_alias, &alias.columns, policy)?;
			let relation_index = if alias.columns.is_empty() {
				scope.register_subquery(&sub_alias, policy)
			} else {
				scope.register_subquery_with_columns(
					&sub_alias,
					derived_column_names(&bound_sub),
					policy,
				)
			};
			scope.relations[relation_index].source_query = Some(Arc::new(bound_sub.clone()));

			Ok(BoundTableSource::Subquery {
				query: Box::new(bound_sub),
				relation_index,
				alias: sub_alias,
			})
		}
		TableRef::Join {
			left,
			join_type,
			right,
			condition,
			..
		} => {
			if *join_type == JoinType::Right {
				let bound_left = bind_table_ref(right, scope, catalog, policy)?;
				let bound_right = bind_table_ref(left, scope, catalog, policy)?;
				let bound_condition = match condition {
					Some(JoinCondition::On(expr)) => {
						Some(BoundJoinCondition::On(bind_expr(expr, scope, catalog, policy)?))
					}
					Some(JoinCondition::Using(cols)) => {
						Some(BoundJoinCondition::Using(cols.clone()))
					}
					None => None,
				};
				return Ok(BoundTableSource::Join {
					left: Box::new(bound_left),
					join_type: BoundJoinType::Left,
					right: Box::new(bound_right),
					condition: bound_condition,
				});
			}

			let bound_left = bind_table_ref(left, scope, catalog, policy)?;
			let bound_right = bind_table_ref(right, scope, catalog, policy)?;

			let bound_join_type = match join_type {
				JoinType::Inner => BoundJoinType::Inner,
				JoinType::Left => BoundJoinType::Left,
				JoinType::Right => BoundJoinType::Right,
				JoinType::Full => BoundJoinType::Full,
				JoinType::Cross => BoundJoinType::Cross,
			};

			let bound_condition = match condition {
				Some(JoinCondition::On(expr)) => {
					Some(BoundJoinCondition::On(bind_expr(expr, scope, catalog, policy)?))
				}
				Some(JoinCondition::Using(cols)) => Some(BoundJoinCondition::Using(cols.clone())),
				None => None,
			};

			Ok(BoundTableSource::Join {
				left: Box::new(bound_left),
				join_type: bound_join_type,
				right: Box::new(bound_right),
				condition: bound_condition,
			})
		}
		TableRef::GraphTable {
			..
		}
		| TableRef::JsonTable(_) => Err(PlanError::UnsupportedStatement(
			"GraphTable / JsonTable in FROM not supported".into(),
		)),
	}
}

/// Resolves a QualifiedName into (schema, table_name).
///
/// In PostgreSQL:
/// - `users` -> `(None, "users")`
/// - `public.users` -> `(Some("public"), "users")` (public schema elided to plain table name)
/// - `myschema.users` -> `(Some("myschema"), "myschema.users")` (namespaced table per D4)
pub fn resolve_table_qualified_name(
	name: &QualifiedName,
	policy: &DialectPolicy,
	catalog: Option<&dyn BinderCatalog>,
) -> Result<(Option<String>, String), PlanError> {
	let folded_parts: Vec<String> =
		name.parts.iter().map(|p| policy.fold_identifier(p).into_owned()).collect();
	crate::namespace::resolve_pgsql_table_name(&folded_parts, catalog)
}

/// Bind a PostgreSQL expression into BoundExpr.
pub fn bind_expr(
	expr: &Expr,
	scope: &QueryScope,
	catalog: &dyn BinderCatalog,
	policy: &DialectPolicy,
) -> Result<BoundExpr, PlanError> {
	match expr {
		Expr::Literal {
			lit,
			..
		} => match lit {
			PgLiteral::Integer(i) => Ok(BoundExpr::Literal(BoundLiteral::Integer(*i))),
			PgLiteral::Float(f) => Ok(BoundExpr::Literal(BoundLiteral::Float(*f))),
			PgLiteral::String(s) => Ok(BoundExpr::Literal(BoundLiteral::String(s.clone()))),
			PgLiteral::Boolean(b) => Ok(BoundExpr::Literal(BoundLiteral::Boolean(*b))),
			PgLiteral::Null => Ok(BoundExpr::Literal(BoundLiteral::Null)),
			_ => Ok(BoundExpr::Literal(BoundLiteral::Null)),
		},
		Expr::ColumnRef(col) => {
			let bound_col = scope.resolve_column(&col.fields, policy, catalog)?;
			Ok(BoundExpr::Column(bound_col))
		}
		Expr::Param {
			id,
			..
		} => {
			let idx = *id as usize;
			let name = surrealdb_types::positional_param_name(idx);
			Ok(BoundExpr::Param {
				index: idx,
				name,
			})
		}
		Expr::UnaryOp {
			op,
			expr,
			..
		} => {
			let inner = bind_expr(expr, scope, catalog, policy)?;
			let bound_op = match op {
				surrealdb_pgsql::ast::op::UnaryOperator::Not => BoundUnaryOp::Not,
				surrealdb_pgsql::ast::op::UnaryOperator::Plus => BoundUnaryOp::Positive,
				surrealdb_pgsql::ast::op::UnaryOperator::Minus => BoundUnaryOp::Negate,
				surrealdb_pgsql::ast::op::UnaryOperator::BitNot
				| surrealdb_pgsql::ast::op::UnaryOperator::Custom(_) => {
					return Err(PlanError::UnsupportedExpression(format!("Unary op {op:?}")));
				}
			};
			Ok(BoundExpr::Unary {
				op: bound_op,
				expr: Box::new(inner),
			})
		}
		Expr::BinaryOp {
			left,
			op,
			right,
			..
		} => {
			let l = bind_expr(left, scope, catalog, policy)?;
			let r = bind_expr(right, scope, catalog, policy)?;
			let bound_op = match op {
				surrealdb_pgsql::ast::op::BinaryOperator::Add => BoundBinaryOp::Add,
				surrealdb_pgsql::ast::op::BinaryOperator::Sub => BoundBinaryOp::Subtract,
				surrealdb_pgsql::ast::op::BinaryOperator::Mul => BoundBinaryOp::Multiply,
				surrealdb_pgsql::ast::op::BinaryOperator::Div => BoundBinaryOp::Divide,
				surrealdb_pgsql::ast::op::BinaryOperator::Mod => BoundBinaryOp::Remainder,
				surrealdb_pgsql::ast::op::BinaryOperator::Eq => BoundBinaryOp::Equal,
				surrealdb_pgsql::ast::op::BinaryOperator::NotEq => BoundBinaryOp::NotEqual,
				surrealdb_pgsql::ast::op::BinaryOperator::Lt => BoundBinaryOp::LessThan,
				surrealdb_pgsql::ast::op::BinaryOperator::LtEq => BoundBinaryOp::LessThanEqual,
				surrealdb_pgsql::ast::op::BinaryOperator::Gt => BoundBinaryOp::GreaterThan,
				surrealdb_pgsql::ast::op::BinaryOperator::GtEq => BoundBinaryOp::GreaterThanEqual,
				surrealdb_pgsql::ast::op::BinaryOperator::And => BoundBinaryOp::And,
				surrealdb_pgsql::ast::op::BinaryOperator::Or => BoundBinaryOp::Or,
				surrealdb_pgsql::ast::op::BinaryOperator::Concat => BoundBinaryOp::Concat,
				surrealdb_pgsql::ast::op::BinaryOperator::Like => BoundBinaryOp::Like,
				surrealdb_pgsql::ast::op::BinaryOperator::NotLike => {
					return Ok(BoundExpr::Unary {
						op: BoundUnaryOp::Not,
						expr: Box::new(BoundExpr::Binary {
							left: Box::new(l),
							op: BoundBinaryOp::Like,
							right: Box::new(r),
						}),
					});
				}
				surrealdb_pgsql::ast::op::BinaryOperator::ILike => BoundBinaryOp::ILike,
				surrealdb_pgsql::ast::op::BinaryOperator::NotILike => {
					return Ok(BoundExpr::Unary {
						op: BoundUnaryOp::Not,
						expr: Box::new(BoundExpr::Binary {
							left: Box::new(l),
							op: BoundBinaryOp::ILike,
							right: Box::new(r),
						}),
					});
				}
				surrealdb_pgsql::ast::op::BinaryOperator::RegexMatch => BoundBinaryOp::Regexp,
				surrealdb_pgsql::ast::op::BinaryOperator::RegexMatchCi => BoundBinaryOp::RegexpCi,
				surrealdb_pgsql::ast::op::BinaryOperator::RegexNotMatch => {
					return Ok(BoundExpr::Unary {
						op: BoundUnaryOp::Not,
						expr: Box::new(BoundExpr::Binary {
							left: Box::new(l),
							op: BoundBinaryOp::Regexp,
							right: Box::new(r),
						}),
					});
				}
				surrealdb_pgsql::ast::op::BinaryOperator::RegexNotMatchCi => {
					return Ok(BoundExpr::Unary {
						op: BoundUnaryOp::Not,
						expr: Box::new(BoundExpr::Binary {
							left: Box::new(l),
							op: BoundBinaryOp::RegexpCi,
							right: Box::new(r),
						}),
					});
				}
				surrealdb_pgsql::ast::op::BinaryOperator::Exp => BoundBinaryOp::Power,
				surrealdb_pgsql::ast::op::BinaryOperator::Arrow => BoundBinaryOp::JsonExtract,
				surrealdb_pgsql::ast::op::BinaryOperator::LongArrow => {
					BoundBinaryOp::JsonExtractText
				}
				surrealdb_pgsql::ast::op::BinaryOperator::HashLongArrow => {
					BoundBinaryOp::JsonPathExtractText
				}
				surrealdb_pgsql::ast::op::BinaryOperator::Contains => BoundBinaryOp::JsonContains,
				surrealdb_pgsql::ast::op::BinaryOperator::BitAnd
				| surrealdb_pgsql::ast::op::BinaryOperator::BitOr
				| surrealdb_pgsql::ast::op::BinaryOperator::BitXor
				| surrealdb_pgsql::ast::op::BinaryOperator::ShiftLeft
				| surrealdb_pgsql::ast::op::BinaryOperator::ShiftRight
				| surrealdb_pgsql::ast::op::BinaryOperator::SimilarTo
				| surrealdb_pgsql::ast::op::BinaryOperator::NotSimilarTo
				| surrealdb_pgsql::ast::op::BinaryOperator::HashArrow
				| surrealdb_pgsql::ast::op::BinaryOperator::ContainedBy
				| surrealdb_pgsql::ast::op::BinaryOperator::Custom(_) => {
					return Err(PlanError::UnsupportedExpression(format!("Binary op {op:?}")));
				}
			};
			Ok(BoundExpr::Binary {
				left: Box::new(l),
				op: bound_op,
				right: Box::new(r),
			})
		}
		Expr::FunctionCall {
			name,
			args,
			distinct,
			filter,
			over,
			..
		} => {
			let fn_name =
				if name.parts.len() == 2 && name.parts[0].eq_ignore_ascii_case("pg_catalog") {
					name.parts[1].to_ascii_lowercase()
				} else {
					name.parts.last().map(|s| s.to_ascii_lowercase()).unwrap_or_default()
				};
			let func_def = FunctionRegistry::lookup(&fn_name, SqlDialect::Postgres)
				.ok_or_else(|| PlanError::UndefinedFunction(fn_name.clone()))?;

			// Validate arity
			if args.len() < func_def.min_args {
				return Err(PlanError::FunctionArgumentCountMismatch {
					name: fn_name.clone(),
					expected: format!("at least {}", func_def.min_args),
					got: args.len(),
				});
			}
			if let Some(max_args) = func_def.max_args
				&& args.len() > max_args
			{
				return Err(PlanError::FunctionArgumentCountMismatch {
					name: fn_name.clone(),
					expected: format!("at most {}", max_args),
					got: args.len(),
				});
			}

			if over.is_none() && func_def.kind == SqlFunctionKind::Window {
				return Err(PlanError::WindowFunctionWithoutOver(fn_name.clone()));
			}
			if over.is_some() && func_def.kind == SqlFunctionKind::Scalar {
				return Err(PlanError::WindowNotAllowed(format!("scalar function '{fn_name}'")));
			}

			let bound_args = if let Some(cond) = filter {
				let bound_cond = bind_expr(cond, scope, catalog, policy)?;
				if fn_name == "count"
					&& (args.is_empty()
						|| (args.len() == 1
							&& matches!(&args[0], Expr::ColumnRef(col) if col.fields.len() == 1 && col.fields[0] == "*")))
				{
					// COUNT(*) FILTER (WHERE cond) -> count(CASE WHEN cond THEN 1 ELSE NULL END)
					vec![BoundExpr::Case {
						operand: None,
						branches: vec![(bound_cond, BoundExpr::Literal(BoundLiteral::Integer(1)))],
						else_expr: Some(Box::new(BoundExpr::Literal(BoundLiteral::Null))),
					}]
				} else if args.len() == 1
					&& matches!(fn_name.as_str(), "array_agg" | "json_agg" | "jsonb_agg")
				{
					// These keep the NULLs the rewrite below would leave for every filtered row.
					return Err(PlanError::UnsupportedExpression(format!(
						"FILTER (WHERE ...) on {}",
						fn_name.to_uppercase()
					)));
				} else if args.len() == 1 {
					let bound_arg = bind_expr(&args[0], scope, catalog, policy)?;
					// AGG(arg) FILTER (WHERE cond) -> agg(CASE WHEN cond THEN arg ELSE NULL END)
					vec![BoundExpr::Case {
						operand: None,
						branches: vec![(bound_cond, bound_arg)],
						else_expr: Some(Box::new(BoundExpr::Literal(BoundLiteral::Null))),
					}]
				} else {
					return Err(PlanError::UnsupportedExpression(
						"FILTER (WHERE ...) only supported on single-argument aggregates".into(),
					));
				}
			} else if fn_name == "count"
				&& args.len() == 1
				&& matches!(&args[0], Expr::ColumnRef(col) if col.fields.len() == 1 && col.fields[0] == "*")
			{
				vec![BoundExpr::Column(BoundColumnRef {
					relation_index: 0,
					relation_name: String::new(),
					column: "*".to_string(),
					correlation_depth: 0,
					id_table: None,
					link_table: None,
					declared_type: crate::SqlType::Unknown,
					aggregate_output: false,
				})]
			} else {
				args.iter()
					.map(|a| bind_expr(a, scope, catalog, policy))
					.collect::<Result<Vec<_>, _>>()?
			};

			if let Some(spec) = over {
				if bound_args.iter().any(|a| a.contains_window()) {
					return Err(PlanError::WindowNotAllowed("window function arguments".into()));
				}
				let bound_spec = bind_window_spec(spec, scope, catalog, policy)?;
				Ok(BoundExpr::WindowFunction {
					name: fn_name,
					args: bound_args,
					distinct: *distinct,
					spec: bound_spec,
				})
			} else {
				Ok(BoundExpr::FunctionCall {
					name: fn_name,
					args: bound_args,
					distinct: *distinct,
				})
			}
		}
		Expr::Case {
			operand,
			branches,
			else_expr,
			..
		} => {
			let bound_operand = operand
				.as_ref()
				.map(|op| bind_expr(op, scope, catalog, policy))
				.transpose()?
				.map(Box::new);
			let mut bound_branches = Vec::new();
			for when in branches {
				let cond = bind_expr(&when.when, scope, catalog, policy)?;
				let val = bind_expr(&when.then, scope, catalog, policy)?;
				bound_branches.push((cond, val));
			}
			let bound_else = else_expr
				.as_ref()
				.map(|el| bind_expr(el, scope, catalog, policy))
				.transpose()?
				.map(Box::new);
			Ok(BoundExpr::Case {
				operand: bound_operand,
				branches: bound_branches,
				else_expr: bound_else,
			})
		}
		Expr::Between {
			expr,
			low,
			high,
			negated,
			..
		} => {
			let val = bind_expr(expr, scope, catalog, policy)?;
			let l = bind_expr(low, scope, catalog, policy)?;
			let h = bind_expr(high, scope, catalog, policy)?;
			Ok(BoundExpr::Between {
				expr: Box::new(val),
				low: Box::new(l),
				high: Box::new(h),
				negated: *negated,
			})
		}
		Expr::InList {
			expr,
			list,
			negated,
			..
		} => {
			let val = bind_expr(expr, scope, catalog, policy)?;
			let items = list
				.iter()
				.map(|item| bind_expr(item, scope, catalog, policy))
				.collect::<Result<Vec<_>, _>>()?;
			Ok(BoundExpr::InList {
				expr: Box::new(val),
				list: items,
				negated: *negated,
			})
		}
		Expr::IsNull {
			expr,
			negated,
			..
		} => {
			let val = bind_expr(expr, scope, catalog, policy)?;
			Ok(BoundExpr::IsNull {
				expr: Box::new(val),
				negated: *negated,
			})
		}
		Expr::TypeCast {
			expr,
			data_type,
			..
		} => {
			let val = bind_expr(expr, scope, catalog, policy)?;
			let target_type = data_type.name.parts.join(".");
			Ok(BoundExpr::Cast {
				expr: Box::new(val),
				target_type,
			})
		}
		Expr::Grouping {
			expr,
			..
		} => bind_expr(expr, scope, catalog, policy),
		Expr::IsDistinctFrom {
			left,
			right,
			negated,
			..
		} => {
			let l = bind_expr(left, scope, catalog, policy)?;
			let r = bind_expr(right, scope, catalog, policy)?;
			let op = if *negated {
				BoundBinaryOp::NullSafeEqual
			} else {
				BoundBinaryOp::DistinctFrom
			};
			Ok(BoundExpr::Binary {
				left: Box::new(l),
				op,
				right: Box::new(r),
			})
		}
		Expr::Subquery {
			subquery,
			..
		} => {
			let bound_subquery =
				bind_select(subquery, catalog, policy, Some(Arc::new(scope.clone())))?;
			Ok(BoundExpr::ScalarSubquery(Box::new(bound_subquery)))
		}
		Expr::Exists {
			subquery,
			..
		} => {
			let bound_subquery =
				bind_select(subquery, catalog, policy, Some(Arc::new(scope.clone())))?;
			Ok(BoundExpr::Exists(Box::new(bound_subquery)))
		}
		Expr::InSubquery {
			expr,
			subquery,
			negated,
			..
		} => {
			let val = bind_expr(expr, scope, catalog, policy)?;
			let bound_subquery =
				bind_select(subquery, catalog, policy, Some(Arc::new(scope.clone())))?;
			Ok(BoundExpr::InSubquery {
				expr: Box::new(val),
				subquery: Box::new(bound_subquery),
				negated: *negated,
			})
		}
		Expr::ArraySubquery {
			subquery,
			..
		} => {
			let bound_subquery =
				bind_select(subquery, catalog, policy, Some(Arc::new(scope.clone())))?;
			Ok(BoundExpr::ArraySubquery(Box::new(bound_subquery)))
		}
		Expr::Quantified {
			expr,
			op,
			quantifier,
			subquery,
			..
		} => {
			use surrealdb_pgsql::Quantifier;
			use surrealdb_pgsql::ast::op::BinaryOperator;

			let val = bind_expr(expr, scope, catalog, policy)?;
			let bound_subquery =
				bind_select(subquery, catalog, policy, Some(Arc::new(scope.clone())))?;

			match (op, quantifier) {
				(BinaryOperator::Eq, Quantifier::Any | Quantifier::Some) => {
					Ok(BoundExpr::InSubquery {
						expr: Box::new(val),
						subquery: Box::new(bound_subquery),
						negated: false,
					})
				}
				(BinaryOperator::NotEq, Quantifier::All) => Ok(BoundExpr::InSubquery {
					expr: Box::new(val),
					subquery: Box::new(bound_subquery),
					negated: true,
				}),
				_ => {
					let bound_op = match op {
						BinaryOperator::Lt => BoundBinaryOp::LessThan,
						BinaryOperator::LtEq => BoundBinaryOp::LessThanEqual,
						BinaryOperator::Gt => BoundBinaryOp::GreaterThan,
						BinaryOperator::GtEq => BoundBinaryOp::GreaterThanEqual,
						BinaryOperator::Eq => BoundBinaryOp::Equal,
						BinaryOperator::NotEq => BoundBinaryOp::NotEqual,
						BinaryOperator::Add
						| BinaryOperator::Sub
						| BinaryOperator::Mul
						| BinaryOperator::Div
						| BinaryOperator::Mod
						| BinaryOperator::Exp
						| BinaryOperator::BitAnd
						| BinaryOperator::BitOr
						| BinaryOperator::BitXor
						| BinaryOperator::ShiftLeft
						| BinaryOperator::ShiftRight
						| BinaryOperator::And
						| BinaryOperator::Or
						| BinaryOperator::Concat
						| BinaryOperator::Like
						| BinaryOperator::NotLike
						| BinaryOperator::ILike
						| BinaryOperator::NotILike
						| BinaryOperator::SimilarTo
						| BinaryOperator::NotSimilarTo
						| BinaryOperator::RegexMatch
						| BinaryOperator::RegexMatchCi
						| BinaryOperator::RegexNotMatch
						| BinaryOperator::RegexNotMatchCi
						| BinaryOperator::Arrow
						| BinaryOperator::LongArrow
						| BinaryOperator::HashArrow
						| BinaryOperator::HashLongArrow
						| BinaryOperator::Contains
						| BinaryOperator::ContainedBy
						| BinaryOperator::Custom(_) => {
							return Err(PlanError::UnsupportedExpression(format!(
								"Quantified op {op:?}"
							)));
						}
					};
					let agg_fn = match (op, quantifier) {
						(
							BinaryOperator::Gt | BinaryOperator::GtEq,
							Quantifier::Any | Quantifier::Some,
						) => "min",
						(BinaryOperator::Gt | BinaryOperator::GtEq, Quantifier::All) => "max",
						(
							BinaryOperator::Lt | BinaryOperator::LtEq,
							Quantifier::Any | Quantifier::Some,
						) => "max",
						(BinaryOperator::Lt | BinaryOperator::LtEq, Quantifier::All) => "min",
						_ => {
							return Err(PlanError::UnsupportedExpression(format!(
								"Quantified comparison {op:?} {quantifier:?}"
							)));
						}
					};

					let mut transformed_subquery = bound_subquery;
					if transformed_subquery.targets.len() == 1 {
						let original_expr = transformed_subquery.targets[0].expr.clone();
						transformed_subquery.targets[0].expr = BoundExpr::FunctionCall {
							name: agg_fn.to_string(),
							args: vec![original_expr],
							distinct: false,
						};
						Ok(BoundExpr::Binary {
							left: Box::new(val),
							op: bound_op,
							right: Box::new(BoundExpr::ScalarSubquery(Box::new(
								transformed_subquery,
							))),
						})
					} else {
						Err(PlanError::UnsupportedExpression(
							"Quantified subquery must return exactly 1 column".into(),
						))
					}
				}
			}
		}
		Expr::Subscript {
			..
		}
		| Expr::Slice {
			..
		}
		| Expr::IsTrue {
			..
		}
		| Expr::IsFalse {
			..
		}
		| Expr::IsUnknown {
			..
		}
		| Expr::Collate {
			..
		}
		| Expr::ArrayConstructor {
			..
		}
		| Expr::Row {
			..
		}
		| Expr::IsBound {
			..
		}
		| Expr::JsonObject {
			..
		}
		| Expr::JsonArray {
			..
		}
		| Expr::JsonExists {
			..
		}
		| Expr::JsonValue {
			..
		}
		| Expr::JsonQuery {
			..
		} => Err(PlanError::UnsupportedExpression(format!("{expr:?}"))),
	}
}

fn select_contains_table_reference(stmt: &SelectStmt, table_name: &str) -> bool {
	for from in &stmt.from {
		if table_ref_contains(from, table_name) {
			return true;
		}
	}
	if let Some(so) = &stmt.set_op
		&& select_contains_table_reference(&so.right, table_name)
	{
		return true;
	}
	false
}

fn table_ref_contains(tr: &TableRef, table_name: &str) -> bool {
	match tr {
		TableRef::Table {
			name,
			..
		} => name.parts.last().map(|s| s == table_name).unwrap_or(false),
		TableRef::Subquery {
			query,
			..
		} => select_contains_table_reference(query, table_name),
		TableRef::Join {
			left,
			right,
			..
		} => table_ref_contains(left, table_name) || table_ref_contains(right, table_name),
		TableRef::GraphTable {
			..
		}
		| TableRef::JsonTable(_) => false,
	}
}

fn bind_window_spec(
	spec: &surrealdb_pgsql::ast::expr::WindowSpec,
	scope: &QueryScope,
	catalog: &dyn BinderCatalog,
	policy: &DialectPolicy,
) -> Result<BoundWindowSpec, PlanError> {
	let (base_partitions, base_orders, base_frame) = if let Some(ref name) = spec.window_name {
		let base = scope
			.resolve_window(name, policy)
			.ok_or_else(|| PlanError::Custom(format!("window \"{name}\" does not exist")))?;
		(base.partition_by.clone(), base.order_by.clone(), base.frame.clone())
	} else {
		(Vec::new(), Vec::new(), None)
	};

	let partition_by = if !spec.partition_by.is_empty() {
		spec.partition_by
			.iter()
			.map(|p| {
				let bound = bind_expr(p, scope, catalog, policy)?;
				if bound.contains_window() {
					return Err(PlanError::WindowNotAllowed("window function PARTITION BY".into()));
				}
				Ok(bound)
			})
			.collect::<Result<Vec<_>, _>>()?
	} else {
		base_partitions
	};

	let order_by = if !spec.order_by.is_empty() {
		spec.order_by
			.iter()
			.map(|item| {
				let bound = bind_expr(&item.expr, scope, catalog, policy)?;
				if bound.contains_window() {
					return Err(PlanError::WindowNotAllowed("window function ORDER BY".into()));
				}
				let direction = match item.direction {
					Some(OrderDirection::Desc) => BoundOrderDirection::Desc,
					_ => BoundOrderDirection::Asc,
				};
				let nulls_first = match item.nulls {
					Some(NullsOrder::First) => true,
					Some(NullsOrder::Last) => false,
					None => match direction {
						BoundOrderDirection::Asc => false, // PG ASC NULLS LAST
						BoundOrderDirection::Desc => true, // PG DESC NULLS FIRST
					},
				};
				Ok(BoundOrder {
					expr: bound,
					direction,
					nulls_first,
				})
			})
			.collect::<Result<Vec<_>, _>>()?
	} else {
		base_orders
	};

	let frame = if let Some(ref f) = spec.frame {
		let units = match f.units {
			surrealdb_pgsql::ast::expr::WindowFrameUnits::Rows => BoundWindowFrameUnits::Rows,
			surrealdb_pgsql::ast::expr::WindowFrameUnits::Range => BoundWindowFrameUnits::Range,
			surrealdb_pgsql::ast::expr::WindowFrameUnits::Groups => BoundWindowFrameUnits::Groups,
		};
		let start = match &f.start {
			surrealdb_pgsql::ast::expr::WindowFrameBound::CurrentRow => {
				BoundWindowFrameBound::CurrentRow
			}
			surrealdb_pgsql::ast::expr::WindowFrameBound::UnboundedPreceding => {
				BoundWindowFrameBound::UnboundedPreceding
			}
			surrealdb_pgsql::ast::expr::WindowFrameBound::UnboundedFollowing => {
				BoundWindowFrameBound::UnboundedFollowing
			}
			surrealdb_pgsql::ast::expr::WindowFrameBound::Preceding(e) => {
				let bound = bind_expr(e, scope, catalog, policy)?;
				if bound.contains_window() {
					return Err(PlanError::WindowNotAllowed(
						"window frame bound expression".into(),
					));
				}
				BoundWindowFrameBound::Preceding(Box::new(bound))
			}
			surrealdb_pgsql::ast::expr::WindowFrameBound::Following(e) => {
				let bound = bind_expr(e, scope, catalog, policy)?;
				if bound.contains_window() {
					return Err(PlanError::WindowNotAllowed(
						"window frame bound expression".into(),
					));
				}
				BoundWindowFrameBound::Following(Box::new(bound))
			}
		};
		let end = match &f.end {
			Some(surrealdb_pgsql::ast::expr::WindowFrameBound::CurrentRow) => {
				Some(BoundWindowFrameBound::CurrentRow)
			}
			Some(surrealdb_pgsql::ast::expr::WindowFrameBound::UnboundedPreceding) => {
				Some(BoundWindowFrameBound::UnboundedPreceding)
			}
			Some(surrealdb_pgsql::ast::expr::WindowFrameBound::UnboundedFollowing) => {
				Some(BoundWindowFrameBound::UnboundedFollowing)
			}
			Some(surrealdb_pgsql::ast::expr::WindowFrameBound::Preceding(e)) => {
				let bound = bind_expr(e, scope, catalog, policy)?;
				if bound.contains_window() {
					return Err(PlanError::WindowNotAllowed(
						"window frame bound expression".into(),
					));
				}
				Some(BoundWindowFrameBound::Preceding(Box::new(bound)))
			}
			Some(surrealdb_pgsql::ast::expr::WindowFrameBound::Following(e)) => {
				let bound = bind_expr(e, scope, catalog, policy)?;
				if bound.contains_window() {
					return Err(PlanError::WindowNotAllowed(
						"window frame bound expression".into(),
					));
				}
				Some(BoundWindowFrameBound::Following(Box::new(bound)))
			}
			None => None,
		};
		let exclusion = f.exclusion.map(|ex| match ex {
			surrealdb_pgsql::ast::expr::WindowFrameExclusion::CurrentRow => {
				BoundWindowFrameExclusion::CurrentRow
			}
			surrealdb_pgsql::ast::expr::WindowFrameExclusion::Group => {
				BoundWindowFrameExclusion::Group
			}
			surrealdb_pgsql::ast::expr::WindowFrameExclusion::Ties => {
				BoundWindowFrameExclusion::Ties
			}
			surrealdb_pgsql::ast::expr::WindowFrameExclusion::NoOthers => {
				BoundWindowFrameExclusion::NoOthers
			}
		});
		Some(BoundWindowFrame {
			units,
			start,
			end,
			exclusion,
		})
	} else {
		base_frame
	};

	Ok(BoundWindowSpec {
		window_name: spec.window_name.clone(),
		partition_by,
		order_by,
		frame,
	})
}
