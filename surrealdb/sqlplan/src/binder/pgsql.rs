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
	BoundBinaryOp, BoundColumnRef, BoundCte, BoundDistinct, BoundExpr, BoundHnswIndex,
	BoundJoinCondition, BoundJoinType, BoundLiteral, BoundOrder, BoundOrderDirection, BoundSelect,
	BoundSetOpKind, BoundSetOperation, BoundTableSource, BoundTarget, BoundUnaryOp, BoundWindowDef,
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
			} else if let [qualifier @ .., star] = col.fields.as_slice()
				&& !qualifier.is_empty()
				&& star == "*"
			{
				targets.extend(scope.expand_table_wildcard(
					&qualifier.join("."),
					policy,
					catalog,
				)?);
				continue;
			}
		}

		let bound_expr = bind_expr(&target.expr, &scope, catalog, policy)?;
		let alias =
			target.alias.clone().map(|a| policy.fold_identifier(&a).into_owned()).or_else(|| {
				// A cast of a column is named after the column, as in `start_value::int8`.
				if let BoundExpr::Cast {
					expr: inner,
					..
				} = &bound_expr && let BoundExpr::Column(col) = inner.as_ref()
					&& col.column != "*"
				{
					Some(col.column.clone())
				} else {
					None
				}
			});
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

	let hnsw_indexes = match &from_source {
		Some(BoundTableSource::Table {
			schema,
			name,
			..
		}) => catalog
			.hnsw_indexes(schema.as_deref(), name)
			.into_iter()
			.map(|(column, metric)| BoundHnswIndex {
				column,
				metric,
			})
			.collect(),
		_ => Vec::new(),
	};

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
		database: catalog.current_database().map(str::to_owned),
		hnsw_indexes,
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

/// Bind `LATERAL (SELECT targets) AS alias(columns)`: a subquery that has no `FROM` and no
/// clause but its targets, whose targets read the relations to its left as columns of the row they
/// extend. Each target is a column of the relation `alias`.
/// A type named by a constant and cast to `regtype`, as the oid of the type when it is compared
/// with a value that is one (`CASE atttypid WHEN 'int8'::regtype ...`). Any other expression is
/// returned as it is.
fn regtype_constant_as_oid(expr: BoundExpr) -> BoundExpr {
	if let BoundExpr::Cast {
		expr: inner,
		target_type,
	} = &expr
		&& target_type.eq_ignore_ascii_case("regtype")
		&& let BoundExpr::Literal(BoundLiteral::String(name)) = inner.as_ref()
		&& let Some(oid) = crate::lower::expr::regtype_oid(name)
	{
		return BoundExpr::Literal(BoundLiteral::Integer(oid));
	}
	expr
}

fn bind_lateral(
	query: &SelectStmt,
	alias: &surrealdb_pgsql::ast::stmt::TableAlias,
	scope: &mut QueryScope,
	catalog: &dyn BinderCatalog,
	policy: &DialectPolicy,
) -> Result<BoundTableSource, PlanError> {
	let plain = query.with.is_none()
		&& query.distinct.is_none()
		&& query.from.is_empty()
		&& query.where_clause.is_none()
		&& query.group_by.is_empty()
		&& query.having.is_none()
		&& query.window.is_empty()
		&& query.order_by.is_empty()
		&& query.limit.is_none()
		&& query.offset.is_none()
		&& query.locking.is_empty()
		&& query.set_op.is_none();
	if !plain || query.targets.is_empty() {
		return Err(PlanError::UnsupportedStatement(
			"a LATERAL subquery must be a SELECT of expressions with no other clause".into(),
		));
	}
	if alias.columns.len() > query.targets.len() {
		return Err(PlanError::UnsupportedStatement(
			"a column alias list longer than the LATERAL subquery's targets".into(),
		));
	}
	let mut targets = Vec::with_capacity(query.targets.len());
	let mut columns = Vec::with_capacity(query.targets.len());
	for (position, target) in query.targets.iter().enumerate() {
		if matches!(&target.expr, Expr::ColumnRef(column) if column.fields.last().is_some_and(|f| f == "*"))
		{
			return Err(PlanError::UnsupportedStatement("a wildcard in a LATERAL subquery".into()));
		}
		let name = alias
			.columns
			.get(position)
			.cloned()
			.or_else(|| target.alias.clone())
			.unwrap_or_else(|| format!("column{}", position + 1));
		if columns.contains(&name) {
			return Err(PlanError::UnsupportedStatement(format!(
				"the LATERAL subquery names two columns \"{name}\""
			)));
		}
		targets.push(BoundTarget {
			expr: bind_expr(&target.expr, scope, catalog, policy)?,
			alias: Some(name.clone()),
			is_wildcard: false,
			table_wildcard: None,
		});
		columns.push(name);
	}
	let relation_index = scope.register_subquery_with_columns(&alias.name, Some(columns), policy);
	// The relation keeps the targets, so a column read from it has the type of its expression.
	scope.relations[relation_index].source_query = Some(Arc::new(BoundSelect {
		with: None,
		from: None,
		where_clause: None,
		targets: targets.clone(),
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
		database: None,
		hnsw_indexes: Vec::new(),
	}));
	Ok(BoundTableSource::Lateral {
		targets,
		relation_index,
		alias: alias.name.clone(),
	})
}

/// The functions whose answer is a property of the catalog (`pg_get_indexdef`,
/// `pg_get_constraintdef`, `pg_get_serial_sequence`): the call becomes a lookup over what the
/// catalog lists, or `None` when `fn_name` is no such function. Kept apart from [`bind_expr`] so
/// that its frame, which every nested expression pays for, stays small.
#[inline(never)]
fn bind_catalog_function(
	fn_name: &str,
	over_is_none: bool,
	bound_args: &[BoundExpr],
	catalog: &dyn BinderCatalog,
) -> Result<Option<BoundExpr>, PlanError> {
	if fn_name == "pg_get_indexdef" && over_is_none && bound_args.len() == 1 {
		// The definition is a property of the index the oid names, which only the catalog
		// knows: the call becomes a lookup over the indexes it lists, and an oid that
		// names none is NULL.
		let definitions =
			crate::catalog::relations::index_definitions(&catalog.constraint_tables());
		if definitions.is_empty() {
			return Ok(Some(BoundExpr::Literal(BoundLiteral::Null)));
		}
		let Some(operand) = bound_args.first().cloned() else {
			return Ok(Some(BoundExpr::Literal(BoundLiteral::Null)));
		};
		return Ok(Some(BoundExpr::Case {
			operand: Some(Box::new(operand)),
			branches: definitions
				.into_iter()
				.map(|(oid, definition)| {
					(
						BoundExpr::Literal(BoundLiteral::Integer(oid)),
						BoundExpr::Literal(BoundLiteral::String(definition)),
					)
				})
				.collect(),
			else_expr: Some(Box::new(BoundExpr::Literal(BoundLiteral::Null))),
		}));
	}

	if fn_name == "pg_get_constraintdef" && over_is_none {
		// The definition is a property of the constraint the oid names, which only the
		// catalog knows: the call becomes a lookup over the constraints it lists, and
		// an oid that names none is NULL.
		let definitions =
			crate::catalog::relations::constraint_definitions(&catalog.constraint_tables());
		if definitions.is_empty() {
			return Ok(Some(BoundExpr::Literal(BoundLiteral::Null)));
		}
		let mut args = bound_args.iter().cloned();
		let operand = args.next().ok_or_else(|| PlanError::FunctionArgumentCountMismatch {
			name: fn_name.to_owned(),
			expected: "at least 1".into(),
			got: 0,
		})?;
		return Ok(Some(BoundExpr::Case {
			operand: Some(Box::new(operand)),
			branches: definitions
				.into_iter()
				.map(|(oid, definition)| {
					(
						BoundExpr::Literal(BoundLiteral::Integer(oid)),
						BoundExpr::Literal(BoundLiteral::String(definition)),
					)
				})
				.collect(),
			else_expr: Some(Box::new(BoundExpr::Literal(BoundLiteral::Null))),
		}));
	}

	if fn_name == "pg_get_serial_sequence" && over_is_none && bound_args.len() == 2 {
		// The sequence is a property of a column the catalog lists, so the call becomes a
		// lookup over the serial columns: a table is named as the SQL text of its name,
		// qualified or not, quoted or not, or as its oid, and a column that no sequence
		// numbers is NULL. The sequence is named as `regclass` writes it.
		let serial = crate::catalog::relations::serial_columns(&catalog.constraint_tables());
		if serial.is_empty() {
			return Ok(Some(BoundExpr::Literal(BoundLiteral::Null)));
		}
		let text = |value: String| BoundExpr::Literal(BoundLiteral::String(value));
		let equal = |left: BoundExpr, right: BoundExpr| BoundExpr::Binary {
			left: Box::new(left),
			op: BoundBinaryOp::Equal,
			right: Box::new(right),
		};
		let mut args = bound_args.iter().cloned();
		let (Some(table), Some(column)) = (args.next(), args.next()) else {
			return Ok(Some(BoundExpr::Literal(BoundLiteral::Null)));
		};
		let branches = serial
			.into_iter()
			.map(|serial| {
				let quoted = format!("\"{}\"", serial.table.replace('"', "\"\""));
				let mut names = vec![
					serial.table.clone(),
					quoted.clone(),
					format!("{}.{}", serial.schema, serial.table),
					format!("{}.{quoted}", serial.schema),
					format!("\"{}\".{}", serial.schema, serial.table),
					format!("\"{}\".{quoted}", serial.schema),
					serial.table_oid.to_string(),
				];
				names.dedup();
				let table_matches = BoundExpr::InList {
					expr: Box::new(table.clone()),
					list: names.into_iter().map(text).collect(),
					negated: false,
				};
				(
					BoundExpr::Binary {
						left: Box::new(table_matches),
						op: BoundBinaryOp::And,
						right: Box::new(equal(column.clone(), text(serial.column))),
					},
					text(crate::catalog::relations::quote_ident(&serial.sequence)),
				)
			})
			.collect();
		return Ok(Some(BoundExpr::Case {
			operand: None,
			branches,
			else_expr: Some(Box::new(BoundExpr::Literal(BoundLiteral::Null))),
		}));
	}
	Ok(None)
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

			let relation_index =
				scope.register_relation(schema.clone(), &table_name, alias_name, catalog, policy);

			let exposed_name = scope.relations[relation_index].exposed_name.clone();

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
				database: None,
				hnsw_indexes: Vec::new(),
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
			lateral: true,
			query,
			alias,
			..
		} => bind_lateral(query, alias, scope, catalog, policy),
		TableRef::Subquery {
			lateral: false,
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
			if let Some(JoinCondition::Using(cols)) = condition {
				scope.note_using_columns(cols, policy);
			}
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
			if let surrealdb_pgsql::ast::op::BinaryOperator::Custom(custom) = op
				&& let Some(metric) = crate::vector::VectorMetric::from_operator(custom)
			{
				return Ok(BoundExpr::Binary {
					left: Box::new(vector_operand(l)),
					op: BoundBinaryOp::VectorDistance(metric),
					right: Box::new(vector_operand(r)),
				});
			}
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
				surrealdb_pgsql::ast::op::BinaryOperator::HashArrow => {
					BoundBinaryOp::JsonPathExtract
				}
				surrealdb_pgsql::ast::op::BinaryOperator::HashLongArrow => {
					BoundBinaryOp::JsonPathExtractText
				}
				surrealdb_pgsql::ast::op::BinaryOperator::Contains => BoundBinaryOp::JsonContains,
				surrealdb_pgsql::ast::op::BinaryOperator::BitAnd => {
					return Ok(BoundExpr::FunctionCall {
						name: crate::functions::BIT_AND.to_owned(),
						args: vec![l, r],
						distinct: false,
					});
				}
				surrealdb_pgsql::ast::op::BinaryOperator::BitOr
				| surrealdb_pgsql::ast::op::BinaryOperator::BitXor
				| surrealdb_pgsql::ast::op::BinaryOperator::ShiftLeft
				| surrealdb_pgsql::ast::op::BinaryOperator::ShiftRight
				| surrealdb_pgsql::ast::op::BinaryOperator::SimilarTo
				| surrealdb_pgsql::ast::op::BinaryOperator::NotSimilarTo
				| surrealdb_pgsql::ast::op::BinaryOperator::ContainedBy
				| surrealdb_pgsql::ast::op::BinaryOperator::Custom(_) => {
					return Err(PlanError::UnsupportedExpression(format!("Binary op {op:?}")));
				}
			};
			let (l, r) = coerce_comparison_operands(bound_op, l, r, catalog);
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
			// A FILTER applies to the rows an aggregate folds; no other function folds rows.
			if filter.is_some() && func_def.kind != SqlFunctionKind::Aggregate {
				return Err(PlanError::UnsupportedExpression(if over.is_some() {
					format!("FILTER on the non-aggregate window function {fn_name}")
				} else {
					format!("FILTER on {fn_name}, which is not an aggregate function")
				}));
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
					key_read: crate::ir::KeyRead::None,
					also_in: Vec::new(),
				})]
			} else {
				args.iter()
					.map(|a| bind_expr(a, scope, catalog, policy))
					.collect::<Result<Vec<_>, _>>()?
			};

			let bound_args =
				if crate::vector::is_distance_function(&fn_name) || fn_name == "inner_product" {
					bound_args.into_iter().map(vector_operand).collect()
				} else {
					bound_args
				};
			if let Some(call) =
				bind_catalog_function(&fn_name, over.is_none(), &bound_args, catalog)?
			{
				return Ok(call);
			}

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
				if crate::functions::FunctionRegistry::is_bound_aggregate(&fn_name)
					&& bound_args.iter().any(|a| a.contains_window())
				{
					return Err(PlanError::WindowNotAllowed("aggregate function calls".into()));
				}
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
				let mut cond = bind_expr(&when.when, scope, catalog, policy)?;
				if bound_operand.is_some() {
					cond = regtype_constant_as_oid(cond);
				}
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
			let mut target_type = data_type.name.parts.join(".");
			if data_type.name.parts.last().is_some_and(|name| name.eq_ignore_ascii_case("vector")) {
				if !data_type.array_bounds.is_empty() {
					return Err(PlanError::UnsupportedExpression(
						"a cast to an array of vector".into(),
					));
				}
				target_type = "vector".to_string();
				if data_type.modifiers.len() > 1 {
					return Err(PlanError::InvalidParameterValue(
						"type modifier is not allowed for type \"vector\"".into(),
					));
				}
				if let Some(dimensions) = data_type.modifiers.first() {
					let dimensions = dimensions.parse::<i64>().map_err(|_| {
						PlanError::InvalidParameterValue(format!(
							"invalid type modifier for type vector: {dimensions}"
						))
					})?;
					target_type =
						format!("vector({})", crate::vector::check_dimensions(dimensions)?);
				}
			} else {
				for _ in &data_type.array_bounds {
					target_type.push_str("[]");
				}
			}
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
			// A wildcard stands for columns that are not known until the rows are read.
			let columns_known =
				!bound_subquery.targets.iter().any(|t| t.is_wildcard || t.table_wildcard.is_some());
			if columns_known && bound_subquery.targets.len() != 1 {
				return Err(PlanError::Custom("subquery must return only one column".into()));
			}
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
		Expr::QuantifiedArray {
			expr,
			op,
			quantifier,
			array,
			..
		} => {
			use surrealdb_pgsql::Quantifier;
			use surrealdb_pgsql::ast::op::BinaryOperator;

			let (BinaryOperator::Eq | BinaryOperator::NotEq) = op else {
				return Err(PlanError::UnsupportedExpression(format!(
					"{op:?} {quantifier:?} (array)"
				)));
			};
			let equal = *op == BinaryOperator::Eq;
			// The comparison and the quantifier are the two literal arguments of the internal
			// function the lowering reads three-valued.
			Ok(BoundExpr::FunctionCall {
				name: crate::functions::QUANTIFIED_ARRAY.to_owned(),
				args: vec![
					bind_expr(expr, scope, catalog, policy)?,
					bind_expr(array, scope, catalog, policy)?,
					BoundExpr::Literal(BoundLiteral::Boolean(equal)),
					BoundExpr::Literal(BoundLiteral::Boolean(matches!(
						quantifier,
						Quantifier::All
					))),
				],
				distinct: false,
			})
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

					// The comparison is rewritten over one aggregate of the subquery's column,
					// which reads the same only where the subquery answers every row of its
					// source once.
					if !bound_subquery.group_by.is_empty()
						|| bound_subquery.having.is_some()
						|| bound_subquery.set_op.is_some()
						|| bound_subquery.limit.is_some()
						|| bound_subquery.offset.is_some()
					{
						return Err(PlanError::UnsupportedExpression(
							"a quantified comparison over a subquery with GROUP BY, HAVING, LIMIT, OFFSET or a set operation"
								.into(),
						));
					}
					// A `DISTINCT` or an `ORDER BY` of the subquery changes no `min` or `max` of
					// its column, so the aggregate is taken over the rows it reads.
					let mut transformed_subquery = bound_subquery;
					transformed_subquery.distinct = None;
					transformed_subquery.order_by.clear();
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
			expr,
			index,
			..
		} => Ok(BoundExpr::FunctionCall {
			name: crate::functions::SUBSCRIPT.to_owned(),
			args: vec![
				bind_expr(expr, scope, catalog, policy)?,
				bind_expr(index, scope, catalog, policy)?,
			],
			distinct: false,
		}),
		Expr::Collate {
			expr: inner,
			collation,
			..
		} => {
			if is_byte_order_collation(&collation.parts) {
				bind_expr(inner, scope, catalog, policy)
			} else {
				Err(unsupported_collation(&collation.parts))
			}
		}
		Expr::Slice {
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
		TableRef::Function {
			..
		}
		| TableRef::GraphTable {
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
		super::check_inherited_window(
			name,
			base,
			!spec.partition_by.is_empty(),
			!spec.order_by.is_empty(),
			spec.frame.is_some(),
		)?;
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

/// An operand of a vector function: a text literal, which PostgreSQL types as the `vector` the
/// function takes, is cast to one.
fn vector_operand(operand: BoundExpr) -> BoundExpr {
	if matches!(operand, BoundExpr::Literal(BoundLiteral::String(_))) {
		BoundExpr::Cast {
			expr: Box::new(operand),
			target_type: "vector".to_string(),
		}
	} else {
		operand
	}
}

/// The integer a text stands for when it meets an `oid` column, as the text of an `oid` constant
/// is read.
fn oid_literal(text: &str) -> Option<BoundExpr> {
	text.trim()
		.parse::<u32>()
		.ok()
		.map(|oid| BoundExpr::Literal(BoundLiteral::Integer(i64::from(oid))))
}

fn is_regclass(target_type: &str) -> bool {
	target_type.eq_ignore_ascii_case("regclass")
		|| target_type.eq_ignore_ascii_case("pg_catalog.regclass")
}

/// A `regclass` cast read as the oid it names, for a comparison with a column of oids: the name of
/// a user table, written as its SQL text qualified or not and quoted or not, or the oid itself as
/// text, answers with the oid `pg_class` lists for the table. A name that is none of them is the
/// cast's own value, which matches no oid.
fn regclass_as_oid(cast: BoundExpr, catalog: &dyn BinderCatalog) -> BoundExpr {
	let BoundExpr::Cast {
		expr,
		..
	} = cast
	else {
		return cast;
	};
	let branches: Vec<(BoundExpr, BoundExpr)> = catalog
		.constraint_tables()
		.iter()
		.enumerate()
		.map(|(i, table)| {
			let oid = crate::catalog::relations::oid_of_table(i);
			let quoted = format!("\"{}\"", table.name.replace('"', "\"\""));
			let schema = table.schema.as_deref().unwrap_or("public");
			let names = [
				table.name.clone(),
				quoted.clone(),
				format!("{schema}.{}", table.name),
				format!("{schema}.{quoted}"),
				oid.to_string(),
			];
			(
				BoundExpr::InList {
					expr: expr.clone(),
					list: names
						.into_iter()
						.map(|name| BoundExpr::Literal(BoundLiteral::String(name)))
						.collect(),
					negated: false,
				},
				BoundExpr::Literal(BoundLiteral::Integer(oid)),
			)
		})
		.collect();
	if branches.is_empty() {
		return BoundExpr::Literal(BoundLiteral::Null);
	}
	BoundExpr::Case {
		operand: None,
		branches,
		else_expr: Some(Box::new(BoundExpr::Literal(BoundLiteral::Null))),
	}
}

/// The operands of a comparison, with a text that meets an `oid` column read as the oid and a
/// `regclass` cast that meets one read as the oid of the table it names. Kept out of
/// [`bind_expr`] so that its frame stays small for deeply nested expressions.
#[inline(never)]
fn coerce_comparison_operands(
	op: BoundBinaryOp,
	l: BoundExpr,
	r: BoundExpr,
	catalog: &dyn BinderCatalog,
) -> (BoundExpr, BoundExpr) {
	if !matches!(op, BoundBinaryOp::Equal | BoundBinaryOp::NotEqual) {
		return (l, r);
	}
	match (&l, &r) {
		(BoundExpr::Column(col), BoundExpr::Literal(BoundLiteral::String(text)))
			if col.declared_type == crate::SqlType::Oid =>
		{
			let r = oid_literal(text).unwrap_or(r);
			(l, r)
		}
		(BoundExpr::Literal(BoundLiteral::String(text)), BoundExpr::Column(col))
			if col.declared_type == crate::SqlType::Oid =>
		{
			let l = oid_literal(text).unwrap_or(l);
			(l, r)
		}
		(
			BoundExpr::Column(_),
			BoundExpr::Cast {
				target_type,
				..
			},
		) if is_regclass(target_type) => (l, regclass_as_oid(r, catalog)),
		(
			BoundExpr::Cast {
				target_type,
				..
			},
			BoundExpr::Column(_),
		) if is_regclass(target_type) => (regclass_as_oid(l, catalog), r),
		_ => (l, r),
	}
}

/// Whether a collation name means comparison by bytes, which is how every string compares:
/// `default`, `C`, `POSIX` and `ucs_basic`, bare or in `pg_catalog`.
fn is_byte_order_collation(parts: &[String]) -> bool {
	let named = parts.last().is_some_and(|name| {
		["default", "c", "posix", "ucs_basic"]
			.iter()
			.any(|byte_order| name.eq_ignore_ascii_case(byte_order))
	});
	named && (parts.len() == 1 || (parts.len() == 2 && parts[0].eq_ignore_ascii_case("pg_catalog")))
}

#[inline(never)]
fn unsupported_collation(parts: &[String]) -> PlanError {
	PlanError::UnsupportedExpression(format!(
		"COLLATE {}: only the byte-order collations default, C, POSIX and ucs_basic are supported",
		parts.join(".")
	))
}
