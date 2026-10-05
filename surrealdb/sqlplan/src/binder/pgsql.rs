//! PostgreSQL AST binder implementation.

use std::sync::Arc;

use surrealdb_pgsql::ast::expr::{Expr, NullsOrder, OrderDirection, QualifiedName};
use surrealdb_pgsql::ast::literal::Literal as PgLiteral;
use surrealdb_pgsql::ast::stmt::{DistinctClause, JoinCondition, JoinType, SelectStmt, TableRef};

use crate::catalog::BinderCatalog;
use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::ir::{
	BoundBinaryOp, BoundColumnRef, BoundDistinct, BoundExpr, BoundJoinCondition, BoundJoinType,
	BoundLiteral, BoundOrder, BoundOrderDirection, BoundSelect, BoundTableSource, BoundTarget,
	BoundUnaryOp,
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

	// 2. Bind projection target list
	let mut targets = Vec::new();
	for target in &select.targets {
		if let Expr::ColumnRef(col) = &target.expr {
			if col.fields.len() == 1 && col.fields[0] == "*" {
				targets.extend(scope.expand_wildcard());
				continue;
			} else if col.fields.len() == 2 && col.fields[1] == "*" {
				targets.extend(scope.expand_table_wildcard(&col.fields[0], policy)?);
				continue;
			}
		}

		let bound_expr = bind_expr(&target.expr, &scope, policy)?;
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

	// 3. Bind WHERE clause
	let where_clause =
		select.where_clause.as_ref().map(|e| bind_expr(e, &scope, policy)).transpose()?;

	// 4. Bind GROUP BY
	let mut group_by = Vec::new();
	for g in &select.group_by {
		match g {
			Expr::Literal {
				lit: PgLiteral::Integer(pos),
				..
			} => {
				if policy.group_by_ordinal {
					group_by.push(scope.resolve_group_by_ordinal(*pos as usize)?);
				} else {
					return Err(PlanError::Custom("GROUP BY ordinal not allowed".into()));
				}
			}
			Expr::ColumnRef(col) if col.fields.len() == 1 => {
				let col_name = &col.fields[0];
				if policy.group_by_alias
					&& let Some(target_expr) = scope.find_target_by_alias(col_name, policy)
				{
					group_by.push(target_expr);
					continue;
				}
				group_by.push(bind_expr(g, &scope, policy)?);
			}
			_ => {
				group_by.push(bind_expr(g, &scope, policy)?);
			}
		}
	}

	// 5. Bind HAVING
	let having = select.having.as_ref().map(|e| bind_expr(e, &scope, policy)).transpose()?;

	// 6. Bind ORDER BY
	let mut order_by = Vec::new();
	for item in &select.order_by {
		let expr = match &item.expr {
			Expr::Literal {
				lit: PgLiteral::Integer(pos),
				..
			} => {
				if policy.order_by_ordinal {
					scope.resolve_order_by_ordinal(*pos as usize)?
				} else {
					return Err(PlanError::Custom("ORDER BY ordinal not allowed".into()));
				}
			}
			Expr::ColumnRef(col) if col.fields.len() == 1 => {
				let col_name = &col.fields[0];
				if let Some(target_expr) = scope.find_target_by_alias(col_name, policy) {
					target_expr
				} else {
					bind_expr(&item.expr, &scope, policy)?
				}
			}
			_ => bind_expr(&item.expr, &scope, policy)?,
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
	let limit = select.limit.as_ref().map(|e| bind_expr(e, &scope, policy)).transpose()?;
	let offset = select.offset.as_ref().map(|e| bind_expr(e, &scope, policy)).transpose()?;

	// 8. Bind DISTINCT
	let distinct = select
		.distinct
		.as_ref()
		.map(|d| match d {
			DistinctClause::All => Ok(BoundDistinct::All),
			DistinctClause::On(exprs) => {
				let bound_exprs = exprs
					.iter()
					.map(|e| bind_expr(e, &scope, policy))
					.collect::<Result<Vec<_>, _>>()?;
				Ok(BoundDistinct::On(bound_exprs))
			}
		})
		.transpose()?;

	let for_update = !select.locking.is_empty();

	Ok(BoundSelect {
		from: from_source,
		where_clause,
		targets,
		group_by,
		having,
		order_by,
		limit,
		offset,
		distinct,
		for_update,
	})
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
		TableRef::Subquery {
			query,
			alias,
			..
		} => {
			let sub_alias = alias.name.clone();
			let bound_sub = bind_select(query, catalog, policy, scope.parent.clone())?;
			let relation_index = scope.register_subquery(&sub_alias, policy);

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
					Some(BoundJoinCondition::On(bind_expr(expr, scope, policy)?))
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
			let bound_col = scope.resolve_column(&col.fields, policy)?;
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
			let inner = bind_expr(expr, scope, policy)?;
			let bound_op = match op {
				surrealdb_pgsql::ast::op::UnaryOperator::Not => BoundUnaryOp::Not,
				surrealdb_pgsql::ast::op::UnaryOperator::Plus => BoundUnaryOp::Positive,
				surrealdb_pgsql::ast::op::UnaryOperator::Minus => BoundUnaryOp::Negate,
				_ => return Err(PlanError::UnsupportedExpression(format!("Unary op {op:?}"))),
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
			let l = bind_expr(left, scope, policy)?;
			let r = bind_expr(right, scope, policy)?;
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
				surrealdb_pgsql::ast::op::BinaryOperator::RegexMatch
				| surrealdb_pgsql::ast::op::BinaryOperator::RegexMatchCi => BoundBinaryOp::Regexp,
				_ => return Err(PlanError::UnsupportedExpression(format!("Binary op {op:?}"))),
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
			..
		} => {
			let fn_name = name.parts.last().map(|s| s.to_lowercase()).unwrap_or_default();
			let bound_args = if let Some(cond) = filter {
				let bound_cond = bind_expr(cond, scope, policy)?;
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
				} else if args.len() == 1 {
					let bound_arg = bind_expr(&args[0], scope, policy)?;
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
				})]
			} else {
				args.iter().map(|a| bind_expr(a, scope, policy)).collect::<Result<Vec<_>, _>>()?
			};
			Ok(BoundExpr::FunctionCall {
				name: fn_name,
				args: bound_args,
				distinct: *distinct,
			})
		}
		Expr::Case {
			operand,
			branches,
			else_expr,
			..
		} => {
			let bound_operand =
				operand.as_ref().map(|op| bind_expr(op, scope, policy)).transpose()?.map(Box::new);
			let mut bound_branches = Vec::new();
			for when in branches {
				let cond = bind_expr(&when.when, scope, policy)?;
				let val = bind_expr(&when.then, scope, policy)?;
				bound_branches.push((cond, val));
			}
			let bound_else = else_expr
				.as_ref()
				.map(|el| bind_expr(el, scope, policy))
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
			let val = bind_expr(expr, scope, policy)?;
			let l = bind_expr(low, scope, policy)?;
			let h = bind_expr(high, scope, policy)?;
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
			let val = bind_expr(expr, scope, policy)?;
			let items = list
				.iter()
				.map(|item| bind_expr(item, scope, policy))
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
			let val = bind_expr(expr, scope, policy)?;
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
			let val = bind_expr(expr, scope, policy)?;
			let target_type = data_type.name.parts.join(".");
			Ok(BoundExpr::Cast {
				expr: Box::new(val),
				target_type,
			})
		}
		Expr::Grouping {
			expr,
			..
		} => bind_expr(expr, scope, policy),
		Expr::IsDistinctFrom {
			left,
			right,
			negated,
			..
		} => {
			let l = bind_expr(left, scope, policy)?;
			let r = bind_expr(right, scope, policy)?;
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
		_ => Err(PlanError::UnsupportedExpression(format!("{expr:?}"))),
	}
}
