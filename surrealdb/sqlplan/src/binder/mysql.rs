//! MySQL AST binder implementation.

use std::sync::Arc;

use surrealdb_mysql::ast::expr::{ColumnRef, Expr, Quantifier, TruthValue};
use surrealdb_mysql::ast::literal::Literal as MyLiteral;
use surrealdb_mysql::ast::operator::{BinaryOperator, UnaryOperator};
use surrealdb_mysql::ast::stmt::{
	JoinCondition, JoinType, QualifiedName, SelectItem, SelectStmt, SetOpKind, TableRef,
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

/// Bind a MySQL `SelectStmt` into a `BoundSelect` using the given catalog and policy.
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
			if with_clause.recursive && select_contains_table_reference(&cte.query, &cte.name) {
				return Err(PlanError::UnsupportedStatement(
					"Recursive CTE self-reference is not supported in streaming engine".into(),
				));
			}

			let mut bound_query =
				bind_select(&cte.query, catalog, policy, Some(Arc::new(scope.clone())))?;

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
						let name = match &t.expr {
							BoundExpr::Column(c) => c.column.clone(),
							_ => format!("col_{}", i + 1),
						};
						t.alias = Some(name);
					}
				}
			}

			let bound_cte = BoundCte {
				name: cte.name.clone(),
				columns: cte.columns.clone(),
				materialized: None,
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

	// 1. Bind FROM clause
	let from_source = match &select.from {
		None => Some(BoundTableSource::Values),
		Some(table_ref) => Some(bind_table_ref(table_ref, &mut scope, catalog, policy)?),
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
	for item in &select.projection {
		match item {
			SelectItem::Wildcard(_) => {
				targets.extend(scope.expand_wildcard());
			}
			SelectItem::QualifiedWildcard(name, _) => {
				let prefix = name.clone();
				targets.extend(scope.expand_table_wildcard(&prefix, policy)?);
			}
			SelectItem::Expr {
				expr,
				alias,
				..
			} => {
				let bound_expr = bind_expr(expr, &scope, catalog, policy)?;
				let folded_alias = alias.as_ref().map(|a| policy.fold_identifier(a).into_owned());
				targets.push(BoundTarget {
					expr: bound_expr,
					alias: folded_alias,
					is_wildcard: false,
					table_wildcard: None,
				});
			}
		}
	}

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
		let bound = match g {
			Expr::Literal(MyLiteral::Integer(pos), _) => {
				if policy.group_by_ordinal {
					scope.resolve_group_by_ordinal(*pos as usize)?
				} else {
					return Err(PlanError::Custom("GROUP BY ordinal not allowed".into()));
				}
			}
			Expr::ColumnRef(ColumnRef {
				fields,
				..
			}) if fields.len() == 1 => {
				let col_name = &fields[0];
				if policy.group_by_alias
					&& let Some(target_expr) = scope.find_target_by_alias(col_name, policy)
				{
					target_expr
				} else {
					bind_expr(g, &scope, catalog, policy)?
				}
			}
			_ => bind_expr(g, &scope, catalog, policy)?,
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
		let expr = match &item.expr {
			Expr::Literal(MyLiteral::Integer(pos), _) => {
				if policy.order_by_ordinal {
					scope.resolve_order_by_ordinal(*pos as usize)?
				} else {
					return Err(PlanError::Custom("ORDER BY ordinal not allowed".into()));
				}
			}
			Expr::ColumnRef(ColumnRef {
				fields,
				..
			}) if fields.len() == 1 => {
				let col_name = &fields[0];
				if let Some(target_expr) = scope.find_target_by_alias(col_name, policy) {
					target_expr
				} else {
					bind_expr(&item.expr, &scope, catalog, policy)?
				}
			}
			_ => bind_expr(&item.expr, &scope, catalog, policy)?,
		};

		let direction = if item.asc {
			BoundOrderDirection::Asc
		} else {
			BoundOrderDirection::Desc
		};

		// MySQL default: ASC is NULLS FIRST, DESC is NULLS LAST
		let nulls_first = match direction {
			BoundOrderDirection::Asc => true,
			BoundOrderDirection::Desc => false,
		};

		order_by.push(BoundOrder {
			expr,
			direction,
			nulls_first,
		});
	}

	// 7. Bind LIMIT / OFFSET
	let (limit, offset) = if let Some(l) = &select.limit {
		let lim = bind_expr(&l.count, &scope, catalog, policy)?;
		let off = l.offset.as_ref().map(|o| bind_expr(o, &scope, catalog, policy)).transpose()?;
		(Some(lim), off)
	} else {
		(None, None)
	};

	let distinct = if select.distinct {
		Some(BoundDistinct::All)
	} else {
		None
	};

	let for_update = select.lock.is_some();

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
				let exposed_name = alias.clone().unwrap_or_else(|| cte.name.clone());

				let columns = if !cte.columns.is_empty() {
					Some(cte.columns.clone())
				} else {
					Some(
						cte.query
							.targets
							.iter()
							.enumerate()
							.map(|(i, t)| {
								t.alias.clone().unwrap_or_else(|| match &t.expr {
									BoundExpr::Column(c) => c.column.clone(),
									_ => format!("col_{}", i + 1),
								})
							})
							.collect(),
					)
				};

				let relation_index =
					scope.register_subquery_with_columns(&exposed_name, columns, policy);

				return Ok(BoundTableSource::Subquery {
					query: cte.query,
					relation_index,
					alias: exposed_name,
				});
			}

			let (schema, table_name) = resolve_table_qualified_name(name, policy, Some(catalog))?;
			let alias_name = alias.clone();

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
			subquery,
			alias,
			..
		} => {
			let bound_sub = bind_select(subquery, catalog, policy, scope.parent.clone())?;
			let relation_index = scope.register_subquery(alias, policy);

			Ok(BoundTableSource::Subquery {
				query: Box::new(bound_sub),
				relation_index,
				alias: alias.clone(),
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
				JoinType::Cross | JoinType::Straight => BoundJoinType::Cross,
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
	}
}

pub fn resolve_table_qualified_name(
	name: &QualifiedName,
	policy: &DialectPolicy,
	catalog: Option<&dyn BinderCatalog>,
) -> Result<(Option<String>, String), PlanError> {
	let folded_parts: Vec<String> =
		name.parts.iter().map(|p| policy.fold_identifier(p).into_owned()).collect();
	crate::namespace::resolve_mysql_table_name(&folded_parts, catalog)
}

pub fn bind_expr(
	expr: &Expr,
	scope: &QueryScope,
	catalog: &dyn BinderCatalog,
	policy: &DialectPolicy,
) -> Result<BoundExpr, PlanError> {
	match expr {
		Expr::Literal(lit, _) => match lit {
			MyLiteral::Integer(i) => Ok(BoundExpr::Literal(BoundLiteral::Integer(*i))),
			MyLiteral::Float(f) => Ok(BoundExpr::Literal(BoundLiteral::Float(*f))),
			MyLiteral::String(s) => Ok(BoundExpr::Literal(BoundLiteral::String(s.clone()))),
			MyLiteral::IntroducedString {
				value,
				..
			} => Ok(BoundExpr::Literal(BoundLiteral::String(value.clone()))),
			MyLiteral::HexString(bytes) => Ok(BoundExpr::Literal(BoundLiteral::String(
				String::from_utf8_lossy(bytes).into_owned(),
			))),
			MyLiteral::IntroducedHexString {
				value,
				..
			} => Ok(BoundExpr::Literal(BoundLiteral::String(
				String::from_utf8_lossy(value).into_owned(),
			))),
			MyLiteral::Boolean(b) => Ok(BoundExpr::Literal(BoundLiteral::Boolean(*b))),
			MyLiteral::Null => Ok(BoundExpr::Literal(BoundLiteral::Null)),
			_ => Ok(BoundExpr::Literal(BoundLiteral::Null)),
		},
		Expr::ColumnRef(ColumnRef {
			fields,
			..
		}) => {
			let bound_col = scope.resolve_column(fields, policy)?;
			Ok(BoundExpr::Column(bound_col))
		}
		Expr::Param(idx, _) => {
			let name = surrealdb_types::positional_param_name(*idx);
			Ok(BoundExpr::Param {
				index: *idx,
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
				UnaryOperator::Not => BoundUnaryOp::Not,
				UnaryOperator::Plus => BoundUnaryOp::Positive,
				UnaryOperator::Minus => BoundUnaryOp::Negate,
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
			let l = bind_expr(left, scope, catalog, policy)?;
			let r = bind_expr(right, scope, catalog, policy)?;
			let bound_op = match op {
				BinaryOperator::Add => BoundBinaryOp::Add,
				BinaryOperator::Sub => BoundBinaryOp::Subtract,
				BinaryOperator::Mul => BoundBinaryOp::Multiply,
				BinaryOperator::Div => BoundBinaryOp::Divide,
				BinaryOperator::Mod => BoundBinaryOp::Remainder,
				BinaryOperator::Equal => BoundBinaryOp::Equal,
				BinaryOperator::NotEqual => BoundBinaryOp::NotEqual,
				BinaryOperator::Less => BoundBinaryOp::LessThan,
				BinaryOperator::LessEqual => BoundBinaryOp::LessThanEqual,
				BinaryOperator::Greater => BoundBinaryOp::GreaterThan,
				BinaryOperator::GreaterEqual => BoundBinaryOp::GreaterThanEqual,
				BinaryOperator::And => BoundBinaryOp::And,
				BinaryOperator::Or => BoundBinaryOp::Or,
				BinaryOperator::Concat => BoundBinaryOp::Concat,
				BinaryOperator::NullSafeEqual => BoundBinaryOp::NullSafeEqual,
				BinaryOperator::IntDiv => BoundBinaryOp::IntDiv,
				BinaryOperator::Like => BoundBinaryOp::Like,
				BinaryOperator::Regexp | BinaryOperator::Rlike => BoundBinaryOp::Regexp,
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
			over,
			..
		} => {
			let fn_name = name.to_lowercase();
			let func_def = FunctionRegistry::lookup(&fn_name, SqlDialect::MySql)
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

			let bound_args = if fn_name == "count"
				&& args.len() == 1
				&& matches!(&args[0], Expr::ColumnRef(col) if col.fields.len() == 1 && col.fields[0] == "*")
			{
				vec![BoundExpr::Column(BoundColumnRef {
					relation_index: 0,
					relation_name: String::new(),
					column: "*".to_string(),
					correlation_depth: 0,
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
		Expr::Cast {
			expr,
			target_type,
			..
		} => {
			let val = bind_expr(expr, scope, catalog, policy)?;
			Ok(BoundExpr::Cast {
				expr: Box::new(val),
				target_type: target_type.clone(),
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
		Expr::UserVar(name, _) => Ok(BoundExpr::UserVar(name.clone())),
		Expr::SystemVar {
			name,
			is_global,
			..
		} => Ok(BoundExpr::SystemVar {
			name: name.clone(),
			is_global: *is_global,
		}),
		Expr::Quantified {
			expr,
			op,
			quantifier,
			subquery,
			..
		} => {
			let val = bind_expr(expr, scope, catalog, policy)?;
			let bound_subquery =
				bind_select(subquery, catalog, policy, Some(Arc::new(scope.clone())))?;

			match (op, quantifier) {
				(BinaryOperator::Equal, Quantifier::Any | Quantifier::Some) => {
					Ok(BoundExpr::InSubquery {
						expr: Box::new(val),
						subquery: Box::new(bound_subquery),
						negated: false,
					})
				}
				(BinaryOperator::NotEqual, Quantifier::All) => Ok(BoundExpr::InSubquery {
					expr: Box::new(val),
					subquery: Box::new(bound_subquery),
					negated: true,
				}),
				_ => {
					let bound_op = match op {
						BinaryOperator::Less => BoundBinaryOp::LessThan,
						BinaryOperator::LessEqual => BoundBinaryOp::LessThanEqual,
						BinaryOperator::Greater => BoundBinaryOp::GreaterThan,
						BinaryOperator::GreaterEqual => BoundBinaryOp::GreaterThanEqual,
						BinaryOperator::Equal => BoundBinaryOp::Equal,
						BinaryOperator::NotEqual => BoundBinaryOp::NotEqual,
						_ => {
							return Err(PlanError::UnsupportedExpression(format!(
								"Quantified op {op:?}"
							)));
						}
					};
					let agg_fn = match (op, quantifier) {
						(
							BinaryOperator::Greater | BinaryOperator::GreaterEqual,
							Quantifier::Any | Quantifier::Some,
						) => "min",
						(
							BinaryOperator::Greater | BinaryOperator::GreaterEqual,
							Quantifier::All,
						) => "max",
						(
							BinaryOperator::Less | BinaryOperator::LessEqual,
							Quantifier::Any | Quantifier::Some,
						) => "max",
						(BinaryOperator::Less | BinaryOperator::LessEqual, Quantifier::All) => {
							"min"
						}
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
		Expr::IsTruthValue {
			expr,
			negated,
			value,
			..
		} => {
			let val = bind_expr(expr, scope, catalog, policy)?;
			match value {
				TruthValue::Unknown => Ok(BoundExpr::IsNull {
					expr: Box::new(val),
					negated: *negated,
				}),
				TruthValue::True => {
					let is_not_null = BoundExpr::IsNull {
						expr: Box::new(val.clone()),
						negated: true,
					};
					let is_true = BoundExpr::Binary {
						left: Box::new(val),
						op: BoundBinaryOp::NotEqual,
						right: Box::new(BoundExpr::Literal(BoundLiteral::Integer(0))),
					};
					let condition = BoundExpr::Binary {
						left: Box::new(is_not_null),
						op: BoundBinaryOp::And,
						right: Box::new(is_true),
					};
					if *negated {
						Ok(BoundExpr::Unary {
							op: BoundUnaryOp::Not,
							expr: Box::new(condition),
						})
					} else {
						Ok(condition)
					}
				}
				TruthValue::False => {
					let is_not_null = BoundExpr::IsNull {
						expr: Box::new(val.clone()),
						negated: true,
					};
					let is_false = BoundExpr::Binary {
						left: Box::new(val),
						op: BoundBinaryOp::Equal,
						right: Box::new(BoundExpr::Literal(BoundLiteral::Integer(0))),
					};
					let condition = BoundExpr::Binary {
						left: Box::new(is_not_null),
						op: BoundBinaryOp::And,
						right: Box::new(is_false),
					};
					if *negated {
						Ok(BoundExpr::Unary {
							op: BoundUnaryOp::Not,
							expr: Box::new(condition),
						})
					} else {
						Ok(condition)
					}
				}
			}
		}
		_ => Err(PlanError::UnsupportedExpression(format!("{expr:?}"))),
	}
}

fn select_contains_table_reference(stmt: &SelectStmt, table_name: &str) -> bool {
	if let Some(from) = &stmt.from
		&& table_ref_contains(from, table_name)
	{
		return true;
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
		} => name.parts.last().map(|s| s.eq_ignore_ascii_case(table_name)).unwrap_or(false),
		TableRef::Subquery {
			subquery,
			..
		} => select_contains_table_reference(subquery, table_name),
		TableRef::Join {
			left,
			right,
			..
		} => table_ref_contains(left, table_name) || table_ref_contains(right, table_name),
	}
}

fn bind_window_spec(
	spec: &surrealdb_mysql::ast::expr::WindowSpec,
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
				let direction = if item.asc {
					BoundOrderDirection::Asc
				} else {
					BoundOrderDirection::Desc
				};
				let nulls_first = match direction {
					BoundOrderDirection::Asc => true,
					BoundOrderDirection::Desc => false,
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
			surrealdb_mysql::ast::expr::WindowFrameUnits::Rows => BoundWindowFrameUnits::Rows,
			surrealdb_mysql::ast::expr::WindowFrameUnits::Range => BoundWindowFrameUnits::Range,
			surrealdb_mysql::ast::expr::WindowFrameUnits::Groups => BoundWindowFrameUnits::Groups,
		};
		let start = match &f.start {
			surrealdb_mysql::ast::expr::WindowFrameBound::CurrentRow => {
				BoundWindowFrameBound::CurrentRow
			}
			surrealdb_mysql::ast::expr::WindowFrameBound::UnboundedPreceding => {
				BoundWindowFrameBound::UnboundedPreceding
			}
			surrealdb_mysql::ast::expr::WindowFrameBound::UnboundedFollowing => {
				BoundWindowFrameBound::UnboundedFollowing
			}
			surrealdb_mysql::ast::expr::WindowFrameBound::Preceding(e) => {
				let bound = bind_expr(e, scope, catalog, policy)?;
				if bound.contains_window() {
					return Err(PlanError::WindowNotAllowed(
						"window frame bound expression".into(),
					));
				}
				BoundWindowFrameBound::Preceding(Box::new(bound))
			}
			surrealdb_mysql::ast::expr::WindowFrameBound::Following(e) => {
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
			Some(surrealdb_mysql::ast::expr::WindowFrameBound::CurrentRow) => {
				Some(BoundWindowFrameBound::CurrentRow)
			}
			Some(surrealdb_mysql::ast::expr::WindowFrameBound::UnboundedPreceding) => {
				Some(BoundWindowFrameBound::UnboundedPreceding)
			}
			Some(surrealdb_mysql::ast::expr::WindowFrameBound::UnboundedFollowing) => {
				Some(BoundWindowFrameBound::UnboundedFollowing)
			}
			Some(surrealdb_mysql::ast::expr::WindowFrameBound::Preceding(e)) => {
				let bound = bind_expr(e, scope, catalog, policy)?;
				if bound.contains_window() {
					return Err(PlanError::WindowNotAllowed(
						"window frame bound expression".into(),
					));
				}
				Some(BoundWindowFrameBound::Preceding(Box::new(bound)))
			}
			Some(surrealdb_mysql::ast::expr::WindowFrameBound::Following(e)) => {
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
			surrealdb_mysql::ast::expr::WindowFrameExclusion::CurrentRow => {
				BoundWindowFrameExclusion::CurrentRow
			}
			surrealdb_mysql::ast::expr::WindowFrameExclusion::Group => {
				BoundWindowFrameExclusion::Group
			}
			surrealdb_mysql::ast::expr::WindowFrameExclusion::Ties => {
				BoundWindowFrameExclusion::Ties
			}
			surrealdb_mysql::ast::expr::WindowFrameExclusion::NoOthers => {
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
