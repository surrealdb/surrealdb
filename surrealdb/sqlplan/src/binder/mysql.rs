//! MySQL AST binder implementation.

use std::sync::Arc;

use surrealdb_mysql::ast::expr::{ColumnRef, Expr};
use surrealdb_mysql::ast::literal::Literal as MyLiteral;
use surrealdb_mysql::ast::operator::{BinaryOperator, UnaryOperator};
use surrealdb_mysql::ast::stmt::{
	JoinCondition, JoinType, QualifiedName, SelectItem, SelectStmt, TableRef,
};

use crate::catalog::BinderCatalog;
use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::ir::{
	BoundBinaryOp, BoundDistinct, BoundExpr, BoundJoinCondition, BoundJoinType, BoundLiteral,
	BoundOrder, BoundOrderDirection, BoundSelect, BoundTableSource, BoundTarget, BoundUnaryOp,
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

	// 1. Bind FROM clause
	let from_source = match &select.from {
		None => Some(BoundTableSource::Values),
		Some(table_ref) => Some(bind_table_ref(table_ref, &mut scope, catalog, policy)?),
	};

	// 2. Bind projection target list
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
				let bound_expr = bind_expr(expr, &scope, policy)?;
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

	// 3. Bind WHERE clause
	let where_clause =
		select.where_clause.as_ref().map(|e| bind_expr(e, &scope, policy)).transpose()?;

	// 4. Bind GROUP BY
	let mut group_by = Vec::new();
	for g in &select.group_by {
		match g {
			Expr::Literal(MyLiteral::Integer(pos), _) => {
				if policy.group_by_ordinal {
					group_by.push(scope.resolve_group_by_ordinal(*pos as usize)?);
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
					bind_expr(&item.expr, &scope, policy)?
				}
			}
			_ => bind_expr(&item.expr, &scope, policy)?,
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
		let lim = bind_expr(&l.count, &scope, policy)?;
		let off = l.offset.as_ref().map(|o| bind_expr(o, &scope, policy)).transpose()?;
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
	policy: &DialectPolicy,
) -> Result<BoundExpr, PlanError> {
	match expr {
		Expr::Literal(lit, _) => match lit {
			MyLiteral::Integer(i) => Ok(BoundExpr::Literal(BoundLiteral::Integer(*i))),
			MyLiteral::Float(f) => Ok(BoundExpr::Literal(BoundLiteral::Float(*f))),
			MyLiteral::String(s) => Ok(BoundExpr::Literal(BoundLiteral::String(s.clone()))),
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
			let inner = bind_expr(expr, scope, policy)?;
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
			let l = bind_expr(left, scope, policy)?;
			let r = bind_expr(right, scope, policy)?;
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
			..
		} => {
			let fn_name = name.to_lowercase();
			let bound_args =
				args.iter().map(|a| bind_expr(a, scope, policy)).collect::<Result<Vec<_>, _>>()?;
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
		Expr::Cast {
			expr,
			target_type,
			..
		} => {
			let val = bind_expr(expr, scope, policy)?;
			Ok(BoundExpr::Cast {
				expr: Box::new(val),
				target_type: target_type.clone(),
			})
		}
		_ => Err(PlanError::UnsupportedExpression(format!("{expr:?}"))),
	}
}
