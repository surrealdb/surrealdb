//! PostgreSQL AST to SurrealDB `LogicalOp` plan lowering.

use surrealdb_expr::expr::logical::{
	EvalFrame, LogicalAggregate, LogicalDistinct, LogicalExpr, LogicalField, LogicalFields,
	LogicalFilter, LogicalJoin, LogicalLimit, LogicalLiteral, LogicalOp, LogicalOrder,
	LogicalOrdering, LogicalProject, LogicalSort, LogicalSource, LogicalSubquery, LogicalUnionAll,
	LogicalValues, OutputPath, RelationAlias, SourceRef,
};
use surrealdb_expr::expr::{BinaryOperator, Function, PrefixOperator};
use surrealdb_pgsql::ast::expr::{Expr, OrderByItem, OrderDirection};
use surrealdb_pgsql::ast::literal::Literal;
use surrealdb_pgsql::ast::stmt::{
	JoinCondition, JoinType, ResTarget, SelectStmt, SetOpKind, Statement, TableRef,
};
use surrealdb_strand::{Strand, TableName};
use thiserror::Error;

/// Error type for lowering failures.
#[derive(Debug, Error)]
pub enum LowerError {
	#[error("unsupported expression: {0}")]
	UnsupportedExpr(String),
	#[error("unsupported statement: {0}")]
	UnsupportedStmt(String),
	#[error("lowering error: {0}")]
	Custom(String),
}

/// Indicates a statement has no direct logical plan representation and must be routed
/// to the transactional engine via AST translation.
#[derive(Debug)]
pub enum NoLogicalForm<'a> {
	NonSelectStatement(&'a Statement),
}

/// Lower a PostgreSQL statement directly to a `LogicalOp`.
///
/// Returns `Ok(LogicalOp)` for statements that lower to the streaming engine (e.g. `SELECT`),
/// or `Err(NoLogicalForm)` for statements that must execute on the transactional engine (e.g.
/// `INSERT`, `CREATE`).
pub fn lower_stmt(stmt: &Statement) -> Result<LogicalOp, NoLogicalForm<'_>> {
	match stmt {
		Statement::Select(select) => {
			lower_select(select).map_err(|_| NoLogicalForm::NonSelectStatement(stmt))
		}
		_ => Err(NoLogicalForm::NonSelectStatement(stmt)),
	}
}

/// Lower a PostgreSQL `SELECT` statement directly to a `LogicalOp` pipeline.
pub fn lower_select(select: &SelectStmt) -> Result<LogicalOp, LowerError> {
	// 1. FROM clause
	let mut op = if select.from.is_empty() {
		LogicalOp::Values(LogicalValues {
			expr: LogicalExpr::Array(vec![LogicalExpr::Object(vec![])]),
			for_update: false,
		})
	} else if select.from.len() == 1 {
		lower_table_ref(&select.from[0], !select.locking.is_empty())?
	} else {
		let mut current = lower_table_ref(&select.from[0], !select.locking.is_empty())?;
		for (idx, table) in select.from.iter().skip(1).enumerate() {
			let right = lower_table_ref(table, !select.locking.is_empty())?;
			current = LogicalOp::Join(LogicalJoin::new(
				current,
				right,
				surrealdb_expr::expr::logical::JoinKind::Inner,
				vec![],
				None,
				RelationAlias::new(idx),
				EvalFrame::caller(),
			));
		}
		current
	};

	// 2. WHERE clause
	if let Some(where_expr) = &select.where_clause {
		op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(op),
			predicate: lower_expr(where_expr)?,
		});
	}

	// 3. GROUP BY & Projection preparation
	let fields = lower_targets(&select.targets)?;
	for g in &select.group_by {
		if matches!(
			g,
			Expr::Literal {
				lit: Literal::Integer(_),
				..
			}
		) {
			return Err(LowerError::UnsupportedExpr(
				"GROUP BY ordinal is not supported; use column names".into(),
			));
		}
	}
	let grouped = !select.group_by.is_empty();
	let has_aggregates = select.targets.iter().any(|t| expr_contains_aggregate(&t.expr));
	let is_aggregate = grouped || has_aggregates;
	if is_aggregate {
		let group = select.group_by.iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?;
		op = LogicalOp::Aggregate(LogicalAggregate {
			input: Box::new(op),
			group,
			fields: fields.clone(),
		});
	}

	// 4. HAVING
	if let Some(having_expr) = &select.having {
		op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(op),
			predicate: lower_expr(having_expr)?,
		});
	}

	// 5. DISTINCT
	if select.distinct.is_some() {
		op = LogicalOp::Distinct(LogicalDistinct {
			input: Box::new(op),
		});
	}

	// 6. ORDER BY
	if !select.order_by.is_empty() {
		let orders =
			select.order_by.iter().map(lower_order_by_item).collect::<Result<Vec<_>, _>>()?;
		op = LogicalOp::Sort(LogicalSort {
			input: Box::new(op),
			ordering: LogicalOrdering::Order(orders),
			tempfiles: false,
		});
	}

	// 7. LIMIT / OFFSET
	if select.limit.is_some() || select.offset.is_some() {
		let limit = select.limit.as_ref().map(lower_expr).transpose()?;
		let start = select.offset.as_ref().map(lower_expr).transpose()?;
		op = LogicalOp::Limit(LogicalLimit {
			input: Box::new(op),
			limit,
			start,
		});
	}

	// 8. Projection
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

	// 9. SET OPERATION (UNION, INTERSECT, EXCEPT)
	if let Some(set_op) = &select.set_op {
		let right_op = lower_select(&set_op.right)?;
		match set_op.op {
			SetOpKind::Union => {
				let union_op = LogicalOp::UnionAll(LogicalUnionAll {
					inputs: vec![op, right_op],
				});
				op = if set_op.all {
					union_op
				} else {
					LogicalOp::Distinct(LogicalDistinct {
						input: Box::new(union_op),
					})
				};
			}
			_ => {
				return Err(LowerError::UnsupportedStmt(format!(
					"Set operation {:?} not yet supported in streaming engine",
					set_op.op
				)));
			}
		}
	}

	Ok(op)
}

fn lower_table_ref(table: &TableRef, for_update: bool) -> Result<LogicalOp, LowerError> {
	match table {
		TableRef::Table {
			name,
			..
		} => {
			let table_name = name.parts.last().map(|s| s.as_str()).unwrap_or("");
			Ok(LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(TableName::from(table_name)),
				schema: None,
				with: None,
				for_update,
			}))
		}
		TableRef::Subquery {
			query,
			..
		} => {
			let inner = lower_select(query)?;
			Ok(LogicalOp::Subquery(LogicalSubquery {
				input: Box::new(inner),
			}))
		}
		TableRef::Join {
			left,
			join_type,
			right,
			condition,
			..
		} => {
			let left_op = lower_table_ref(left, for_update)?;
			let right_op = lower_table_ref(right, for_update)?;
			let kind = match join_type {
				JoinType::Inner | JoinType::Cross => surrealdb_expr::expr::logical::JoinKind::Inner,
				JoinType::Left => surrealdb_expr::expr::logical::JoinKind::Left,
				_ => {
					return Err(LowerError::UnsupportedStmt(format!(
						"Join type {join_type:?} not supported directly in streaming engine"
					)));
				}
			};
			let mut on = Vec::new();
			let mut filter = None;
			if let Some(JoinCondition::On(expr)) = condition {
				let cond_expr = lower_expr(expr)?;
				if let LogicalExpr::Binary {
					left,
					op: BinaryOperator::Equal,
					right,
				} = cond_expr
				{
					on.push((*left, *right));
				} else {
					filter = Some(cond_expr);
				}
			}
			Ok(LogicalOp::Join(LogicalJoin::new(
				left_op,
				right_op,
				kind,
				on,
				filter,
				RelationAlias::new(0),
				EvalFrame::caller(),
			)))
		}
		_ => Err(LowerError::UnsupportedStmt(format!("TableRef variant not supported: {table:?}"))),
	}
}

fn lower_targets(targets: &[ResTarget]) -> Result<LogicalFields, LowerError> {
	let mut fields = Vec::new();
	for target in targets {
		if let Expr::ColumnRef(col) = &target.expr
			&& col.fields.len() == 1
			&& col.fields[0] == "*"
		{
			fields.push(LogicalField::All);
			continue;
		}
		let expr = lower_expr(&target.expr)?;
		let alias = target.alias.as_ref().map(|a| OutputPath::field(a.clone()));
		fields.push(LogicalField::user(expr, alias));
	}
	Ok(LogicalFields::Select(fields))
}

fn lower_order_by_item(item: &OrderByItem) -> Result<LogicalOrder, LowerError> {
	let value = lower_expr(&item.expr)?;
	let direction = matches!(item.direction, None | Some(OrderDirection::Asc));
	Ok(LogicalOrder {
		value,
		collate: false,
		numeric: false,
		direction,
	})
}

/// Lower a PostgreSQL AST expression into a SurrealDB `LogicalExpr`.
pub fn lower_expr(expr: &Expr) -> Result<LogicalExpr, LowerError> {
	match expr {
		Expr::Literal {
			lit,
			..
		} => match lit {
			Literal::Integer(i) => Ok(LogicalExpr::Literal(LogicalLiteral::Integer(*i))),
			Literal::Float(f) => Ok(LogicalExpr::Literal(LogicalLiteral::Float(*f))),
			Literal::String(s) => {
				Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from(s.as_str()))))
			}
			Literal::Boolean(b) => Ok(LogicalExpr::Literal(LogicalLiteral::Bool(*b))),
			Literal::Null => Ok(LogicalExpr::Literal(LogicalLiteral::Null)),
			_ => Ok(LogicalExpr::Literal(LogicalLiteral::Null)),
		},
		Expr::ColumnRef(col) => {
			if col.fields.len() == 1 {
				Ok(LogicalExpr::Field(Strand::from(col.fields[0].as_str())))
			} else if col.fields.len() == 2 {
				Ok(LogicalExpr::Access {
					base: Box::new(LogicalExpr::Field(Strand::from(col.fields[0].as_str()))),
					field: Strand::from(col.fields[1].as_str()),
				})
			} else {
				let mut current = LogicalExpr::Field(Strand::from(col.fields[0].as_str()));
				for f in &col.fields[1..] {
					current = LogicalExpr::Access {
						base: Box::new(current),
						field: Strand::from(f.as_str()),
					};
				}
				Ok(current)
			}
		}
		Expr::Param {
			id,
			..
		} => Ok(LogicalExpr::Param(surrealdb_expr::expr::Param::from(
			surrealdb_types::positional_param_name(*id as usize),
		))),
		Expr::UnaryOp {
			op,
			expr,
			..
		} => {
			let inner = lower_expr(expr)?;
			let prefix_op = match op {
				surrealdb_pgsql::ast::op::UnaryOperator::Not => PrefixOperator::Not,
				surrealdb_pgsql::ast::op::UnaryOperator::Plus => PrefixOperator::Positive,
				surrealdb_pgsql::ast::op::UnaryOperator::Minus => PrefixOperator::Negate,
				_ => {
					return Err(LowerError::UnsupportedExpr(format!("Unary op {op:?}")));
				}
			};
			Ok(LogicalExpr::Prefix {
				op: prefix_op,
				expr: Box::new(inner),
			})
		}
		Expr::BinaryOp {
			left,
			op,
			right,
			..
		} => {
			let l = lower_expr(left)?;
			let r = lower_expr(right)?;
			let bin_op = match op {
				surrealdb_pgsql::ast::op::BinaryOperator::Add => BinaryOperator::Add,
				surrealdb_pgsql::ast::op::BinaryOperator::Sub => BinaryOperator::Subtract,
				surrealdb_pgsql::ast::op::BinaryOperator::Mul => BinaryOperator::Multiply,
				surrealdb_pgsql::ast::op::BinaryOperator::Div => BinaryOperator::Divide,
				surrealdb_pgsql::ast::op::BinaryOperator::Mod => BinaryOperator::Remainder,
				surrealdb_pgsql::ast::op::BinaryOperator::Eq => BinaryOperator::Equal,
				surrealdb_pgsql::ast::op::BinaryOperator::NotEq => BinaryOperator::NotEqual,
				surrealdb_pgsql::ast::op::BinaryOperator::Lt => BinaryOperator::LessThan,
				surrealdb_pgsql::ast::op::BinaryOperator::LtEq => BinaryOperator::LessThanEqual,
				surrealdb_pgsql::ast::op::BinaryOperator::Gt => BinaryOperator::MoreThan,
				surrealdb_pgsql::ast::op::BinaryOperator::GtEq => BinaryOperator::MoreThanEqual,
				surrealdb_pgsql::ast::op::BinaryOperator::And => BinaryOperator::And,
				surrealdb_pgsql::ast::op::BinaryOperator::Or => BinaryOperator::Or,
				surrealdb_pgsql::ast::op::BinaryOperator::Concat => BinaryOperator::Add,
				_ => {
					return Err(LowerError::UnsupportedExpr(format!("Binary op {op:?}")));
				}
			};
			Ok(LogicalExpr::Binary {
				left: Box::new(l),
				op: bin_op,
				right: Box::new(r),
			})
		}
		Expr::FunctionCall {
			name,
			args,
			..
		} => {
			let fn_name = name.parts.last().map(|s| s.to_lowercase()).unwrap_or_default();
			let mapped_fn = match fn_name.as_str() {
				"count" => "count",
				"sum" => "math::sum",
				"avg" => "math::mean",
				"min" => "math::min",
				"max" => "math::max",
				"abs" => "math::abs",
				"lower" => "string::lowercase",
				"upper" => "string::uppercase",
				"length" => "string::len",
				other => other,
			};
			let lower_args = args.iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?;
			Ok(LogicalExpr::Call(Box::new(surrealdb_expr::expr::logical::LogicalCall {
				receiver: Function::Normal(mapped_fn.to_string()),
				args: lower_args,
			})))
		}
		Expr::Case {
			operand,
			branches,
			else_expr,
			..
		} => {
			let mut lower_branches = Vec::new();
			for when in branches {
				let cond = if let Some(base) = operand {
					LogicalExpr::Binary {
						left: Box::new(lower_expr(base)?),
						op: BinaryOperator::Equal,
						right: Box::new(lower_expr(&when.when)?),
					}
				} else {
					lower_expr(&when.when)?
				};
				let value = lower_expr(&when.then)?;
				lower_branches.push((cond, value));
			}
			let otherwise = else_expr.as_ref().map(|e| lower_expr(e)).transpose()?;
			Ok(LogicalExpr::If(Box::new(surrealdb_expr::expr::logical::LogicalIf {
				branches: lower_branches,
				otherwise,
			})))
		}
		Expr::Between {
			expr,
			low,
			high,
			negated,
			..
		} => {
			let val = lower_expr(expr)?;
			let l = lower_expr(low)?;
			let h = lower_expr(high)?;
			let ge = LogicalExpr::Binary {
				left: Box::new(val.clone()),
				op: BinaryOperator::MoreThanEqual,
				right: Box::new(l),
			};
			let le = LogicalExpr::Binary {
				left: Box::new(val),
				op: BinaryOperator::LessThanEqual,
				right: Box::new(h),
			};
			let between = LogicalExpr::Binary {
				left: Box::new(ge),
				op: BinaryOperator::And,
				right: Box::new(le),
			};
			if *negated {
				Ok(LogicalExpr::Prefix {
					op: PrefixOperator::Not,
					expr: Box::new(between),
				})
			} else {
				Ok(between)
			}
		}
		Expr::IsNull {
			expr,
			negated,
			..
		} => {
			let val = lower_expr(expr)?;
			let is_null = LogicalExpr::Binary {
				left: Box::new(val),
				op: BinaryOperator::Equal,
				right: Box::new(LogicalExpr::Literal(LogicalLiteral::Null)),
			};
			if *negated {
				Ok(LogicalExpr::Prefix {
					op: PrefixOperator::Not,
					expr: Box::new(is_null),
				})
			} else {
				Ok(is_null)
			}
		}
		Expr::InList {
			expr,
			list,
			negated,
			..
		} => {
			let val = lower_expr(expr)?;
			let items = list.iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?;
			let op = if *negated {
				BinaryOperator::NotInside
			} else {
				BinaryOperator::Inside
			};
			Ok(LogicalExpr::Binary {
				left: Box::new(val),
				op,
				right: Box::new(LogicalExpr::Array(items)),
			})
		}
		Expr::Grouping {
			expr,
			..
		} => lower_expr(expr),
		Expr::TypeCast {
			expr,
			..
		} => lower_expr(expr),
		_ => Err(LowerError::UnsupportedExpr(format!("{expr:?}"))),
	}
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
	match expr {
		Expr::FunctionCall {
			name,
			args,
			..
		} => {
			if let Some(fn_name) = name.parts.last() {
				let fn_lower = fn_name.to_ascii_lowercase();
				if matches!(
					fn_lower.as_str(),
					"count" | "sum" | "avg" | "min" | "max" | "string_agg" | "array_agg"
				) {
					return true;
				}
			}
			args.iter().any(expr_contains_aggregate)
		}
		Expr::UnaryOp {
			expr,
			..
		} => expr_contains_aggregate(expr),
		Expr::BinaryOp {
			left,
			right,
			..
		} => expr_contains_aggregate(left) || expr_contains_aggregate(right),
		Expr::TypeCast {
			expr,
			..
		} => expr_contains_aggregate(expr),
		Expr::Between {
			expr,
			low,
			high,
			..
		} => {
			expr_contains_aggregate(expr)
				|| expr_contains_aggregate(low)
				|| expr_contains_aggregate(high)
		}
		Expr::InList {
			expr,
			list,
			..
		} => expr_contains_aggregate(expr) || list.iter().any(expr_contains_aggregate),
		Expr::Case {
			operand,
			branches,
			else_expr,
			..
		} => {
			operand.as_ref().is_some_and(|e| expr_contains_aggregate(e))
				|| branches
					.iter()
					.any(|b| expr_contains_aggregate(&b.when) || expr_contains_aggregate(&b.then))
				|| else_expr.as_ref().is_some_and(|e| expr_contains_aggregate(e))
		}
		Expr::Grouping {
			expr,
			..
		} => expr_contains_aggregate(expr),
		_ => false,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_lower_simple_select() {
		let stmts = surrealdb_pgsql::parse_statements("SELECT 1 AS num").expect("parse");
		let op = lower_stmt(&stmts[0]).expect("lower");
		match op {
			LogicalOp::Project(proj) => {
				assert!(matches!(proj.input.as_ref(), LogicalOp::Values(_)));
				assert!(matches!(&proj.fields, LogicalFields::Select(f) if f.len() == 1));
			}
			other => panic!("expected Project, got {other:?}"),
		}
	}

	#[test]
	fn test_lower_select_from_where_order_limit() {
		let stmts = surrealdb_pgsql::parse_statements(
			"SELECT id, name FROM users WHERE age > 18 ORDER BY name ASC LIMIT 10 OFFSET 5",
		)
		.expect("parse");
		let op = lower_stmt(&stmts[0]).expect("lower");
		match op {
			LogicalOp::Project(proj) => match proj.input.as_ref() {
				LogicalOp::Limit(lim) => {
					assert!(lim.limit.is_some());
					assert!(lim.start.is_some());
					match lim.input.as_ref() {
						LogicalOp::Sort(sort) => match sort.input.as_ref() {
							LogicalOp::Filter(filter) => match filter.input.as_ref() {
								LogicalOp::Source(src) => {
									assert!(matches!(src.source, SourceRef::Table(_)));
								}
								other => panic!("expected Source, got {other:?}"),
							},
							other => panic!("expected Filter, got {other:?}"),
						},
						other => panic!("expected Sort, got {other:?}"),
					}
				}
				other => panic!("expected Limit, got {other:?}"),
			},
			other => panic!("expected Project, got {other:?}"),
		}
	}

	#[test]
	fn test_lower_join() {
		let stmts = surrealdb_pgsql::parse_statements(
			"SELECT u.id, o.id FROM users JOIN orders ON users.id = orders.user_id",
		)
		.expect("parse");
		let op = lower_stmt(&stmts[0]).expect("lower");
		match op {
			LogicalOp::Project(proj) => match proj.input.as_ref() {
				LogicalOp::Join(join) => {
					assert_eq!(join.kind, surrealdb_expr::expr::logical::JoinKind::Inner);
					assert_eq!(join.on.len(), 1);
				}
				other => panic!("expected Join, got {other:?}"),
			},
			other => panic!("expected Project, got {other:?}"),
		}
	}

	#[test]
	fn test_lower_aggregate_without_group_by() {
		let stmts = surrealdb_pgsql::parse_statements("SELECT count(*) FROM users").expect("parse");
		let op = lower_stmt(&stmts[0]).expect("lower");
		match op {
			LogicalOp::Project(proj) => match proj.input.as_ref() {
				LogicalOp::Aggregate(agg) => {
					assert!(agg.group.is_empty(), "whole-table aggregate has empty group");
					assert!(matches!(agg.input.as_ref(), LogicalOp::Source(_)));
				}
				other => panic!("expected Aggregate, got {other:?}"),
			},
			other => panic!("expected Project, got {other:?}"),
		}
	}

	#[test]
	fn test_lower_group_by_ordinal_rejected() {
		let stmts = surrealdb_pgsql::parse_statements("SELECT id, name FROM users GROUP BY 1")
			.expect("parse");
		let Statement::Select(ref select) = stmts[0] else {
			panic!("expected select")
		};
		let err = lower_select(select).expect_err("should reject ordinal");
		assert!(
			matches!(&err, LowerError::UnsupportedExpr(msg) if msg.contains("GROUP BY ordinal")),
			"expected GROUP BY ordinal rejection, got {err:?}"
		);
	}
}
