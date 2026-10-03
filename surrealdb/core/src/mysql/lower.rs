//! MySQL AST to SurrealDB `LogicalOp` plan lowering.

use surrealdb_expr::expr::logical::{
	EvalFrame, LogicalAggregate, LogicalDistinct, LogicalExpr, LogicalField, LogicalFields,
	LogicalFilter, LogicalJoin, LogicalLimit, LogicalLiteral, LogicalOp, LogicalOrder,
	LogicalOrdering, LogicalProject, LogicalSort, LogicalSource, LogicalSubquery, LogicalValues,
	OutputPath, RelationAlias, SourceRef,
};
use surrealdb_expr::expr::{BinaryOperator, Function, PrefixOperator};
use surrealdb_mysql::ast::expr::Expr;
use surrealdb_mysql::ast::literal::Literal;
use surrealdb_mysql::ast::stmt::{
	JoinType, OrderByExpr, SelectItem, SelectStmt, Statement, TableRef,
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

/// Lower a MySQL statement directly to a `LogicalOp`.
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

/// Lower a MySQL `SELECT` statement directly to a `LogicalOp` pipeline.
pub fn lower_select(select: &SelectStmt) -> Result<LogicalOp, LowerError> {
	// 1. FROM clause
	let mut op = match &select.from {
		None => LogicalOp::Values(LogicalValues {
			expr: LogicalExpr::Array(vec![LogicalExpr::Object(vec![])]),
			for_update: false,
		}),
		Some(table_ref) => lower_table_ref(table_ref, select.lock.is_some())?,
	};

	// 2. WHERE clause
	if let Some(where_expr) = &select.where_clause {
		op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(op),
			predicate: lower_expr(where_expr)?,
		});
	}

	// 3. GROUP BY & Projection preparation
	let fields = lower_projection(&select.projection)?;
	let grouped = !select.group_by.is_empty();
	if grouped {
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
	if select.distinct {
		op = LogicalOp::Distinct(LogicalDistinct {
			input: Box::new(op),
		});
	}

	// 6. ORDER BY
	if !select.order_by.is_empty() {
		let orders = select.order_by.iter().map(lower_order_by).collect::<Result<Vec<_>, _>>()?;
		op = LogicalOp::Sort(LogicalSort {
			input: Box::new(op),
			ordering: LogicalOrdering::Order(orders),
			tempfiles: false,
		});
	}

	// 7. LIMIT / OFFSET
	if let Some(limit_clause) = &select.limit {
		let limit = Some(lower_expr(&limit_clause.count)?);
		let start = limit_clause.offset.as_ref().map(lower_expr).transpose()?;
		op = LogicalOp::Limit(LogicalLimit {
			input: Box::new(op),
			limit,
			start,
		});
	}

	// 8. Projection
	if !grouped {
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
			subquery,
			..
		} => {
			let inner = lower_select(subquery)?;
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
			if let Some(surrealdb_mysql::ast::stmt::JoinCondition::On(expr)) = condition {
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
	}
}

fn lower_projection(items: &[SelectItem]) -> Result<LogicalFields, LowerError> {
	let mut fields = Vec::new();
	for item in items {
		match item {
			SelectItem::Wildcard(_) => {
				fields.push(LogicalField::All);
			}
			SelectItem::QualifiedWildcard(_, _) => {
				fields.push(LogicalField::All);
			}
			SelectItem::Expr {
				expr,
				alias,
				..
			} => {
				let lowered = lower_expr(expr)?;
				let out_alias = alias.as_ref().map(|a| OutputPath::field(a.clone()));
				fields.push(LogicalField::user(lowered, out_alias));
			}
		}
	}
	Ok(LogicalFields::Select(fields))
}

fn lower_order_by(item: &OrderByExpr) -> Result<LogicalOrder, LowerError> {
	let value = lower_expr(&item.expr)?;
	Ok(LogicalOrder {
		value,
		collate: false,
		numeric: false,
		direction: item.asc,
	})
}

/// Lower a MySQL AST expression into a SurrealDB `LogicalExpr`.
pub fn lower_expr(expr: &Expr) -> Result<LogicalExpr, LowerError> {
	match expr {
		Expr::Literal(lit, _) => match lit {
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
		Expr::SystemVar {
			name,
			..
		} => {
			let val = match name.to_lowercase().as_str() {
				"version_comment" => "SurrealDB Community",
				"version" => "8.0.32-SurrealDB",
				_ => "SurrealDB",
			};
			Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from(val))))
		}
		Expr::UserVar(name, _) => {
			Ok(LogicalExpr::Param(surrealdb_expr::expr::Param::from(name.clone())))
		}
		Expr::Param(idx, _) => {
			Ok(LogicalExpr::Param(surrealdb_expr::expr::Param::from(format!("param_{idx}"))))
		}
		Expr::UnaryOp {
			op,
			expr,
			..
		} => {
			let inner = lower_expr(expr)?;
			let prefix_op = match op {
				surrealdb_mysql::ast::operator::UnaryOperator::Not => PrefixOperator::Not,
				surrealdb_mysql::ast::operator::UnaryOperator::Plus => PrefixOperator::Positive,
				surrealdb_mysql::ast::operator::UnaryOperator::Minus => PrefixOperator::Negate,
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
				surrealdb_mysql::ast::operator::BinaryOperator::Add => BinaryOperator::Add,
				surrealdb_mysql::ast::operator::BinaryOperator::Sub => BinaryOperator::Subtract,
				surrealdb_mysql::ast::operator::BinaryOperator::Mul => BinaryOperator::Multiply,
				surrealdb_mysql::ast::operator::BinaryOperator::Div => BinaryOperator::Divide,
				surrealdb_mysql::ast::operator::BinaryOperator::Mod => BinaryOperator::Remainder,
				surrealdb_mysql::ast::operator::BinaryOperator::Equal
				| surrealdb_mysql::ast::operator::BinaryOperator::NullSafeEqual => BinaryOperator::Equal,
				surrealdb_mysql::ast::operator::BinaryOperator::NotEqual => {
					BinaryOperator::NotEqual
				}
				surrealdb_mysql::ast::operator::BinaryOperator::Less => BinaryOperator::LessThan,
				surrealdb_mysql::ast::operator::BinaryOperator::LessEqual => {
					BinaryOperator::LessThanEqual
				}
				surrealdb_mysql::ast::operator::BinaryOperator::Greater => BinaryOperator::MoreThan,
				surrealdb_mysql::ast::operator::BinaryOperator::GreaterEqual => {
					BinaryOperator::MoreThanEqual
				}
				surrealdb_mysql::ast::operator::BinaryOperator::And => BinaryOperator::And,
				surrealdb_mysql::ast::operator::BinaryOperator::Or => BinaryOperator::Or,
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
			let fn_name = name.to_lowercase();
			let mapped_fn = match fn_name.as_str() {
				"count" => "count",
				"sum" => "math::sum",
				"avg" => "math::mean",
				"min" => "math::min",
				"max" => "math::max",
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
		Expr::Subquery {
			subquery,
			..
		} => {
			let plan = lower_select(subquery)?;
			Ok(LogicalExpr::Subquery(Box::new(plan)))
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
			negated,
			low,
			high,
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
		Expr::Cast {
			expr,
			..
		} => lower_expr(expr),
		_ => Err(LowerError::UnsupportedExpr(format!("{expr:?}"))),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_lower_simple_select() {
		let stmts = surrealdb_mysql::parse_statements("SELECT 1 AS num").expect("parse");
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
		let stmts = surrealdb_mysql::parse_statements(
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
		let stmts = surrealdb_mysql::parse_statements(
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
}
