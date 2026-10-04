//! PostgreSQL AST to SurrealDB `LogicalOp` plan lowering.

use surrealdb_expr::expr::logical::{LogicalDistinct, LogicalOp, LogicalUnionAll};
use surrealdb_pgsql::ast::stmt::{SelectStmt, SetOpKind, Statement};
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

impl From<surrealdb_sqlplan::PlanError> for LowerError {
	fn from(e: surrealdb_sqlplan::PlanError) -> Self {
		match e {
			surrealdb_sqlplan::PlanError::UnsupportedExpression(s) => {
				LowerError::UnsupportedExpr(s)
			}
			surrealdb_sqlplan::PlanError::UnsupportedStatement(s) => LowerError::UnsupportedStmt(s),
			other => LowerError::Custom(other.to_string()),
		}
	}
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
	let policy = surrealdb_sqlplan::DialectPolicy::postgres();
	let catalog = surrealdb_sqlplan::EmptyCatalog;
	let mut op = surrealdb_sqlplan::bind_and_lower_pgsql(select, &catalog, &policy)?;

	// SET OPERATION (UNION, INTERSECT, EXCEPT)
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

#[cfg(test)]
mod tests {
	use surrealdb_expr::expr::logical::{LogicalFields, SourceRef};

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
			"SELECT u.id, o.id FROM users u JOIN orders o ON u.id = o.user_id",
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
	fn test_lower_group_by_ordinal_supported() {
		let stmts = surrealdb_pgsql::parse_statements("SELECT id, name FROM users GROUP BY 1")
			.expect("parse");
		let Statement::Select(ref select) = stmts[0] else {
			panic!("expected select")
		};
		let op = lower_select(select).expect("should succeed with ordinal");
		assert!(matches!(op, LogicalOp::Project(_)));
	}
}
