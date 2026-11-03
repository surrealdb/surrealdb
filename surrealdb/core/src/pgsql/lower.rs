//! PostgreSQL AST to SurrealDB `LogicalOp` plan lowering.

use surrealdb_expr::expr::logical::LogicalOp;
use surrealdb_pgsql::ast::stmt::{SelectStmt, Statement};
use thiserror::Error;

/// Error type for lowering failures.
///
/// Each variant displays as the planner's own message for the failure, so the message names the
/// failure once, however many layers wrap it.
#[derive(Debug, Error)]
pub enum LowerError {
	#[error("unsupported expression: {0}")]
	UnsupportedExpr(String),
	#[error("unsupported statement: {0}")]
	UnsupportedStmt(String),
	#[error("{0}")]
	Custom(String),
}

impl From<surrealdb_sqlplan::PlanError> for LowerError {
	fn from(e: surrealdb_sqlplan::PlanError) -> Self {
		match e {
			surrealdb_sqlplan::PlanError::UnsupportedExpression(s) => {
				LowerError::UnsupportedExpr(s)
			}
			surrealdb_sqlplan::PlanError::UnsupportedStatement(s) => LowerError::UnsupportedStmt(s),
			other @ (surrealdb_sqlplan::PlanError::UndefinedTable(_)
			| surrealdb_sqlplan::PlanError::UndefinedColumn(_)
			| surrealdb_sqlplan::PlanError::AmbiguousColumn(_)
			| surrealdb_sqlplan::PlanError::MissingFromClauseEntry(_)
			| surrealdb_sqlplan::PlanError::InvalidGroupByPosition(_)
			| surrealdb_sqlplan::PlanError::InvalidOrderByPosition(_)
			| surrealdb_sqlplan::PlanError::DistinctOrderByNotSelected
			| surrealdb_sqlplan::PlanError::ColumnNotInGroupBy(_)
			| surrealdb_sqlplan::PlanError::AggregateNotAllowed(_)
			| surrealdb_sqlplan::PlanError::WindowNotAllowed(_)
			| surrealdb_sqlplan::PlanError::WindowPartitionOverride(_)
			| surrealdb_sqlplan::PlanError::WindowOrderOverride(_)
			| surrealdb_sqlplan::PlanError::WindowFrameCopy(_)
			| surrealdb_sqlplan::PlanError::UndefinedFunction(_)
			| surrealdb_sqlplan::PlanError::UndefinedOperator(_)
			| surrealdb_sqlplan::PlanError::FunctionArgumentCountMismatch {
				..
			}
			| surrealdb_sqlplan::PlanError::WindowFunctionWithoutOver(_)
			| surrealdb_sqlplan::PlanError::SubqueryMustHaveAlias
			| surrealdb_sqlplan::PlanError::ParameterOutOfRange(_)
			| surrealdb_sqlplan::PlanError::DivisionByZero
			| surrealdb_sqlplan::PlanError::NumericValueOutOfRange
			| surrealdb_sqlplan::PlanError::ValueOutOfRange(_)
			| surrealdb_sqlplan::PlanError::InvalidTextRepresentation(_)
			| surrealdb_sqlplan::PlanError::NoActiveTransaction
			| surrealdb_sqlplan::PlanError::InFailedTransaction
			| surrealdb_sqlplan::PlanError::SavepointDoesNotExist(_)
			| surrealdb_sqlplan::PlanError::ActiveTransactionAlreadyInProgress
			| surrealdb_sqlplan::PlanError::UndefinedObject(_)
			| surrealdb_sqlplan::PlanError::UndefinedConstraint {
				..
			}
			| surrealdb_sqlplan::PlanError::UndefinedType(_)
			| surrealdb_sqlplan::PlanError::UndefinedDomainConstraint {
				..
			}
			| surrealdb_sqlplan::PlanError::DuplicateDomainConstraint {
				..
			}
			| surrealdb_sqlplan::PlanError::DuplicateEnumLabel(_)
			| surrealdb_sqlplan::PlanError::DuplicateColumn(_)
			| surrealdb_sqlplan::PlanError::InvalidParameterValue(_)
			| surrealdb_sqlplan::PlanError::DatatypeMismatch(_)
			| surrealdb_sqlplan::PlanError::DataException(_)
			| surrealdb_sqlplan::PlanError::ProgramLimitExceeded(_)
			| surrealdb_sqlplan::PlanError::UndefinedOperatorClass {
				..
			}
			| surrealdb_sqlplan::PlanError::NoDefaultOperatorClass {
				..
			}
			| surrealdb_sqlplan::PlanError::ExtensionNotAvailable(_)
			| surrealdb_sqlplan::PlanError::Custom(_)) => LowerError::Custom(other.to_string()),
		}
	}
}

/// Indicates a statement has no direct logical plan representation and is lowered
/// to a SurrealQL statement instead.
#[derive(Debug)]
pub enum NoLogicalForm<'a> {
	NonSelectStatement(&'a Statement),
}

/// Lower a PostgreSQL statement directly to a `LogicalOp`.
///
/// Returns `Ok(LogicalOp)` for statements that lower to the engine (e.g. `SELECT`),
/// or `Err(NoLogicalForm)` for statements lowered to SurrealQL statements instead (e.g.
/// `INSERT`, `CREATE`).
pub fn lower_stmt(stmt: &Statement) -> Result<LogicalOp, NoLogicalForm<'_>> {
	if let Statement::Select(select) = stmt {
		lower_select(select).map_err(|_| NoLogicalForm::NonSelectStatement(stmt))
	} else {
		Err(NoLogicalForm::NonSelectStatement(stmt))
	}
}

/// Lower a PostgreSQL `SELECT` statement directly to a `LogicalOp` pipeline, against a
/// catalog that describes no table.
pub fn lower_select(select: &SelectStmt) -> Result<LogicalOp, LowerError> {
	lower_select_with(select, &surrealdb_sqlplan::EmptyCatalog)
}

/// Lower a PostgreSQL `SELECT` statement directly to a `LogicalOp` pipeline, against `catalog`.
pub fn lower_select_with(
	select: &SelectStmt,
	catalog: &dyn surrealdb_sqlplan::BinderCatalog,
) -> Result<LogicalOp, LowerError> {
	let policy = surrealdb_sqlplan::DialectPolicy::postgres();
	let op = surrealdb_sqlplan::bind_and_lower_pgsql(select, catalog, &policy)?;
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
		let LogicalOp::Project(proj) = op else {
			panic!("expected Project, got {op:?}");
		};
		assert!(matches!(proj.input.as_ref(), LogicalOp::Values(_)));
		assert!(matches!(&proj.fields, LogicalFields::Select(f) if f.len() == 1));
	}

	#[test]
	fn test_lower_select_from_where_order_limit() {
		let stmts = surrealdb_pgsql::parse_statements(
			"SELECT id, name FROM users WHERE age > 18 ORDER BY name ASC LIMIT 10 OFFSET 5",
		)
		.expect("parse");
		let op = lower_stmt(&stmts[0]).expect("lower");
		let LogicalOp::Project(proj) = op else {
			panic!("expected Project, got {op:?}");
		};
		let LogicalOp::Limit(lim) = proj.input.as_ref() else {
			panic!("expected Limit, got {:?}", proj.input);
		};
		assert!(lim.limit.is_some());
		assert!(lim.start.is_some());
		let LogicalOp::Sort(sort) = lim.input.as_ref() else {
			panic!("expected Sort, got {:?}", lim.input);
		};
		let LogicalOp::Filter(filter) = sort.input.as_ref() else {
			panic!("expected Filter, got {:?}", sort.input);
		};
		let LogicalOp::Source(src) = filter.input.as_ref() else {
			panic!("expected Source, got {:?}", filter.input);
		};
		assert!(matches!(src.source, SourceRef::Table(_)));
	}

	#[test]
	fn test_lower_join() {
		let stmts = surrealdb_pgsql::parse_statements(
			"SELECT u.id, o.id FROM users u JOIN orders o ON u.id = o.user_id",
		)
		.expect("parse");
		let op = lower_stmt(&stmts[0]).expect("lower");
		let LogicalOp::Project(proj) = op else {
			panic!("expected Project, got {op:?}");
		};
		let LogicalOp::Join(join) = proj.input.as_ref() else {
			panic!("expected Join, got {:?}", proj.input);
		};
		assert_eq!(join.kind, surrealdb_expr::expr::logical::JoinKind::Inner);
		assert_eq!(join.on.len(), 1);
	}

	#[test]
	fn test_lower_aggregate_without_group_by() {
		let stmts = surrealdb_pgsql::parse_statements("SELECT count(*) FROM users").expect("parse");
		let op = lower_stmt(&stmts[0]).expect("lower");
		let LogicalOp::Project(proj) = op else {
			panic!("expected Project, got {op:?}");
		};
		let LogicalOp::Aggregate(agg) = proj.input.as_ref() else {
			panic!("expected Aggregate, got {:?}", proj.input);
		};
		assert!(agg.group.is_empty(), "whole-table aggregate has empty group");
		assert!(matches!(agg.input.as_ref(), LogicalOp::Source(_)));
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
