use std::sync::Arc;

use crate::err::Error;
use crate::exec::{PlannedStatement, SessionCommand};
use crate::expr::{Expr, TopLevelExpr};

/// Attempts to convert a logical plan to an execution plan.
///
/// If the conversion is not possible, the original plan and the error are returned.
#[allow(clippy::result_large_err)]
pub(crate) fn logical_plan_to_execution_plan(
	plan: crate::expr::LogicalPlan,
) -> Result<Vec<PlannedStatement>, (crate::expr::LogicalPlan, Error)> {
	let mut execution_plans = Vec::with_capacity(plan.expressions.len());
	for expr in plan.expressions.clone() {
		match top_level_expr_to_execution_plan(expr) {
			Ok(plan) => execution_plans.push(plan),
			Err(e) => return Err((plan, e)),
		}
	}

	Ok(execution_plans)
}

fn top_level_expr_to_execution_plan(expr: TopLevelExpr) -> Result<PlannedStatement, Error> {
	match expr {
		TopLevelExpr::Begin => Ok(PlannedStatement::SessionCommand(SessionCommand::Begin)),
		TopLevelExpr::Cancel => Ok(PlannedStatement::SessionCommand(SessionCommand::Cancel)),
		TopLevelExpr::Commit => Ok(PlannedStatement::SessionCommand(SessionCommand::Commit)),
		TopLevelExpr::Use(use_stmt) => convert_use_statement(use_stmt),
		TopLevelExpr::Access(_) => Err(Error::Unimplemented(
			"ACCESS statements not yet supported in execution plans".to_string(),
		)),
		TopLevelExpr::Kill(_) => Err(Error::Unimplemented(
			"KILL statements not yet supported in execution plans".to_string(),
		)),
		TopLevelExpr::Live(_) => Err(Error::Unimplemented(
			"LIVE statements not yet supported in execution plans".to_string(),
		)),
		TopLevelExpr::Show(_) => Err(Error::Unimplemented(
			"SHOW statements not yet supported in execution plans".to_string(),
		)),
		TopLevelExpr::Option(_) => Err(Error::Unimplemented(
			"OPTION statements not yet supported in execution plans".to_string(),
		)),
		TopLevelExpr::Expr(expr) => expr_to_execution_plan(expr),
	}
}

fn convert_use_statement(
	use_stmt: crate::expr::statements::UseStatement,
) -> Result<PlannedStatement, Error> {
	use crate::expr::statements::UseStatement;

	match use_stmt {
		UseStatement::Ns(ns_expr) => {
			let ns = expr_to_physical_expr(ns_expr)?;
			// Validate that it doesn't reference current_value
			if ns.references_current_value() {
				return Err(Error::Unimplemented(
					"USE NS expression cannot reference current row".to_string(),
				));
			}
			Ok(PlannedStatement::SessionCommand(SessionCommand::Use {
				ns: Some(ns),
				db: None,
			}))
		}
		UseStatement::Db(db_expr) => {
			let db = expr_to_physical_expr(db_expr)?;
			// Validate that it doesn't reference current_value
			if db.references_current_value() {
				return Err(Error::Unimplemented(
					"USE DB expression cannot reference current row".to_string(),
				));
			}
			Ok(PlannedStatement::SessionCommand(SessionCommand::Use {
				ns: None,
				db: Some(db),
			}))
		}
		UseStatement::NsDb(ns_expr, db_expr) => {
			let ns = expr_to_physical_expr(ns_expr)?;
			let db = expr_to_physical_expr(db_expr)?;
			// Validate that they don't reference current_value
			if ns.references_current_value() || db.references_current_value() {
				return Err(Error::Unimplemented(
					"USE expression cannot reference current row".to_string(),
				));
			}
			Ok(PlannedStatement::SessionCommand(SessionCommand::Use {
				ns: Some(ns),
				db: Some(db),
			}))
		}
		UseStatement::Default => {
			// USE DEFAULT - no expressions to evaluate
			Ok(PlannedStatement::SessionCommand(SessionCommand::Use {
				ns: None,
				db: None,
			}))
		}
	}
}

fn expr_to_physical_expr(expr: Expr) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
	use crate::exec::{Field, Literal as PhysicalLiteral, Param};

	match expr {
		Expr::Literal(lit) => {
			// Convert the logical Literal to a physical Value
			let value = literal_to_value(lit)?;
			Ok(Arc::new(PhysicalLiteral(value)))
		}
		Expr::Param(param) => Ok(Arc::new(Param(param.as_str().to_string()))),
		Expr::Idiom(idiom) => Ok(Arc::new(Field(idiom))),
		_ => Err(Error::Unimplemented(format!(
			"Expression type not yet supported in execution plans: {:?}",
			expr
		))),
	}
}

/// Convert a Literal to a Value for static (non-computed) cases
/// This is used for USE statement expressions which must be static
fn literal_to_value(lit: crate::expr::literal::Literal) -> Result<crate::val::Value, Error> {
	use crate::expr::literal::Literal;
	use crate::val::{Number, Range, Value};

	match lit {
		Literal::None => Ok(Value::None),
		Literal::Null => Ok(Value::Null),
		Literal::UnboundedRange => Ok(Value::Range(Box::new(Range::unbounded()))),
		Literal::Bool(x) => Ok(Value::Bool(x)),
		Literal::Float(x) => Ok(Value::Number(Number::Float(x))),
		Literal::Integer(i) => Ok(Value::Number(Number::Int(i))),
		Literal::Decimal(d) => Ok(Value::Number(Number::Decimal(d))),
		Literal::String(s) => Ok(Value::String(s)),
		Literal::Bytes(b) => Ok(Value::Bytes(b)),
		Literal::Regex(r) => Ok(Value::Regex(r)),
		Literal::Duration(d) => Ok(Value::Duration(d)),
		Literal::Datetime(dt) => Ok(Value::Datetime(dt)),
		Literal::Uuid(u) => Ok(Value::Uuid(u)),
		Literal::Geometry(g) => Ok(Value::Geometry(g)),
		Literal::File(f) => Ok(Value::File(f)),
		// These require async computation, not allowed for USE statements
		Literal::RecordId(_) => Err(Error::Unimplemented(
			"RecordId literals in USE statements not yet supported".to_string(),
		)),
		Literal::Array(_) => Err(Error::Unimplemented(
			"Array literals in USE statements not yet supported".to_string(),
		)),
		Literal::Set(_) => Err(Error::Unimplemented(
			"Set literals in USE statements not yet supported".to_string(),
		)),
		Literal::Object(_) => Err(Error::Unimplemented(
			"Object literals in USE statements not yet supported".to_string(),
		)),
	}
}

fn expr_to_execution_plan(_expr: Expr) -> Result<PlannedStatement, Error> {
	Err(Error::Unimplemented("Query execution plans not yet implemented".to_string()))
}
