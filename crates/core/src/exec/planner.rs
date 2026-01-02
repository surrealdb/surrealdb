use std::sync::Arc;

use crate::err::Error;
use crate::exec::operators::{
	ContentSource, Create, Delete, Filter, Project, RecordIdLookup, Scan, SetField, Union, Update,
};
use crate::exec::{ExecutionPlan, PlannedStatement, SessionCommand};
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
		TopLevelExpr::Begin => Err(Error::Unimplemented(
			"BEGIN transactions not yet supported in execution plans".to_string(),
		)),
		TopLevelExpr::Cancel => {
			Err(Error::Unimplemented("CANCEL not yet supported in execution plans".to_string()))
		}
		TopLevelExpr::Commit => {
			Err(Error::Unimplemented("COMMIT not yet supported in execution plans".to_string()))
		}
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

pub(crate) fn expr_to_physical_expr(
	expr: Expr,
) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
	use crate::exec::physical_expr::{BinaryOp, Field, Literal as PhysicalLiteral, Param};

	match expr {
		Expr::Literal(lit) => {
			// Convert the logical Literal to a physical Value
			let value = literal_to_value(lit)?;
			Ok(Arc::new(PhysicalLiteral(value)))
		}
		Expr::Param(param) => Ok(Arc::new(Param(param.as_str().to_string()))),
		Expr::Idiom(idiom) => Ok(Arc::new(Field(idiom))),
		Expr::Binary {
			left,
			op,
			right,
		} => {
			let left_phys = expr_to_physical_expr(*left)?;
			let right_phys = expr_to_physical_expr(*right)?;
			Ok(Arc::new(BinaryOp {
				left: left_phys,
				op,
				right: right_phys,
			}))
		}
		_ => Err(Error::Unimplemented(format!(
			"Expression type not yet supported in execution plans: {:?}",
			std::mem::discriminant(&expr)
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

fn expr_to_execution_plan(expr: Expr) -> Result<PlannedStatement, Error> {
	match expr {
		Expr::Select(select) => plan_select(*select),
		Expr::Create(create) => plan_create(*create),
		Expr::Update(update) => plan_update(*update),
		Expr::Delete(delete) => plan_delete(*delete),
		_ => Err(Error::Unimplemented(format!(
			"Expression type not yet supported in execution plans: {:?}",
			std::mem::discriminant(&expr)
		))),
	}
}

/// Plan a SELECT statement
fn plan_select(
	select: crate::expr::statements::SelectStatement,
) -> Result<PlannedStatement, Error> {
	// Build the source plan from `what` (FROM clause)
	let source = plan_select_sources(&select.what)?;

	// Apply WHERE clause if present
	let plan = if let Some(cond) = select.cond {
		let predicate = expr_to_physical_expr(cond.0)?;
		Arc::new(Filter {
			input: source,
			predicate,
		}) as Arc<dyn ExecutionPlan>
	} else {
		source
	};

	// TODO: Handle projections (select.expr), GROUP BY, ORDER BY, LIMIT, etc.
	// For now, we only support SELECT * (all fields)

	Ok(PlannedStatement::Query(plan))
}

/// Plan the FROM sources - handles multiple targets with Union
fn plan_select_sources(what: &[Expr]) -> Result<Arc<dyn ExecutionPlan>, Error> {
	if what.is_empty() {
		return Err(Error::Unimplemented("SELECT requires at least one source".to_string()));
	}

	// Convert each source to a plan
	let mut source_plans = Vec::with_capacity(what.len());
	for expr in what {
		let plan = plan_single_source(expr)?;
		source_plans.push(plan);
	}

	// If multiple sources, wrap in Union; otherwise just return the single source
	if source_plans.len() == 1 {
		Ok(source_plans.pop().unwrap())
	} else {
		Ok(Arc::new(Union {
			inputs: source_plans,
		}))
	}
}

/// Plan a single FROM source (table or record ID)
fn plan_single_source(expr: &Expr) -> Result<Arc<dyn ExecutionPlan>, Error> {
	match expr {
		// Table name: SELECT * FROM users
		Expr::Table(table_name) => {
			// Convert table name to a literal string for the physical expression
			let table_expr = expr_to_physical_expr(Expr::Literal(
				crate::expr::literal::Literal::String(table_name.as_str().to_string()),
			))?;
			Ok(Arc::new(Scan {
				table: table_expr,
			}))
		}

		// Record ID literal: SELECT * FROM users:123
		Expr::Literal(crate::expr::literal::Literal::RecordId(record_id_lit)) => {
			// Convert the RecordIdLit to an actual RecordId
			// For now, we only support static record IDs (table:key)
			// More complex expressions would need async evaluation
			let record_id = record_id_lit_to_record_id(record_id_lit)?;
			Ok(Arc::new(RecordIdLookup {
				record_id,
			}))
		}

		// Idiom that might be a table or record reference
		Expr::Idiom(idiom) => {
			// Simple idiom (just a name) is a table reference
			// Convert to a table scan using the idiom as a physical expression
			let table_expr = expr_to_physical_expr(Expr::Idiom(idiom.clone()))?;
			Ok(Arc::new(Scan {
				table: table_expr,
			}))
		}

		// Parameter that will be resolved at runtime
		Expr::Param(param) => {
			// Parameters could be record IDs or table names
			// We'll treat them as table references - Scan evaluates at runtime
			let table_expr = expr_to_physical_expr(Expr::Param(param.clone()))?;
			Ok(Arc::new(Scan {
				table: table_expr,
			}))
		}

		_ => Err(Error::Unimplemented(format!(
			"Unsupported FROM source type: {:?}",
			std::mem::discriminant(expr)
		))),
	}
}

/// Convert a RecordIdLit to an actual RecordId
/// For now, only supports static key patterns (Number, String, Uuid)
fn record_id_lit_to_record_id(
	record_id_lit: &crate::expr::RecordIdLit,
) -> Result<crate::val::RecordId, Error> {
	use crate::expr::record_id::RecordIdKeyLit;
	use crate::val::RecordIdKey;

	let key = match &record_id_lit.key {
		RecordIdKeyLit::Number(n) => RecordIdKey::Number(*n),
		RecordIdKeyLit::String(s) => RecordIdKey::String(s.clone()),
		RecordIdKeyLit::Uuid(u) => RecordIdKey::Uuid(*u),
		RecordIdKeyLit::Generate(generator) => generator.compute(),
		RecordIdKeyLit::Array(_) => {
			return Err(Error::Unimplemented(
				"Array record keys not yet supported in execution plans".to_string(),
			));
		}
		RecordIdKeyLit::Object(_) => {
			return Err(Error::Unimplemented(
				"Object record keys not yet supported in execution plans".to_string(),
			));
		}
		RecordIdKeyLit::Range(_) => {
			return Err(Error::Unimplemented(
				"Range record keys not yet supported in execution plans".to_string(),
			));
		}
	};

	Ok(crate::val::RecordId {
		table: record_id_lit.table.clone(),
		key,
	})
}

/// Plan a CREATE statement
fn plan_create(
	create: crate::expr::statements::CreateStatement,
) -> Result<PlannedStatement, Error> {
	// Get the table name from the first target
	let table = match create.what.first() {
		Some(Expr::Table(table_name)) => table_name.clone(),
		Some(Expr::Literal(crate::expr::literal::Literal::RecordId(rid))) => rid.table.clone(),
		_ => {
			return Err(Error::Unimplemented(
				"CREATE target must be a table or record ID".to_string(),
			));
		}
	};

	// Convert content to ContentSource
	let content = if let Some(data) = create.data {
		use crate::expr::Data;
		match data {
			Data::EmptyExpression => {
				ContentSource::Value(crate::val::Value::Object(crate::val::Object::default()))
			}
			Data::SetExpression(_sets) => {
				// Convert set expressions to a value
				// For now, we need the old compute system to fully evaluate these
				return Err(Error::Unimplemented(
					"CREATE with SET expressions not yet supported in execution plans".to_string(),
				));
			}
			Data::SingleExpression(expr) => {
				// Convert expression to value
				if let Expr::Literal(lit) = expr {
					match literal_to_value(lit) {
						Ok(val) => ContentSource::Value(val),
						Err(_) => {
							return Err(Error::Unimplemented(
								"Complex literals in CREATE not yet supported".to_string(),
							));
						}
					}
				} else {
					return Err(Error::Unimplemented(
						"Complex expressions in CREATE content not yet supported".to_string(),
					));
				}
			}
			Data::ValuesExpression(_) => {
				return Err(Error::Unimplemented(
					"CREATE with VALUES not yet supported in execution plans".to_string(),
				));
			}
			Data::ReplaceExpression(_) => {
				return Err(Error::Unimplemented("CREATE with REPLACE not supported".to_string()));
			}
			Data::UpdateExpression(_) => {
				return Err(Error::Unimplemented(
					"CREATE with UPDATE expression not supported".to_string(),
				));
			}
			Data::ContentExpression(expr) => {
				if let Expr::Literal(lit) = expr {
					match literal_to_value(lit) {
						Ok(val) => ContentSource::Value(val),
						Err(_) => {
							return Err(Error::Unimplemented(
								"Complex literals in CREATE not yet supported".to_string(),
							));
						}
					}
				} else {
					return Err(Error::Unimplemented(
						"Complex expressions in CREATE CONTENT not yet supported".to_string(),
					));
				}
			}
			Data::MergeExpression(_) => {
				return Err(Error::Unimplemented("CREATE with MERGE not supported".to_string()));
			}
			Data::UnsetExpression(_) => {
				return Err(Error::Unimplemented("CREATE with UNSET not supported".to_string()));
			}
			Data::PatchExpression(_) => {
				return Err(Error::Unimplemented("CREATE with PATCH not supported".to_string()));
			}
		}
	} else {
		// No data - create empty record
		ContentSource::Value(crate::val::Value::Object(crate::val::Object::default()))
	};

	Ok(PlannedStatement::Query(Arc::new(Create {
		table,
		content,
	})))
}

/// Plan an UPDATE statement
fn plan_update(
	update: crate::expr::statements::UpdateStatement,
) -> Result<PlannedStatement, Error> {
	// Get the table name from the first target
	let table = match update.what.first() {
		Some(Expr::Table(table_name)) => table_name.clone(),
		Some(Expr::Literal(crate::expr::literal::Literal::RecordId(rid))) => rid.table.clone(),
		_ => {
			return Err(Error::Unimplemented(
				"UPDATE target must be a table or record ID".to_string(),
			));
		}
	};

	// Build the input source plan (what to update)
	let input = plan_select_sources(&update.what)?;

	// Apply WHERE clause if present
	let input = if let Some(cond) = update.cond {
		let predicate = expr_to_physical_expr(cond.0)?;
		Arc::new(Filter {
			input,
			predicate,
		}) as Arc<dyn ExecutionPlan>
	} else {
		input
	};

	// Convert data to changes
	let changes = if let Some(data) = update.data {
		use crate::expr::Data;
		match data {
			Data::SetExpression(sets) => {
				let mut changes = Vec::new();
				for set in sets {
					let field = set.place.to_raw_string();
					let value = expr_to_physical_expr(set.value)?;
					changes.push(SetField {
						field,
						value,
					});
				}
				changes
			}
			_ => {
				return Err(Error::Unimplemented(
					"Only SET expressions supported in UPDATE execution plans".to_string(),
				));
			}
		}
	} else {
		Vec::new()
	};

	Ok(PlannedStatement::Query(Arc::new(Update {
		table,
		input,
		changes,
	})))
}

/// Plan a DELETE statement
fn plan_delete(
	delete: crate::expr::statements::DeleteStatement,
) -> Result<PlannedStatement, Error> {
	// Get the table name from the first target
	let table = match delete.what.first() {
		Some(Expr::Table(table_name)) => table_name.clone(),
		Some(Expr::Literal(crate::expr::literal::Literal::RecordId(rid))) => rid.table.clone(),
		_ => {
			return Err(Error::Unimplemented(
				"DELETE target must be a table or record ID".to_string(),
			));
		}
	};

	// Build the input source plan (what to delete)
	let input = plan_select_sources(&delete.what)?;

	// Apply WHERE clause if present
	let input = if let Some(cond) = delete.cond {
		let predicate = expr_to_physical_expr(cond.0)?;
		Arc::new(Filter {
			input,
			predicate,
		}) as Arc<dyn ExecutionPlan>
	} else {
		input
	};

	Ok(PlannedStatement::Query(Arc::new(Delete {
		table,
		input,
	})))
}
