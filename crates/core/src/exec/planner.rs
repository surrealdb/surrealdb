use std::sync::Arc;

use crate::err::Error;
use crate::exec::operators::{ComputeFields, Filter, Limit, RecordIdLookup, Scan, Sort, Union};
use crate::exec::statement::{
	ScriptPlan, StatementContent, StatementId, StatementKind, StatementLetValue, StatementPlan,
};
use crate::exec::{LetValue, OperatorPlan, PlannedStatement, SessionCommand};
use crate::expr::{Expr, Literal, TopLevelExpr};

/// Attempts to convert a logical plan to an execution plan.
pub(crate) fn logical_plan_to_execution_plan(
	plan: &crate::expr::LogicalPlan,
) -> Result<Vec<PlannedStatement>, Error> {
	let mut execution_plans = Vec::with_capacity(plan.expressions.len());
	for expr in &plan.expressions {
		let planned = top_level_expr_to_execution_plan(expr)?;
		execution_plans.push(planned);
	}

	Ok(execution_plans)
}

fn top_level_expr_to_execution_plan(expr: &TopLevelExpr) -> Result<PlannedStatement, Error> {
	match expr {
		TopLevelExpr::Begin => Ok(PlannedStatement::SessionCommand(SessionCommand::Begin)),
		TopLevelExpr::Cancel => Ok(PlannedStatement::SessionCommand(SessionCommand::Cancel)),
		TopLevelExpr::Commit => Ok(PlannedStatement::SessionCommand(SessionCommand::Commit)),
		TopLevelExpr::Use(use_stmt) => convert_use_statement(use_stmt),
		TopLevelExpr::Explain {
			format,
			statement,
		} => {
			// Convert the inner statement to an execution plan
			let inner_plan = top_level_expr_to_execution_plan(statement)?;
			Ok(PlannedStatement::Explain {
				format: *format,
				statement: Box::new(inner_plan),
			})
		}
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
	use_stmt: &crate::expr::statements::UseStatement,
) -> Result<PlannedStatement, Error> {
	use crate::expr::statements::UseStatement;

	match use_stmt {
		UseStatement::Ns(ns_expr) => {
			let ns = expr_to_physical_expr(ns_expr.clone())?;
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
			let db = expr_to_physical_expr(db_expr.clone())?;
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
			let ns = expr_to_physical_expr(ns_expr.clone())?;
			let db = expr_to_physical_expr(db_expr.clone())?;
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
	use crate::exec::physical_expr::{
		BinaryOp, Field, Literal as PhysicalLiteral, Param, PostfixOp, UnaryOp,
	};

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
		Expr::Constant(constant) => {
			// Convert constant to its computed value
			let value = constant
				.compute()
				.map_err(|e| Error::Unimplemented(format!("Failed to compute constant: {}", e)))?;
			Ok(Arc::new(PhysicalLiteral(value)))
		}
		Expr::Prefix {
			op,
			expr,
		} => {
			let inner = expr_to_physical_expr(*expr)?;
			Ok(Arc::new(UnaryOp {
				op,
				expr: inner,
			}))
		}
		Expr::Postfix {
			op,
			expr,
		} => {
			let inner = expr_to_physical_expr(*expr)?;
			Ok(Arc::new(PostfixOp {
				op,
				expr: inner,
			}))
		}
		Expr::Table(table_name) => {
			// Table name as a string value
			Ok(Arc::new(PhysicalLiteral(crate::val::Value::String(
				table_name.as_str().to_string(),
			))))
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

fn expr_to_execution_plan(expr: &Expr) -> Result<PlannedStatement, Error> {
	match expr {
		// Supported statements
		Expr::Select(select) => plan_select(select),
		Expr::Let(let_stmt) => convert_let_statement(let_stmt),

		// DML statements - not yet supported
		Expr::Create(_) => Err(Error::Unimplemented(
			"CREATE statements not yet supported in execution plans".to_string(),
		)),
		Expr::Update(_) => Err(Error::Unimplemented(
			"UPDATE statements not yet supported in execution plans".to_string(),
		)),
		Expr::Upsert(_) => Err(Error::Unimplemented(
			"UPSERT statements not yet supported in execution plans".to_string(),
		)),
		Expr::Delete(_) => Err(Error::Unimplemented(
			"DELETE statements not yet supported in execution plans".to_string(),
		)),
		Expr::Insert(_) => Err(Error::Unimplemented(
			"INSERT statements not yet supported in execution plans".to_string(),
		)),
		Expr::Relate(_) => Err(Error::Unimplemented(
			"RELATE statements not yet supported in execution plans".to_string(),
		)),

		// DDL statements - not yet supported
		Expr::Define(_) => Err(Error::Unimplemented(
			"DEFINE statements not yet supported in execution plans".to_string(),
		)),
		Expr::Remove(_) => Err(Error::Unimplemented(
			"REMOVE statements not yet supported in execution plans".to_string(),
		)),
		Expr::Rebuild(_) => Err(Error::Unimplemented(
			"REBUILD statements not yet supported in execution plans".to_string(),
		)),
		Expr::Alter(_) => Err(Error::Unimplemented(
			"ALTER statements not yet supported in execution plans".to_string(),
		)),

		// Other statements - not yet supported
		Expr::Info(_) => Err(Error::Unimplemented(
			"INFO statements not yet supported in execution plans".to_string(),
		)),
		Expr::Foreach(_) => Err(Error::Unimplemented(
			"FOR statements not yet supported in execution plans".to_string(),
		)),
		Expr::IfElse(_) => Err(Error::Unimplemented(
			"IF statements not yet supported in execution plans".to_string(),
		)),
		Expr::Block(_) => Err(Error::Unimplemented(
			"Block expressions not yet supported in execution plans".to_string(),
		)),
		Expr::FunctionCall(_) => Err(Error::Unimplemented(
			"Function call expressions not yet supported in execution plans".to_string(),
		)),
		Expr::Closure(_) => Err(Error::Unimplemented(
			"Closure expressions not yet supported in execution plans".to_string(),
		)),
		Expr::Return(_) => Err(Error::Unimplemented(
			"RETURN statements not yet supported in execution plans".to_string(),
		)),
		Expr::Throw(_) => Err(Error::Unimplemented(
			"THROW statements not yet supported in execution plans".to_string(),
		)),
		Expr::Break => Err(Error::Unimplemented(
			"BREAK statements not yet supported in execution plans".to_string(),
		)),
		Expr::Continue => Err(Error::Unimplemented(
			"CONTINUE statements not yet supported in execution plans".to_string(),
		)),
		Expr::Sleep(_) => Err(Error::Unimplemented(
			"SLEEP statements not yet supported in execution plans".to_string(),
		)),

		// Value expressions - evaluate in scalar context and return result
		Expr::Literal(_)
		| Expr::Param(_)
		| Expr::Constant(_)
		| Expr::Prefix {
			..
		}
		| Expr::Binary {
			..
		}
		| Expr::Table(_) => {
			let phys_expr = expr_to_physical_expr(expr.clone())?;
			// Validate that the expression doesn't require row context
			if phys_expr.references_current_value() {
				return Err(Error::Unimplemented(
					"Expression references row context but no table specified".to_string(),
				));
			}
			Ok(PlannedStatement::Scalar(phys_expr))
		}

		// Idiom expressions require row context, so they need special handling
		Expr::Idiom(_) => {
			let phys_expr = expr_to_physical_expr(expr.clone())?;
			// Idioms always reference current_value, so this will be an error for top-level
			if phys_expr.references_current_value() {
				return Err(Error::Unimplemented(
					"Field expressions require a FROM clause to provide row context".to_string(),
				));
			}
			Ok(PlannedStatement::Scalar(phys_expr))
		}

		// Mock expressions generate test data - defer for now
		Expr::Mock(_) => Err(Error::Unimplemented(
			"Mock expressions not yet supported in execution plans".to_string(),
		)),

		// Postfix expressions (ranges, method calls)
		Expr::Postfix {
			..
		} => {
			let phys_expr = expr_to_physical_expr(expr.clone())?;
			// Validate that the expression doesn't require row context
			if phys_expr.references_current_value() {
				return Err(Error::Unimplemented(
					"Postfix expression references row context but no table specified".to_string(),
				));
			}
			Ok(PlannedStatement::Scalar(phys_expr))
		}
	}
}

/// Plan a SELECT statement
fn plan_select(
	select: &crate::expr::statements::SelectStatement,
) -> Result<PlannedStatement, Error> {
	// Extract VERSION timestamp if present (for time-travel queries)
	let version = extract_version(&select.version)?;

	// Build the source plan from `what` (FROM clause)
	let source = plan_select_sources(&select.what, version)?;

	// Apply WHERE clause if present
	let filtered = if let Some(cond) = &select.cond {
		let predicate = expr_to_physical_expr(cond.0.clone())?;
		Arc::new(Filter {
			input: source,
			predicate,
		}) as Arc<dyn OperatorPlan>
	} else {
		source
	};

	// Apply ORDER BY if present
	let sorted = if let Some(order) = &select.order {
		let order_by = plan_order_by(order)?;
		Arc::new(Sort {
			input: filtered,
			order_by,
		}) as Arc<dyn OperatorPlan>
	} else {
		filtered
	};

	// Apply LIMIT/START if present
	let limited = if select.limit.is_some() || select.start.is_some() {
		let limit_expr = if let Some(limit) = &select.limit {
			Some(expr_to_physical_expr(limit.0.clone())?)
		} else {
			None
		};
		let offset_expr = if let Some(start) = &select.start {
			Some(expr_to_physical_expr(start.0.clone())?)
		} else {
			None
		};
		Arc::new(Limit {
			input: sorted,
			limit: limit_expr,
			offset: offset_expr,
		}) as Arc<dyn OperatorPlan>
	} else {
		sorted
	};

	// TODO: Handle projections (select.expr), GROUP BY
	// For now, we only support SELECT * (all fields)

	Ok(PlannedStatement::Query(limited))
}

/// Extract version timestamp from VERSION clause expression.
/// Currently only supports literal Datetime values.
fn extract_version(version_expr: &Expr) -> Result<Option<u64>, Error> {
	match version_expr {
		Expr::Literal(Literal::None) => Ok(None),
		Expr::Literal(Literal::Datetime(dt)) => {
			let stamp = dt
				.to_version_stamp()
				.map_err(|e| Error::Unimplemented(format!("Invalid VERSION timestamp: {}", e)))?;
			Ok(Some(stamp))
		}
		_ => Err(Error::Unimplemented(
			"VERSION clause only supports literal datetime values in execution plans".to_string(),
		)),
	}
}

/// Plan the FROM sources - handles multiple targets with Union
///
/// The `version` parameter is an optional timestamp for time-travel queries (VERSION clause).
fn plan_select_sources(
	what: &[Expr],
	version: Option<u64>,
) -> Result<Arc<dyn OperatorPlan>, Error> {
	if what.is_empty() {
		return Err(Error::Unimplemented("SELECT requires at least one source".to_string()));
	}

	// Convert each source to a plan
	let mut source_plans = Vec::with_capacity(what.len());
	for expr in what {
		let plan = plan_single_source(expr, version)?;
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
/// Always wraps source operators (Scan, RecordIdLookup) with ComputeFields
///
/// The `version` parameter is an optional timestamp for time-travel queries (VERSION clause).
fn plan_single_source(expr: &Expr, version: Option<u64>) -> Result<Arc<dyn OperatorPlan>, Error> {
	match expr {
		// Table name: SELECT * FROM users
		Expr::Table(table_name) => {
			// Convert table name to a literal string for the physical expression
			let table_expr = expr_to_physical_expr(Expr::Literal(
				crate::expr::literal::Literal::String(table_name.as_str().to_string()),
			))?;
			let scan = Arc::new(Scan {
				table: table_expr.clone(),
				version,
			}) as Arc<dyn OperatorPlan>;
			// Wrap with ComputeFields to evaluate computed fields
			Ok(Arc::new(ComputeFields {
				input: scan,
				table: table_expr,
			}))
		}

		// Record ID literal: SELECT * FROM users:123
		Expr::Literal(crate::expr::literal::Literal::RecordId(record_id_lit)) => {
			// Convert the RecordIdLit to an actual RecordId
			// For now, we only support static record IDs (table:key)
			// More complex expressions would need async evaluation
			let record_id = record_id_lit_to_record_id(record_id_lit)?;
			// Get table name for ComputeFields
			let table_expr = expr_to_physical_expr(Expr::Literal(
				crate::expr::literal::Literal::String(record_id.table.as_str().to_string()),
			))?;
			let lookup = Arc::new(RecordIdLookup {
				record_id,
				version,
			}) as Arc<dyn OperatorPlan>;
			// Wrap with ComputeFields to evaluate computed fields
			Ok(Arc::new(ComputeFields {
				input: lookup,
				table: table_expr,
			}))
		}

		// Idiom that might be a table or record reference
		Expr::Idiom(idiom) => {
			// Simple idiom (just a name) is a table reference
			// Convert to a table scan using the idiom as a physical expression
			let table_expr = expr_to_physical_expr(Expr::Idiom(idiom.clone()))?;
			let scan = Arc::new(Scan {
				table: table_expr.clone(),
				version,
			}) as Arc<dyn OperatorPlan>;
			// Wrap with ComputeFields to evaluate computed fields
			Ok(Arc::new(ComputeFields {
				input: scan,
				table: table_expr,
			}))
		}

		// Parameter that will be resolved at runtime
		Expr::Param(param) => {
			// Parameters could be record IDs or table names
			// We'll treat them as table references - Scan evaluates at runtime
			let table_expr = expr_to_physical_expr(Expr::Param(param.clone()))?;
			let scan = Arc::new(Scan {
				table: table_expr.clone(),
				version,
			}) as Arc<dyn OperatorPlan>;
			// Wrap with ComputeFields to evaluate computed fields
			Ok(Arc::new(ComputeFields {
				input: scan,
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

/// Convert a LET statement to an execution plan
fn convert_let_statement(
	let_stmt: &crate::expr::statements::SetStatement,
) -> Result<PlannedStatement, Error> {
	let name = let_stmt.name.clone();

	// Determine if the expression is a query or scalar
	let value = match &let_stmt.what {
		// SELECT produces a stream that gets collected into an array
		Expr::Select(select) => {
			let plan = plan_select(select)?;
			match plan {
				PlannedStatement::Query(exec_plan) => LetValue::Query(exec_plan),
				_ => {
					return Err(Error::Unimplemented(
						"Unexpected plan type from SELECT in LET".to_string(),
					));
				}
			}
		}

		// DML statements in LET are not yet supported
		Expr::Create(_) => {
			return Err(Error::Unimplemented(
				"CREATE statements in LET not yet supported in execution plans".to_string(),
			));
		}
		Expr::Update(_) => {
			return Err(Error::Unimplemented(
				"UPDATE statements in LET not yet supported in execution plans".to_string(),
			));
		}
		Expr::Upsert(_) => {
			return Err(Error::Unimplemented(
				"UPSERT statements in LET not yet supported in execution plans".to_string(),
			));
		}
		Expr::Delete(_) => {
			return Err(Error::Unimplemented(
				"DELETE statements in LET not yet supported in execution plans".to_string(),
			));
		}
		Expr::Insert(_) => {
			return Err(Error::Unimplemented(
				"INSERT statements in LET not yet supported in execution plans".to_string(),
			));
		}
		Expr::Relate(_) => {
			return Err(Error::Unimplemented(
				"RELATE statements in LET not yet supported in execution plans".to_string(),
			));
		}

		// Everything else is a scalar expression
		other => {
			let expr = expr_to_physical_expr(other.clone())?;

			// Validate: LET expressions can't reference current row
			if expr.references_current_value() {
				return Err(Error::Unimplemented(
					"LET expression cannot reference current row context".to_string(),
				));
			}

			LetValue::Scalar(expr)
		}
	};

	Ok(PlannedStatement::Let {
		name,
		value,
	})
}

// ============================================================================
// Script Planner - DAG Construction
// ============================================================================

/// Planner state for building the statement DAG.
struct ScriptPlanner {
	/// Counter for generating statement IDs
	next_id: usize,

	/// The most recent context-mutating statement (USE/LET)
	last_context_source: Option<StatementId>,

	/// The most recent full barrier (mutation/schema/transaction)
	last_barrier: Option<StatementId>,

	/// All statements since the last barrier (for mutation wait_for)
	statements_since_barrier: Vec<StatementId>,
}

impl ScriptPlanner {
	fn new() -> Self {
		Self {
			next_id: 0,
			last_context_source: None,
			last_barrier: None,
			statements_since_barrier: Vec::new(),
		}
	}

	/// Allocate a new statement ID
	fn next_statement_id(&mut self) -> StatementId {
		let id = StatementId(self.next_id);
		self.next_id += 1;
		id
	}

	/// Plan a script into a DAG
	fn plan_script(&mut self, expressions: &[TopLevelExpr]) -> Result<ScriptPlan, Error> {
		let mut statements = Vec::with_capacity(expressions.len());

		for expr in expressions {
			let stmt = self.plan_top_level_expr(expr)?;
			statements.push(stmt);
		}

		Ok(ScriptPlan {
			statements,
		})
	}

	/// Plan a single top-level expression into a statement plan
	fn plan_top_level_expr(&mut self, expr: &TopLevelExpr) -> Result<StatementPlan, Error> {
		let id = self.next_statement_id();

		let (content, kind) = match expr {
			TopLevelExpr::Begin => (StatementContent::Begin, StatementKind::Transaction),
			TopLevelExpr::Cancel => (StatementContent::Cancel, StatementKind::Transaction),
			TopLevelExpr::Commit => (StatementContent::Commit, StatementKind::Transaction),

			TopLevelExpr::Use(use_stmt) => {
				let (ns, db) = self.convert_use_to_content(use_stmt)?;
				(
					StatementContent::Use {
						ns,
						db,
					},
					StatementKind::ContextMutation,
				)
			}

			TopLevelExpr::Explain {
				format: _,
				statement,
			} => {
				// For EXPLAIN, we plan the inner statement but mark it as PureRead
				// since EXPLAIN doesn't actually execute the statement
				let inner = self.plan_top_level_expr(statement)?;
				// Return the inner statement's content but keep it as a read
				(inner.content, StatementKind::PureRead)
			}

			TopLevelExpr::Expr(inner_expr) => self.plan_expr_to_content(inner_expr)?,

			// Unsupported statements - return error
			TopLevelExpr::Access(_) => {
				return Err(Error::Unimplemented(
					"ACCESS statements not yet supported in script plans".to_string(),
				));
			}
			TopLevelExpr::Kill(_) => {
				return Err(Error::Unimplemented(
					"KILL statements not yet supported in script plans".to_string(),
				));
			}
			TopLevelExpr::Live(_) => {
				return Err(Error::Unimplemented(
					"LIVE statements not yet supported in script plans".to_string(),
				));
			}
			TopLevelExpr::Show(_) => {
				return Err(Error::Unimplemented(
					"SHOW statements not yet supported in script plans".to_string(),
				));
			}
			TopLevelExpr::Option(_) => {
				return Err(Error::Unimplemented(
					"OPTION statements not yet supported in script plans".to_string(),
				));
			}
		};

		// Build wait_for list based on statement kind
		let wait_for = if kind.is_full_barrier() {
			// Full barriers wait for ALL prior statements since last barrier
			// Plus the barrier itself if there was one
			let mut deps = self.statements_since_barrier.clone();
			if let Some(barrier_id) = self.last_barrier {
				deps.push(barrier_id);
			}
			deps
		} else {
			// Non-barriers just wait for the last barrier (if any)
			self.last_barrier.into_iter().collect()
		};

		let stmt = StatementPlan {
			id,
			context_source: self.last_context_source,
			wait_for,
			content,
			kind,
		};

		// Update tracking state
		if kind.is_full_barrier() {
			self.last_barrier = Some(id);
			self.statements_since_barrier.clear();
		} else {
			self.statements_since_barrier.push(id);
		}

		if kind.mutates_context() {
			self.last_context_source = Some(id);
		}

		Ok(stmt)
	}

	/// Convert USE statement to content
	fn convert_use_to_content(
		&self,
		use_stmt: &crate::expr::statements::UseStatement,
	) -> Result<(Option<Arc<dyn crate::exec::PhysicalExpr>>, Option<Arc<dyn crate::exec::PhysicalExpr>>), Error> {
		use crate::expr::statements::UseStatement;

		match use_stmt {
			UseStatement::Ns(ns_expr) => {
				let ns = expr_to_physical_expr(ns_expr.clone())?;
				if ns.references_current_value() {
					return Err(Error::Unimplemented(
						"USE NS expression cannot reference current row".to_string(),
					));
				}
				Ok((Some(ns), None))
			}
			UseStatement::Db(db_expr) => {
				let db = expr_to_physical_expr(db_expr.clone())?;
				if db.references_current_value() {
					return Err(Error::Unimplemented(
						"USE DB expression cannot reference current row".to_string(),
					));
				}
				Ok((None, Some(db)))
			}
			UseStatement::NsDb(ns_expr, db_expr) => {
				let ns = expr_to_physical_expr(ns_expr.clone())?;
				let db = expr_to_physical_expr(db_expr.clone())?;
				if ns.references_current_value() || db.references_current_value() {
					return Err(Error::Unimplemented(
						"USE expression cannot reference current row".to_string(),
					));
				}
				Ok((Some(ns), Some(db)))
			}
			UseStatement::Default => Ok((None, None)),
		}
	}

	/// Plan an expression to statement content and kind
	fn plan_expr_to_content(
		&self,
		expr: &Expr,
	) -> Result<(StatementContent, StatementKind), Error> {
		match expr {
			// SELECT - pure read
			Expr::Select(select) => {
				let plan = self.plan_select_internal(select)?;
				Ok((StatementContent::Query(plan), StatementKind::PureRead))
			}

			// LET - context mutation
			Expr::Let(let_stmt) => {
				let (name, value) = self.convert_let_to_content(let_stmt)?;
				Ok((
					StatementContent::Let {
						name,
						value,
					},
					StatementKind::ContextMutation,
				))
			}

			// DML statements - data mutations
			Expr::Create(_) => Err(Error::Unimplemented(
				"CREATE statements not yet supported in script plans".to_string(),
			)),
			Expr::Update(_) => Err(Error::Unimplemented(
				"UPDATE statements not yet supported in script plans".to_string(),
			)),
			Expr::Upsert(_) => Err(Error::Unimplemented(
				"UPSERT statements not yet supported in script plans".to_string(),
			)),
			Expr::Delete(_) => Err(Error::Unimplemented(
				"DELETE statements not yet supported in script plans".to_string(),
			)),
			Expr::Insert(_) => Err(Error::Unimplemented(
				"INSERT statements not yet supported in script plans".to_string(),
			)),
			Expr::Relate(_) => Err(Error::Unimplemented(
				"RELATE statements not yet supported in script plans".to_string(),
			)),

			// DDL statements - schema changes
			Expr::Define(_) => Err(Error::Unimplemented(
				"DEFINE statements not yet supported in script plans".to_string(),
			)),
			Expr::Remove(_) => Err(Error::Unimplemented(
				"REMOVE statements not yet supported in script plans".to_string(),
			)),
			Expr::Rebuild(_) => Err(Error::Unimplemented(
				"REBUILD statements not yet supported in script plans".to_string(),
			)),
			Expr::Alter(_) => Err(Error::Unimplemented(
				"ALTER statements not yet supported in script plans".to_string(),
			)),

			// Other statements
			Expr::Info(_) => Err(Error::Unimplemented(
				"INFO statements not yet supported in script plans".to_string(),
			)),
			Expr::Foreach(_) => Err(Error::Unimplemented(
				"FOR statements not yet supported in script plans".to_string(),
			)),
			Expr::IfElse(_) => Err(Error::Unimplemented(
				"IF statements not yet supported in script plans".to_string(),
			)),
			Expr::Block(_) => Err(Error::Unimplemented(
				"Block expressions not yet supported in script plans".to_string(),
			)),
			Expr::FunctionCall(_) => Err(Error::Unimplemented(
				"Function call expressions not yet supported in script plans".to_string(),
			)),
			Expr::Closure(_) => Err(Error::Unimplemented(
				"Closure expressions not yet supported in script plans".to_string(),
			)),
			Expr::Return(_) => Err(Error::Unimplemented(
				"RETURN statements not yet supported in script plans".to_string(),
			)),
			Expr::Throw(_) => Err(Error::Unimplemented(
				"THROW statements not yet supported in script plans".to_string(),
			)),
			Expr::Break => Err(Error::Unimplemented(
				"BREAK statements not yet supported in script plans".to_string(),
			)),
			Expr::Continue => Err(Error::Unimplemented(
				"CONTINUE statements not yet supported in script plans".to_string(),
			)),
			Expr::Sleep(_) => Err(Error::Unimplemented(
				"SLEEP statements not yet supported in script plans".to_string(),
			)),
			Expr::Mock(_) => Err(Error::Unimplemented(
				"Mock expressions not yet supported in script plans".to_string(),
			)),

			// Value expressions - scalar evaluation as pure read
			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Constant(_)
			| Expr::Prefix { .. }
			| Expr::Binary { .. }
			| Expr::Postfix { .. }
			| Expr::Table(_) => {
				let phys_expr = expr_to_physical_expr(expr.clone())?;
				if phys_expr.references_current_value() {
					return Err(Error::Unimplemented(
						"Expression references row context but no table specified".to_string(),
					));
				}
				Ok((StatementContent::Scalar(phys_expr), StatementKind::PureRead))
			}

			Expr::Idiom(_) => {
				let phys_expr = expr_to_physical_expr(expr.clone())?;
				if phys_expr.references_current_value() {
					return Err(Error::Unimplemented(
						"Field expressions require a FROM clause to provide row context".to_string(),
					));
				}
				Ok((StatementContent::Scalar(phys_expr), StatementKind::PureRead))
			}
		}
	}

	/// Convert LET statement to content
	fn convert_let_to_content(
		&self,
		let_stmt: &crate::expr::statements::SetStatement,
	) -> Result<(String, StatementLetValue), Error> {
		let name = let_stmt.name.clone();

		let value = match &let_stmt.what {
			Expr::Select(select) => {
				let plan = self.plan_select_internal(select)?;
				StatementLetValue::Query(plan)
			}

			// DML in LET not yet supported
			Expr::Create(_)
			| Expr::Update(_)
			| Expr::Upsert(_)
			| Expr::Delete(_)
			| Expr::Insert(_)
			| Expr::Relate(_) => {
				return Err(Error::Unimplemented(
					"DML statements in LET not yet supported in script plans".to_string(),
				));
			}

			other => {
				let expr = expr_to_physical_expr(other.clone())?;
				if expr.references_current_value() {
					return Err(Error::Unimplemented(
						"LET expression cannot reference current row context".to_string(),
					));
				}
				StatementLetValue::Scalar(expr)
			}
		};

		Ok((name, value))
	}

	/// Plan a SELECT statement into an operator plan (internal version)
	fn plan_select_internal(
		&self,
		select: &crate::expr::statements::SelectStatement,
	) -> Result<Arc<dyn OperatorPlan>, Error> {
		// Extract VERSION timestamp if present
		let version = extract_version(&select.version)?;

		// Build the source plan from `what` (FROM clause)
		let source = plan_select_sources(&select.what, version)?;

		// Apply WHERE clause if present
		let filtered = if let Some(cond) = &select.cond {
			let predicate = expr_to_physical_expr(cond.0.clone())?;
			Arc::new(Filter {
				input: source,
				predicate,
			}) as Arc<dyn OperatorPlan>
		} else {
			source
		};

		// Apply ORDER BY if present
		let sorted = if let Some(order) = &select.order {
			let order_by = plan_order_by(order)?;
			Arc::new(Sort {
				input: filtered,
				order_by,
			}) as Arc<dyn OperatorPlan>
		} else {
			filtered
		};

		// Apply LIMIT/START if present
		let limited = if select.limit.is_some() || select.start.is_some() {
			let limit_expr = if let Some(limit) = &select.limit {
				Some(expr_to_physical_expr(limit.0.clone())?)
			} else {
				None
			};
			let offset_expr = if let Some(start) = &select.start {
				Some(expr_to_physical_expr(start.0.clone())?)
			} else {
				None
			};
			Arc::new(Limit {
				input: sorted,
				limit: limit_expr,
				offset: offset_expr,
			}) as Arc<dyn OperatorPlan>
		} else {
			sorted
		};

		// TODO: Handle projections (select.expr), GROUP BY
		Ok(limited)
	}
}

/// Plan ORDER BY clause into OrderByField list
fn plan_order_by(
	order: &crate::expr::order::Ordering,
) -> Result<Vec<crate::exec::operators::OrderByField>, Error> {
	use crate::exec::operators::{NullsOrder, OrderByField, SortDirection};
	use crate::exec::physical_expr::Field;
	use crate::expr::order::Ordering;

	match order {
		Ordering::Random => {
			// ORDER BY RAND() - not yet supported in the new executor
			Err(Error::Unimplemented(
				"ORDER BY RAND() not yet supported in execution plans".to_string(),
			))
		}
		Ordering::Order(order_list) => {
			let mut fields = Vec::with_capacity(order_list.len());
			for order_field in order_list.iter() {
				// Convert idiom to field expression
				let expr: Arc<dyn crate::exec::PhysicalExpr> =
					Arc::new(Field(order_field.value.clone()));

				let direction = if order_field.direction {
					SortDirection::Asc
				} else {
					SortDirection::Desc
				};

				// Default nulls order based on direction
				// ASC -> nulls last, DESC -> nulls first
				let nulls = if order_field.direction {
					NullsOrder::Last
				} else {
					NullsOrder::First
				};

				fields.push(OrderByField {
					expr,
					direction,
					nulls,
				});
			}

			Ok(fields)
		}
	}
}

/// Convert a logical plan to a script plan with DAG dependencies.
///
/// This is the new entry point that builds a ScriptPlan with proper
/// context_source and wait_for dependencies for parallel execution.
pub(crate) fn logical_plan_to_script_plan(
	plan: &crate::expr::LogicalPlan,
) -> Result<ScriptPlan, Error> {
	let mut planner = ScriptPlanner::new();
	planner.plan_script(&plan.expressions)
}
