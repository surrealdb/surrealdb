use std::sync::Arc;

use crate::err::Error;
use crate::exec::block::{
	BlockOutputMode, BlockPlan, ForPlan, IfBranch, IfPlan, LetValueSource,
	PlannedStatement as BlockStatement, StatementOperation,
};
use crate::exec::operators::{
	BeginPlan, CancelPlan, CommitPlan, ComputeFields, ExplainPlan, ExprPlan, Filter, LetPlan,
	Limit, RecordIdLookup, Scan, Sort, Union, UsePlan,
};
use crate::exec::statement::{ExecutionPlan, StatementId, StatementKind, StatementPlan};
use crate::exec::{OperatorPlan, PlannedStatement, SessionCommand};
use crate::expr::{Expr, Literal, TopLevelExpr};

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
	let value: Arc<dyn OperatorPlan> = match &let_stmt.what {
		// SELECT produces a stream that gets collected into an array
		Expr::Select(select) => {
			let plan = plan_select(select)?;
			match plan {
				PlannedStatement::Query(exec_plan) => exec_plan,
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

		// Everything else is a scalar expression - wrap in ExprPlan
		other => {
			let expr = expr_to_physical_expr(other.clone())?;

			// Validate: LET expressions can't reference current row
			if expr.references_current_value() {
				return Err(Error::Unimplemented(
					"LET expression cannot reference current row context".to_string(),
				));
			}

			Arc::new(ExprPlan {
				expr,
			}) as Arc<dyn OperatorPlan>
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
struct QueryExecutionPlanner {
	/// Counter for generating statement IDs
	next_id: usize,

	/// The most recent context-mutating statement (USE/LET)
	last_context_source: Option<StatementId>,

	/// The most recent full barrier (mutation/schema/transaction)
	last_barrier: Option<StatementId>,

	/// All statements since the last barrier (for mutation wait_for)
	statements_since_barrier: Vec<StatementId>,
}

impl QueryExecutionPlanner {
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
	fn plan_statements(&mut self, expressions: &[TopLevelExpr]) -> Result<ExecutionPlan, Error> {
		let mut statements = Vec::with_capacity(expressions.len());

		for expr in expressions {
			let stmt = self.plan_top_level_expr(expr)?;
			statements.push(stmt);
		}

		Ok(ExecutionPlan {
			statements,
		})
	}

	/// Plan a single top-level expression into a statement plan
	fn plan_top_level_expr(&mut self, expr: &TopLevelExpr) -> Result<StatementPlan, Error> {
		let id = self.next_statement_id();

		let (plan, kind) = match expr {
			// Transaction control - create operator plans
			TopLevelExpr::Begin => {
				let op = Arc::new(BeginPlan) as Arc<dyn OperatorPlan>;
				(op, StatementKind::Transaction)
			}
			TopLevelExpr::Cancel => {
				let op = Arc::new(CancelPlan) as Arc<dyn OperatorPlan>;
				(op, StatementKind::Transaction)
			}
			TopLevelExpr::Commit => {
				let op = Arc::new(CommitPlan) as Arc<dyn OperatorPlan>;
				(op, StatementKind::Transaction)
			}

			// USE - create UsePlan operator
			TopLevelExpr::Use(use_stmt) => {
				let (ns, db) = self.convert_use_to_content(use_stmt)?;
				let op = Arc::new(UsePlan {
					ns,
					db,
				}) as Arc<dyn OperatorPlan>;
				(op, StatementKind::ContextMutation)
			}

			TopLevelExpr::Explain {
				format,
				statement,
			} => {
				// Plan the inner statement to get its content
				let (inner_plan, _kind) = self.plan_content_only(statement)?;
				// Create an ExplainPlan operator that wraps the inner plan
				let op = Arc::new(ExplainPlan {
					plan: inner_plan,
					format: *format,
				}) as Arc<dyn OperatorPlan>;
				(op, StatementKind::PureRead)
			}

			TopLevelExpr::Expr(inner_expr) => self.plan_expr_to_plan(inner_expr)?,

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
			plan,
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

	/// Plan the content of a statement without allocating IDs or updating planner state.
	///
	/// This is used for EXPLAIN to get the plan text without affecting the DAG structure.
	fn plan_content_only(
		&self,
		expr: &TopLevelExpr,
	) -> Result<(Arc<dyn OperatorPlan>, StatementKind), Error> {
		match expr {
			// Transaction control - create operator plans
			TopLevelExpr::Begin => {
				let op = Arc::new(BeginPlan) as Arc<dyn OperatorPlan>;
				Ok((op, StatementKind::Transaction))
			}
			TopLevelExpr::Cancel => {
				let op = Arc::new(CancelPlan) as Arc<dyn OperatorPlan>;
				Ok((op, StatementKind::Transaction))
			}
			TopLevelExpr::Commit => {
				let op = Arc::new(CommitPlan) as Arc<dyn OperatorPlan>;
				Ok((op, StatementKind::Transaction))
			}

			// USE - create UsePlan operator
			TopLevelExpr::Use(use_stmt) => {
				let (ns, db) = self.convert_use_to_content(use_stmt)?;
				let op = Arc::new(UsePlan {
					ns,
					db,
				}) as Arc<dyn OperatorPlan>;
				Ok((op, StatementKind::ContextMutation))
			}

			// Nested EXPLAIN - plan recursively
			TopLevelExpr::Explain {
				format,
				statement,
			} => {
				let (inner_plan, _kind) = self.plan_content_only(statement)?;
				let op = Arc::new(ExplainPlan {
					plan: inner_plan,
					format: *format,
				}) as Arc<dyn OperatorPlan>;
				Ok((op, StatementKind::PureRead))
			}

			TopLevelExpr::Expr(inner_expr) => self.plan_expr_to_plan(inner_expr),

			// Unsupported statements
			_ => Err(Error::Unimplemented(format!(
				"Statement type not supported in EXPLAIN: {:?}",
				expr
			))),
		}
	}

	/// Convert USE statement to content
	fn convert_use_to_content(
		&self,
		use_stmt: &crate::expr::statements::UseStatement,
	) -> Result<
		(Option<Arc<dyn crate::exec::PhysicalExpr>>, Option<Arc<dyn crate::exec::PhysicalExpr>>),
		Error,
	> {
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

	/// Plan an expression to an operator plan and statement kind
	fn plan_expr_to_plan(
		&self,
		expr: &Expr,
	) -> Result<(Arc<dyn OperatorPlan>, StatementKind), Error> {
		match expr {
			// SELECT - pure read
			Expr::Select(select) => {
				let plan = self.plan_select_internal(select)?;
				Ok((plan, StatementKind::PureRead))
			}

			// LET - create LetPlan operator
			Expr::Let(let_stmt) => {
				let (name, value) = self.convert_let_to_content(let_stmt)?;
				let op = Arc::new(LetPlan {
					name,
					value,
				}) as Arc<dyn OperatorPlan>;
				Ok((op, StatementKind::ContextMutation))
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
		Expr::Foreach(foreach_stmt) => {
			// Plan FOR loop as a block operation
			let for_plan = self.plan_foreach(foreach_stmt)?;
			// Return the ForPlan wrapped in a PlannedStatement
			// Note: This requires converting to an operation, which we'll handle specially
			Err(Error::Unimplemented(
				"FOR statements require BlockPlan infrastructure - use plan_block_expr instead"
					.to_string(),
			))
		}
		Expr::IfElse(if_stmt) => {
			// Plan IF/ELSE as a block operation
			Err(Error::Unimplemented(
				"IF statements require BlockPlan infrastructure - use plan_block_expr instead"
					.to_string(),
			))
		}
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
			"RETURN statements require BlockPlan infrastructure - use plan_block_expr instead"
				.to_string(),
		)),
		Expr::Throw(_) => Err(Error::Unimplemented(
			"THROW statements require BlockPlan infrastructure - use plan_block_expr instead"
				.to_string(),
		)),
		Expr::Break => Err(Error::Unimplemented(
			"BREAK statements require BlockPlan infrastructure - use plan_block_expr instead"
				.to_string(),
		)),
		Expr::Continue => Err(Error::Unimplemented(
			"CONTINUE statements require BlockPlan infrastructure - use plan_block_expr instead"
				.to_string(),
		)),
		Expr::Sleep(_) => Err(Error::Unimplemented(
			"SLEEP statements require BlockPlan infrastructure - use plan_block_expr instead"
				.to_string(),
		)),
		Expr::Mock(_) => Err(Error::Unimplemented(
			"Mock expressions not yet supported in script plans".to_string(),
		)),

			// Value expressions - scalar evaluation as pure read
			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::Binary {
				..
			}
			| Expr::Postfix {
				..
			}
			| Expr::Table(_) => {
				let phys_expr = expr_to_physical_expr(expr.clone())?;
				if phys_expr.references_current_value() {
					return Err(Error::Unimplemented(
						"Expression references row context but no table specified".to_string(),
					));
				}
				let op = Arc::new(ExprPlan {
					expr: phys_expr,
				}) as Arc<dyn OperatorPlan>;
				Ok((op, StatementKind::PureRead))
			}

			Expr::Idiom(_) => {
				let phys_expr = expr_to_physical_expr(expr.clone())?;
				if phys_expr.references_current_value() {
					return Err(Error::Unimplemented(
						"Field expressions require a FROM clause to provide row context"
							.to_string(),
					));
				}
				let op = Arc::new(ExprPlan {
					expr: phys_expr,
				}) as Arc<dyn OperatorPlan>;
				Ok((op, StatementKind::PureRead))
			}
		}
	}

	/// Convert LET statement to content
	fn convert_let_to_content(
		&self,
		let_stmt: &crate::expr::statements::SetStatement,
	) -> Result<(String, Arc<dyn OperatorPlan>), Error> {
		let name = let_stmt.name.clone();

		let value: Arc<dyn OperatorPlan> = match &let_stmt.what {
			Expr::Select(select) => {
				let plan = self.plan_select_internal(select)?;
				plan
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

			// Scalar expressions - wrap in ExprPlan
			other => {
				let expr = expr_to_physical_expr(other.clone())?;
				if expr.references_current_value() {
					return Err(Error::Unimplemented(
						"LET expression cannot reference current row context".to_string(),
					));
				}
				Arc::new(ExprPlan {
					expr,
				}) as Arc<dyn OperatorPlan>
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

	/// Plan a FOR loop statement.
	fn plan_foreach(
		&self,
		foreach_stmt: &crate::expr::statements::ForeachStatement,
	) -> Result<ForPlan, Error> {
		// Convert the range expression
		let iterable = expr_to_physical_expr(foreach_stmt.range.clone())?;

		// Plan the body block
		let body = self.plan_block(&foreach_stmt.block, BlockOutputMode::Discard)?;

		Ok(ForPlan {
			variable: foreach_stmt.param.as_str().to_string(),
			iterable,
			body,
		})
	}

	/// Plan an IF/ELSE statement.
	fn plan_ifelse(
		&self,
		if_stmt: &crate::expr::statements::IfelseStatement,
	) -> Result<IfPlan, Error> {
		let mut branches = Vec::new();

		for (condition, body) in &if_stmt.exprs {
			let condition_expr = expr_to_physical_expr(condition.clone())?;
			let body_block = self.plan_block_from_expr(body, BlockOutputMode::Discard)?;

			branches.push(IfBranch {
				condition: condition_expr,
				body: body_block,
			});
		}

		let else_branch = if let Some(else_expr) = &if_stmt.close {
			Some(self.plan_block_from_expr(else_expr, BlockOutputMode::Discard)?)
		} else {
			None
		};

		Ok(IfPlan {
			branches,
			else_branch,
		})
	}

	/// Plan a Block into a BlockPlan.
	fn plan_block(
		&self,
		block: &crate::expr::Block,
		output_mode: BlockOutputMode,
	) -> Result<BlockPlan, Error> {
		let mut plan = BlockPlan::new(output_mode);

		for (i, expr) in block.0.iter().enumerate() {
			let operation = self.plan_block_operation(expr)?;
			plan.push(BlockStatement {
				id: StatementId(i),
				context_source: None, // Will be calculated later
				wait_for: vec![],     // Will be calculated later
				operation,
			});
		}

		// Calculate dependencies
		plan.calculate_dependencies();

		Ok(plan)
	}

	/// Plan a single expression into a BlockPlan (for IF bodies that are single expressions).
	fn plan_block_from_expr(
		&self,
		expr: &Expr,
		output_mode: BlockOutputMode,
	) -> Result<BlockPlan, Error> {
		// For single expression bodies, wrap in a one-statement block
		let mut plan = BlockPlan::new(output_mode);

		let operation = self.plan_block_operation(expr)?;
		plan.push(BlockStatement {
			id: StatementId(0),
			context_source: None,
			wait_for: vec![],
			operation,
		});

		Ok(plan)
	}

	/// Plan a single expression into a StatementOperation.
	fn plan_block_operation(&self, expr: &Expr) -> Result<StatementOperation, Error> {
		match expr {
			// SELECT - wrap in operator
			Expr::Select(select) => {
				let plan = self.plan_select_internal(select)?;
				Ok(StatementOperation::Operator(plan))
			}

			// LET - context mutation
			Expr::Let(let_stmt) => {
				let value = match &let_stmt.what {
					Expr::Select(select) => {
						let plan = self.plan_select_internal(select)?;
						LetValueSource::Query(plan)
					}
					other => {
						let expr = expr_to_physical_expr(other.clone())?;
						LetValueSource::Scalar(expr)
					}
				};
				Ok(StatementOperation::Let {
					name: let_stmt.name.clone(),
					value,
				})
			}

			// FOR loop
			Expr::Foreach(foreach_stmt) => {
				let for_plan = self.plan_foreach(foreach_stmt)?;
				Ok(StatementOperation::For(Box::new(for_plan)))
			}

			// IF/ELSE
			Expr::IfElse(if_stmt) => {
				let if_plan = self.plan_ifelse(if_stmt)?;
				Ok(StatementOperation::If(Box::new(if_plan)))
			}

			// Control flow
			Expr::Break => Ok(StatementOperation::Break),
			Expr::Continue => Ok(StatementOperation::Continue),

			Expr::Return(return_stmt) => {
				let value = expr_to_physical_expr(return_stmt.what.clone())?;
				Ok(StatementOperation::Return(value))
			}

			Expr::Throw(throw_expr) => {
				let value = expr_to_physical_expr((**throw_expr).clone())?;
				Ok(StatementOperation::Throw(value))
			}

			Expr::Sleep(sleep_stmt) => {
				// Use the duration directly from the SleepStatement
				let duration = sleep_stmt.duration.0;
				Ok(StatementOperation::Sleep(duration))
			}

			// Scalar expressions - evaluate and return
			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::Binary {
				..
			}
			| Expr::Postfix {
				..
			}
			| Expr::Idiom(_)
			| Expr::Table(_) => {
				// Wrap scalar expression in a simple evaluation operator
				// This would require a ScalarEval operator, but for now return error
				Err(Error::Unimplemented(
					"Scalar expressions in blocks require ScalarEval operator".to_string(),
				))
			}

			// Unsupported
			_ => Err(Error::Unimplemented(format!(
				"Expression type not yet supported in block planning: {:?}",
				std::mem::discriminant(expr)
			))),
		}
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
pub(crate) fn logical_plan_to_execution_plan(
	plan: &crate::expr::LogicalPlan,
) -> Result<ExecutionPlan, Error> {
	let mut planner = QueryExecutionPlanner::new();
	planner.plan_statements(&plan.expressions)
}

#[cfg(test)]
mod script_planner_tests {
	use super::*;
	use crate::expr::statements::UseStatement;
	use crate::expr::{LogicalPlan, TopLevelExpr};

	#[test]
	fn test_planner_creates_use_operator() {
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Use(UseStatement::Ns(Expr::Literal(
				crate::expr::literal::Literal::String("test_ns".to_string()),
			)))],
		};

		let script = logical_plan_to_execution_plan(&plan).expect("Planning failed");

		assert_eq!(script.len(), 1);
		let stmt = &script.statements[0];
		assert_eq!(stmt.kind, StatementKind::ContextMutation);
		assert!(!stmt.kind.is_full_barrier());

		assert_eq!(stmt.plan.name(), "Use");
		assert!(stmt.plan.mutates_context());
	}

	#[test]
	fn test_planner_creates_let_operator() {
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Let(Box::new(
				crate::expr::statements::SetStatement {
					name: "x".to_string(),
					what: Expr::Literal(crate::expr::literal::Literal::Integer(42)),
					kind: None,
				},
			)))],
		};

		let script = logical_plan_to_execution_plan(&plan).expect("Planning failed");

		assert_eq!(script.len(), 1);
		let stmt = &script.statements[0];
		assert_eq!(stmt.kind, StatementKind::ContextMutation);

		assert_eq!(stmt.plan.name(), "Let");
		assert!(stmt.plan.mutates_context());
	}

	#[test]
	fn test_planner_context_source_chain() {
		// Test: USE NS test; LET $x = 1; LET $y = 2
		// Each subsequent statement should get context from the previous one
		let plan = LogicalPlan {
			expressions: vec![
				TopLevelExpr::Use(UseStatement::Ns(Expr::Literal(
					crate::expr::literal::Literal::String("test".to_string()),
				))),
				TopLevelExpr::Expr(Expr::Let(Box::new(crate::expr::statements::SetStatement {
					name: "x".to_string(),
					what: Expr::Literal(crate::expr::literal::Literal::Integer(1)),
					kind: None,
				}))),
				TopLevelExpr::Expr(Expr::Let(Box::new(crate::expr::statements::SetStatement {
					name: "y".to_string(),
					what: Expr::Literal(crate::expr::literal::Literal::Integer(2)),
					kind: None,
				}))),
			],
		};

		let script = logical_plan_to_execution_plan(&plan).expect("Planning failed");

		assert_eq!(script.len(), 3);

		// Statement 0 (USE): no context source
		assert!(script.statements[0].context_source.is_none());
		assert_eq!(script.statements[0].kind, StatementKind::ContextMutation);

		// Statement 1 (LET $x): gets context from statement 0
		assert_eq!(script.statements[1].context_source, Some(StatementId(0)));
		assert_eq!(script.statements[1].kind, StatementKind::ContextMutation);

		// Statement 2 (LET $y): gets context from statement 1
		assert_eq!(script.statements[2].context_source, Some(StatementId(1)));
		assert_eq!(script.statements[2].kind, StatementKind::ContextMutation);
	}
}
