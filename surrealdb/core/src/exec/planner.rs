//! Query Planner for the Streaming Executor
//!
//! This module converts SurrealQL AST expressions (`Expr`) into physical execution
//! plans (`Arc<dyn ExecOperator>`). The planner is a critical component of the
//! streaming query executor, determining how queries are executed.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
//! │   SurrealQL  │     │   Planner    │     │  Execution   │
//! │     AST      │ ──► │   (this)     │ ──► │    Plan      │
//! │    (Expr)    │     │              │     │ (ExecOperator) │
//! └──────────────┘     └──────────────┘     └──────────────┘
//! ```
//!
//! # Usage
//!
//! The main entry point is the [`Planner`] struct:
//!
//! ```ignore
//! use surrealdb_core::exec::planner::Planner;
//!
//! let planner = Planner::new(&ctx);
//! let plan = planner.plan(expr)?;
//! ```
//!
//! For backwards compatibility, [`try_plan_expr`] delegates to `Planner::plan()`.
//!
//! # SELECT Pipeline
//!
//! SELECT statements are planned into a standard operator pipeline:
//!
//! ```text
//! Scan/Union (FROM)
//!     │
//!     ▼
//! Filter (WHERE)
//!     │
//!     ▼
//! Split (SPLIT BY)
//!     │
//!     ▼
//! Aggregate (GROUP BY)
//!     │
//!     ▼
//! Sort (ORDER BY)
//!     │
//!     ▼
//! Limit (LIMIT/START)
//!     │
//!     ▼
//! Fetch (FETCH)
//!     │
//!     ▼
//! Project (SELECT fields)
//!     │
//!     ▼
//! Timeout (TIMEOUT)
//! ```
//!
//! # Unimplemented Features
//!
//! The following features return `Error::Unimplemented` and are tracked for future work:
//!
//! ## DML Subqueries (in expression context)
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `CREATE` | CREATE subqueries in expressions |
//! | `UPDATE` | UPDATE subqueries in expressions |
//! | `UPSERT` | UPSERT subqueries in expressions |
//! | `DELETE` | DELETE subqueries in expressions |
//! | `INSERT` | INSERT subqueries in expressions |
//! | `RELATE` | RELATE subqueries in expressions |
//!
//! ## DDL Statements (in expression context)
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `DEFINE` | Schema definition statements |
//! | `REMOVE` | Schema removal statements |
//! | `REBUILD` | Index rebuild statements |
//! | `ALTER` | Schema alteration statements |
//!
//! ## SELECT Clauses
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `SELECT ... ONLY` | Unwrap single results |
//! | `SELECT ... EXPLAIN` | Query plan output |
//! | `SELECT ... WITH` | Index hints |
//! | `SELECT * GROUP BY` | Wildcard with GROUP BY |
//! | `OMIT + SELECT VALUE` | OMIT clause with SELECT VALUE |
//! | `OMIT + fields` | OMIT clause without wildcard |
//!
//! ## Control Flow (in expression context)
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `BREAK` | Loop break (valid in FOR loops) |
//! | `CONTINUE` | Loop continue (valid in FOR loops) |
//! | `RETURN` | Function return |
//! | `FOR` loops | Iteration (as expression) |
//!
//! ## Data Types
//!
//! | Feature | Description |
//! |---------|-------------|
//! | Array record keys | `table:[1,2,3]` syntax |
//! | Object record keys | `table:{a:1}` syntax |
//! | Nested range keys | Range within range |
//! | Set literals | In USE statements |
//! | Mock expressions | Test data generation |
//!
//! ## Functions
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `search::*` | Full-text search functions |
//! | `api::*` | External API functions |
//! | Nested aggregates | `SUM(COUNT(...))` patterns |
//!
//! ## Other
//!
//! | Feature | Description |
//! |---------|-------------|
//! | Non-idiom FETCH | FETCH with non-field expressions |
//! | Non-idiom OMIT | OMIT with non-field expressions |
//! | Dynamic VERSION | VERSION clause with non-literal |
//! | Row context errors | Expressions requiring FROM clause |
//!
//! # Future Improvements (TODOs)
//!
//! - `SortTopKByKey`: Optimized top-k sorting with pre-computed fields

use std::sync::Arc;

use crate::cnf::MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE;
use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::exec::ExecOperator;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::function::FunctionRegistry;
#[cfg(storage)]
use crate::exec::operators::ExternalSort;
use crate::exec::operators::{
	Aggregate, AggregateExprInfo, AggregateField, ControlFlowKind, ControlFlowPlan,
	DatabaseInfoPlan, ExplainPlan, ExprPlan, ExtractedAggregate, Fetch, FieldSelection, Filter,
	ForeachPlan, IfElsePlan, IndexInfoPlan, LetPlan, Limit, NamespaceInfoPlan, OrderByField,
	Project, ProjectValue, RandomShuffle, RootInfoPlan, Scan, SequencePlan, SleepPlan, Sort,
	SortDirection, SortTopK, SourceExpr, Split, TableInfoPlan, Timeout, Union, UserInfoPlan,
	aggregate_field_name,
};
use crate::expr::field::{Field, Fields};
use crate::expr::statements::IfelseStatement;
use crate::expr::visit::{MutVisitor, VisitMut};
use crate::expr::{BinaryOperator, Cond, Expr, Function, FunctionCall, Idiom, Literal};

// ============================================================================
// Planner Struct
// ============================================================================

/// Query planner that converts logical expressions to physical execution plans.
///
/// The `Planner` holds shared resources (context, function registry) to avoid
/// passing them through every function call. This improves code clarity and
/// reduces parameter lists throughout the planning process.
///
/// # Usage
///
/// ```ignore
/// let planner = Planner::new(&ctx);
/// let plan = planner.plan(expr)?;
/// ```
///
/// # Architecture
///
/// The planner converts SurrealQL AST nodes (`Expr`) into physical execution
/// plans (`Arc<dyn ExecOperator>`). The conversion process:
///
/// 1. **Top-level statements** (SELECT, LET, INFO, etc.) become operator trees
/// 2. **Expressions** (literals, function calls, idioms) become `PhysicalExpr`
/// 3. **SELECT pipelines** follow: Source → Filter → Split → Aggregate → Sort → Limit → Project
///
/// # Limitations
///
/// The following features are not yet implemented in the streaming executor:
///
/// ## DML Subqueries
/// - CREATE, UPDATE, UPSERT, DELETE, INSERT, RELATE subqueries
///
/// ## SELECT Clauses
/// - `SELECT ... ONLY` (unwrap single results)
/// - `SELECT ... EXPLAIN` (query plan output)
/// - `SELECT ... WITH` (index hints)
/// - `SELECT *` with GROUP BY
///
/// ## Control Flow (in expression context)
/// - BREAK, CONTINUE, RETURN statements
/// - FOR loops
///
/// ## DDL Statements (in expression context)
/// - DEFINE, REMOVE, REBUILD, ALTER statements
///
/// ## Data Types
/// - Array and object record keys
/// - Set literals in USE statements
/// - Mock expressions
///
/// ## Functions
/// - `search::*` and `api::*` function families
/// - Nested aggregate functions
pub struct Planner<'ctx> {
	/// The frozen context containing query parameters, capabilities, and session info.
	ctx: &'ctx FrozenContext,
	/// Cached reference to the function registry for aggregate/projection detection.
	function_registry: &'ctx FunctionRegistry,
}

impl<'ctx> Planner<'ctx> {
	/// Create a new planner with the given context.
	///
	/// The function registry is cached from the context for efficient lookups
	/// during aggregate function detection.
	pub fn new(ctx: &'ctx FrozenContext) -> Self {
		Self {
			ctx,
			function_registry: ctx.function_registry(),
		}
	}

	/// Get the underlying frozen context.
	#[inline]
	pub fn ctx(&self) -> &'ctx FrozenContext {
		self.ctx
	}

	/// Get the function registry.
	#[inline]
	pub fn function_registry(&self) -> &'ctx FunctionRegistry {
		self.function_registry
	}

	/// Plan an expression, converting it to an executable operator tree.
	///
	/// This is the main entry point for the planner. It handles all top-level
	/// statement types (SELECT, LET, INFO, etc.) and converts them to physical
	/// execution plans.
	///
	/// # Errors
	///
	/// Returns `Error::Unimplemented` for statements not yet supported in the
	/// streaming executor.
	pub fn plan(&self, expr: Expr) -> Result<Arc<dyn ExecOperator>, Error> {
		// Delegate to the internal planning logic
		self.plan_expr(expr)
	}

	/// Convert an expression to a physical expression.
	///
	/// Physical expressions are evaluated at runtime to produce values.
	/// This is used for expressions within operators (e.g., WHERE predicates,
	/// SELECT field expressions, ORDER BY expressions).
	pub fn physical_expr(&self, expr: Expr) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		expr_to_physical_expr(expr, self.ctx)
	}

	/// Convert an expression to a physical expression, treating simple identifiers as strings.
	///
	/// This is used for expressions like `INFO FOR USER test` where `test` is a name
	/// that should be treated as a string literal, not an undefined variable.
	pub fn physical_expr_as_name(
		&self,
		expr: Expr,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		expr_to_physical_expr_as_name(expr, self.ctx)
	}

	/// Internal method to plan an expression.
	fn plan_expr(&self, expr: Expr) -> Result<Arc<dyn ExecOperator>, Error> {
		match expr {
			// Supported statements
			Expr::Select(select) => self.plan_select(*select),
			Expr::Let(let_stmt) => self.convert_let_statement(*let_stmt),

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

			// INFO statements
			Expr::Info(info) => {
				use crate::expr::statements::info::InfoStatement;
				match *info {
					InfoStatement::Root(structured) => Ok(Arc::new(RootInfoPlan {
						structured,
					}) as Arc<dyn ExecOperator>),
					InfoStatement::Ns(structured) => Ok(Arc::new(NamespaceInfoPlan {
						structured,
					}) as Arc<dyn ExecOperator>),
					InfoStatement::Db(structured, version) => {
						let version = version.map(|v| self.physical_expr(v)).transpose()?;
						Ok(Arc::new(DatabaseInfoPlan {
							structured,
							version,
						}) as Arc<dyn ExecOperator>)
					}
					InfoStatement::Tb(table, structured, version) => {
						// Table names are identifiers that should be treated as strings
						let table = self.physical_expr_as_name(table)?;
						let version = version.map(|v| self.physical_expr(v)).transpose()?;
						Ok(Arc::new(TableInfoPlan {
							table,
							structured,
							version,
						}) as Arc<dyn ExecOperator>)
					}
					InfoStatement::User(user, base, structured) => {
						// User names are identifiers that should be treated as strings
						let user = self.physical_expr_as_name(user)?;
						Ok(Arc::new(UserInfoPlan {
							user,
							base,
							structured,
						}) as Arc<dyn ExecOperator>)
					}
					InfoStatement::Index(index, table, structured) => {
						// Index and table names are identifiers that should be treated as strings
						let index = self.physical_expr_as_name(index)?;
						let table = self.physical_expr_as_name(table)?;
						Ok(Arc::new(IndexInfoPlan {
							index,
							table,
							structured,
						}) as Arc<dyn ExecOperator>)
					}
				}
			}
			Expr::Foreach(stmt) => Ok(Arc::new(ForeachPlan {
				param: stmt.param.clone(),
				range: stmt.range.clone(),
				body: stmt.block.clone(),
			}) as Arc<dyn ExecOperator>),
			Expr::IfElse(stmt) => Ok(Arc::new(IfElsePlan {
				branches: stmt.exprs.clone(),
				else_body: stmt.close.clone(),
			}) as Arc<dyn ExecOperator>),
			Expr::Block(block) => {
				// Deferred planning: wrap the block without converting inner expressions.
				// The SequencePlan will plan and execute each expression at runtime,
				// allowing LET bindings to inform subsequent expression planning.
				if block.0.is_empty() {
					// Empty block returns NONE immediately
					use crate::exec::physical_expr::Literal as PhysicalLiteral;
					Ok(Arc::new(ExprPlan {
						expr: Arc::new(PhysicalLiteral(crate::val::Value::None)),
					}) as Arc<dyn ExecOperator>)
				} else if block.0.len() == 1 {
					// Single statement - plan directly without wrapper
					self.plan_expr(block.0.into_iter().next().unwrap())
				} else {
					// Multiple statements - use SequencePlan with deferred planning
					Ok(Arc::new(SequencePlan {
						block: *block,
					}) as Arc<dyn ExecOperator>)
				}
			}
			Expr::FunctionCall(_) => {
				// Function calls are value expressions - convert to physical expression
				let phys_expr = self.physical_expr(expr)?;
				// Validate that the expression doesn't require row context
				if phys_expr.references_current_value() {
					return Err(Error::Unimplemented(
						"Function call references row context but no table specified".to_string(),
					));
				}
				Ok(Arc::new(ExprPlan {
					expr: phys_expr,
				}) as Arc<dyn ExecOperator>)
			}
			Expr::Closure(_) => {
				let closure_expr = self.physical_expr(expr)?;

				Ok(Arc::new(ExprPlan {
					expr: closure_expr,
				}) as Arc<dyn ExecOperator>)
			}
			Expr::Return(output_stmt) => {
				// Plan the inner expression
				let inner = self.plan_expr(output_stmt.what)?;

				// Wrap with Fetch operator if FETCH clause is present
				let inner = if let Some(fetchs) = output_stmt.fetch {
					// Extract idioms from fetch expressions
					// FETCH expressions are typically Expr::Idiom(idiom)
					let fields: Vec<_> = fetchs
						.iter()
						.filter_map(|f| {
							if let Expr::Idiom(idiom) = &f.0 {
								Some(idiom.clone())
							} else {
								// Non-idiom fetch expressions are not supported in the new planner
								None
							}
						})
						.collect();
					if fields.is_empty() {
						// No idiom fields to fetch, pass through
						inner
					} else {
						Arc::new(Fetch {
							input: inner,
							fields,
						}) as Arc<dyn ExecOperator>
					}
				} else {
					inner
				};

				Ok(Arc::new(ControlFlowPlan {
					kind: ControlFlowKind::Return,
					inner: Some(inner),
				}))
			}
			Expr::Throw(expr) => {
				let inner = self.plan_expr(*expr)?;
				Ok(Arc::new(ControlFlowPlan {
					kind: ControlFlowKind::Throw,
					inner: Some(inner),
				}))
			}
			Expr::Break => Ok(Arc::new(ControlFlowPlan {
				kind: ControlFlowKind::Break,
				inner: None,
			})),
			Expr::Continue => Ok(Arc::new(ControlFlowPlan {
				kind: ControlFlowKind::Continue,
				inner: None,
			})),
			Expr::Sleep(sleep_stmt) => Ok(Arc::new(SleepPlan {
				duration: sleep_stmt.duration,
			})),
			Expr::Explain {
				format,
				statement,
			} => {
				// Plan the inner statement
				let inner_plan = self.plan_expr(*statement)?;
				// Wrap it in an ExplainPlan operator
				Ok(Arc::new(ExplainPlan {
					plan: inner_plan,
					format,
				}))
			}

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
				let phys_expr = self.physical_expr(expr)?;
				// Validate that the expression doesn't require row context
				if phys_expr.references_current_value() {
					return Err(Error::Unimplemented(
						"Expression references row context but no table specified".to_string(),
					));
				}
				Ok(Arc::new(ExprPlan {
					expr: phys_expr,
				}) as Arc<dyn ExecOperator>)
			}

			// Idiom expressions require row context, so they need special handling
			Expr::Idiom(_) => {
				let phys_expr = self.physical_expr(expr)?;
				// Idioms always reference current_value, so this will be an error for top-level
				if phys_expr.references_current_value() {
					return Err(Error::Unimplemented(
						"Field expressions require a FROM clause to provide row context"
							.to_string(),
					));
				}
				Ok(Arc::new(ExprPlan {
					expr: phys_expr,
				}) as Arc<dyn ExecOperator>)
			}

			// Mock expressions generate test data - defer for now
			Expr::Mock(_) => Err(Error::Unimplemented(
				"Mock expressions not yet supported in execution plans".to_string(),
			)),

			// Postfix expressions (ranges, method calls)
			Expr::Postfix {
				..
			} => {
				let phys_expr = self.physical_expr(expr)?;
				// Validate that the expression doesn't require row context
				if phys_expr.references_current_value() {
					return Err(Error::Unimplemented(
						"Postfix expression references row context but no table specified"
							.to_string(),
					));
				}
				Ok(Arc::new(ExprPlan {
					expr: phys_expr,
				}) as Arc<dyn ExecOperator>)
			}
		}
	}

	// ========================================================================
	// SELECT Statement Planning
	// ========================================================================

	/// Plan a SELECT statement into an operator tree.
	///
	/// The operator pipeline is built in this order:
	/// 1. Scan/Union (source from FROM clause)
	/// 2. Filter (WHERE)
	/// 3. Split (SPLIT BY)
	/// 4. Aggregate (GROUP BY)
	/// 5. Sort (ORDER BY)
	/// 6. Limit (LIMIT/START)
	/// 7. Fetch (FETCH)
	/// 8. Project (SELECT fields) or ProjectValue (SELECT VALUE)
	/// 9. Timeout (TIMEOUT)
	fn plan_select(
		&self,
		select: crate::expr::statements::SelectStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		// Delegate to free function during migration
		plan_select(select, self.ctx)
	}

	// ========================================================================
	// LET Statement Planning
	// ========================================================================

	/// Convert a LET statement to an execution plan.
	fn convert_let_statement(
		&self,
		let_stmt: crate::expr::statements::SetStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		// Delegate to free function during migration
		convert_let_statement(let_stmt, self.ctx)
	}
}

// ============================================================================
// Select Pipeline Configuration
// ============================================================================

/// Configuration for the SELECT pipeline.
///
/// This struct bundles together all the optional clauses from a SELECT statement
/// that affect the pipeline: WHERE, SPLIT BY, GROUP BY, ORDER BY, LIMIT, START,
/// and OMIT. Using a struct reduces the parameter count for `plan_select_pipeline`
/// from 12 parameters to 4 (source, fields, config, ctx).
///
/// # Example
///
/// ```ignore
/// let config = SelectPipelineConfig {
///     cond: select.cond,
///     split: select.split,
///     group: select.group,
///     order: select.order,
///     limit: select.limit,
///     start: select.start,
///     omit: select.omit,
///     is_value_source: all_value_sources(&select.what),
///     tempfiles: select.tempfiles,
/// };
/// let plan = plan_select_pipeline(source, Some(fields), config, ctx)?;
/// ```
#[derive(Debug, Default)]
pub(crate) struct SelectPipelineConfig {
	/// WHERE clause predicate
	pub cond: Option<crate::expr::cond::Cond>,
	/// SPLIT BY clause fields
	pub split: Option<crate::expr::split::Splits>,
	/// GROUP BY clause fields
	pub group: Option<crate::expr::group::Groups>,
	/// ORDER BY clause fields
	pub order: Option<crate::expr::order::Ordering>,
	/// LIMIT clause expression
	pub limit: Option<crate::expr::limit::Limit>,
	/// START clause expression (offset)
	pub start: Option<crate::expr::start::Start>,
	/// OMIT clause fields
	pub omit: Vec<Expr>,
	/// Whether the source is a value (array, primitive) vs record (table, record ID).
	/// This affects projection behavior for `$this` references.
	pub is_value_source: bool,
	/// Whether to use disk-based sorting (TEMPFILES hint)
	pub tempfiles: bool,
}

impl SelectPipelineConfig {
	/// Create a new config from a SELECT statement.
	pub fn from_select(
		select: &crate::expr::statements::SelectStatement,
		is_value_source: bool,
	) -> Self {
		Self {
			cond: select.cond.clone(),
			split: select.split.clone(),
			group: select.group.clone(),
			order: select.order.clone(),
			limit: select.limit.clone(),
			start: select.start.clone(),
			omit: select.omit.clone(),
			is_value_source,
			tempfiles: select.tempfiles,
		}
	}
}

// ============================================================================
// Legacy Free Functions (for backwards compatibility during migration)
// ============================================================================

/// Convert an expression to a physical expression, treating simple identifiers as string literals.
///
/// This is used for expressions like `INFO FOR USER test` where `test` is a name
/// that should be treated as a string literal, not an undefined variable.
///
/// For simple identifiers (idioms with a single Field part), returns a string literal.
/// For all other expressions, uses the normal conversion.
fn expr_to_physical_expr_as_name(
	expr: Expr,
	ctx: &FrozenContext,
) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
	use crate::exec::physical_expr::Literal as PhysicalLiteral;
	use crate::expr::part::Part;

	// Check if this is a simple identifier (idiom with single Field part)
	if let Expr::Idiom(ref idiom) = expr
		&& idiom.0.len() == 1
		&& let Part::Field(name) = &idiom.0[0]
	{
		// Convert simple identifier to string literal
		return Ok(Arc::new(PhysicalLiteral(crate::val::Value::String(name.clone()))));
	}

	// Otherwise use normal conversion
	expr_to_physical_expr(expr, ctx)
}

pub(crate) fn expr_to_physical_expr(
	expr: Expr,
	ctx: &FrozenContext,
) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
	use crate::exec::physical_expr::{
		ArrayLiteral, BinaryOp, BlockPhysicalExpr, BuiltinFunctionExec, ClosureCallExec,
		ClosureExec, IfElseExpr, JsFunctionExec, Literal as PhysicalLiteral, ModelFunctionExec,
		ObjectLiteral, Param, PostfixOp, ProjectionFunctionExec, ScalarSubquery, SetLiteral,
		SiloModuleExec, SurrealismModuleExec, UnaryOp, UserDefinedFunctionExec,
	};

	match expr {
		Expr::Literal(crate::expr::literal::Literal::Array(elements)) => {
			// Array literal - convert each element to a physical expression
			let mut phys_elements = Vec::with_capacity(elements.len());
			for elem in elements {
				phys_elements.push(expr_to_physical_expr(elem, ctx)?);
			}
			Ok(Arc::new(ArrayLiteral {
				elements: phys_elements,
			}))
		}
		Expr::Literal(crate::expr::literal::Literal::Object(entries)) => {
			// Object literal - convert each entry to a physical expression
			let mut phys_entries = Vec::with_capacity(entries.len());
			for entry in entries {
				let value = expr_to_physical_expr(entry.value, ctx)?;
				phys_entries.push((entry.key, value));
			}
			Ok(Arc::new(ObjectLiteral {
				entries: phys_entries,
			}))
		}
		Expr::Literal(crate::expr::literal::Literal::Set(elements)) => {
			// Set literal - convert each element to a physical expression
			let mut phys_elements = Vec::with_capacity(elements.len());
			for elem in elements {
				phys_elements.push(expr_to_physical_expr(elem, ctx)?);
			}
			Ok(Arc::new(SetLiteral {
				elements: phys_elements,
			}))
		}
		Expr::Literal(lit) => {
			// Convert the logical Literal to a physical Value
			let value = literal_to_value(lit)?;
			Ok(Arc::new(PhysicalLiteral(value)))
		}
		Expr::Param(param) => Ok(Arc::new(Param(param.as_str().to_string()))),
		Expr::Idiom(idiom) => convert_idiom_to_physical_expr(&idiom, ctx),
		Expr::Binary {
			left,
			op,
			right,
		} => {
			let left_phys = expr_to_physical_expr(*left, ctx)?;
			let right_phys = expr_to_physical_expr(*right, ctx)?;
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
			let inner = expr_to_physical_expr(*expr, ctx)?;
			Ok(Arc::new(UnaryOp {
				op,
				expr: inner,
			}))
		}
		Expr::Postfix {
			op,
			expr,
		} => {
			use crate::expr::operator::PostfixOperator;

			match op {
				PostfixOperator::Call(args) => {
					// Closure call - convert target and arguments to physical expressions
					let target = expr_to_physical_expr(*expr, ctx)?;
					let mut phys_args = Vec::with_capacity(args.len());
					for arg in args {
						phys_args.push(expr_to_physical_expr(arg, ctx)?);
					}
					Ok(Arc::new(ClosureCallExec {
						target,
						arguments: phys_args,
					}))
				}
				_ => {
					// Other postfix operators (Range, RangeSkip, MethodCall)
					let inner = expr_to_physical_expr(*expr, ctx)?;
					Ok(Arc::new(PostfixOp {
						op,
						expr: inner,
					}))
				}
			}
		}
		Expr::Table(table_name) => {
			// Table name as a string value
			Ok(Arc::new(PhysicalLiteral(crate::val::Value::String(
				table_name.as_str().to_string(),
			))))
		}
		Expr::FunctionCall(func_call) => {
			let FunctionCall {
				receiver,
				arguments,
			} = *func_call;

			// Function call - convert arguments to physical expressions
			let mut phys_args = Vec::with_capacity(arguments.len());
			for arg in arguments {
				phys_args.push(expr_to_physical_expr(arg, ctx)?);
			}

			// Dispatch to appropriate PhysicalExpr type based on function variant
			match receiver {
				Function::Normal(name) => {
					// Some functions need database context that the streaming executor
					// doesn't properly provide yet - fall back to legacy compute for these
					if name.starts_with("search::") || name.starts_with("api::") {
						return Err(Error::Unimplemented(format!(
							"Function '{}' not yet supported in streaming executor",
							name
						)));
					}

					// Check if this is a projection function (type::field, type::fields)
					let registry = ctx.function_registry();
					if registry.is_projection(&name) {
						// Get the projection function's required context
						let func_ctx = registry
							.get_projection(&name)
							.map(|f| f.required_context())
							.unwrap_or(crate::exec::ContextLevel::Database);
						Ok(Arc::new(ProjectionFunctionExec {
							name,
							arguments: phys_args,
							func_required_context: func_ctx,
						}))
					} else {
						// Regular scalar function
						Ok(Arc::new(BuiltinFunctionExec {
							name,
							arguments: phys_args,
							func_required_context: crate::exec::ContextLevel::Root,
						}))
					}
				}
				Function::Custom(name) => Ok(Arc::new(UserDefinedFunctionExec {
					name,
					arguments: phys_args,
				})),
				Function::Script(script) => Ok(Arc::new(JsFunctionExec {
					script,
					arguments: phys_args,
				})),
				Function::Model(model) => Ok(Arc::new(ModelFunctionExec {
					model,
					arguments: phys_args,
				})),
				Function::Module(module, sub) => Ok(Arc::new(SurrealismModuleExec {
					module,
					sub,
					arguments: phys_args,
				})),
				Function::Silo {
					org,
					pkg,
					major,
					minor,
					patch,
					sub,
				} => Ok(Arc::new(SiloModuleExec {
					org,
					pkg,
					major,
					minor,
					patch,
					sub,
					arguments: phys_args,
				})),
			}
		}
		Expr::Closure(closure) => {
			// Closure expression - wrap in physical expression
			Ok(Arc::new(ClosureExec {
				closure: *closure,
			}))
		}
		Expr::IfElse(ifelse) => {
			let IfelseStatement {
				exprs,
				close,
			} = *ifelse;
			// IF/THEN/ELSE expression - convert all branches
			let mut branches = Vec::with_capacity(exprs.len());
			for (condition, body) in exprs {
				let cond_phys = expr_to_physical_expr(condition, ctx)?;
				let body_phys = expr_to_physical_expr(body, ctx)?;
				branches.push((cond_phys, body_phys));
			}
			let otherwise = if let Some(else_expr) = close {
				Some(expr_to_physical_expr(else_expr, ctx)?)
			} else {
				None
			};
			Ok(Arc::new(IfElseExpr {
				branches,
				otherwise,
			}))
		}
		Expr::Select(select) => {
			// Scalar subquery - plan the SELECT and wrap in ScalarSubquery
			let plan = plan_select(*select, ctx)?;
			Ok(Arc::new(ScalarSubquery {
				plan,
			}))
		}

		// Control flow expressions - cannot be used in expression context
		Expr::Break => Err(Error::Unimplemented(
			"BREAK cannot be used in expression context - only valid in loops".to_string(),
		)),
		Expr::Continue => Err(Error::Unimplemented(
			"CONTINUE cannot be used in expression context - only valid in loops".to_string(),
		)),
		Expr::Return(_) => Err(Error::Unimplemented(
			"RETURN cannot be used in expression context - only valid in functions".to_string(),
		)),

		// DDL statements - cannot be used in expression context
		Expr::Define(_) => Err(Error::Unimplemented(
			"DEFINE statements cannot be used in expression context".to_string(),
		)),
		Expr::Remove(_) => Err(Error::Unimplemented(
			"REMOVE statements cannot be used in expression context".to_string(),
		)),
		Expr::Rebuild(_) => Err(Error::Unimplemented(
			"REBUILD statements cannot be used in expression context".to_string(),
		)),
		Expr::Alter(_) => Err(Error::Unimplemented(
			"ALTER statements cannot be used in expression context".to_string(),
		)),

		// Utility statements - cannot be used in expression context
		Expr::Info(_) => Err(Error::Unimplemented(
			"INFO statements cannot be used in expression context".to_string(),
		)),
		Expr::Foreach(_) => {
			Err(Error::Unimplemented("FOR loops cannot be used in expression context".to_string()))
		}
		Expr::Sleep(_) => Err(Error::Unimplemented(
			"SLEEP statements cannot be used in expression context".to_string(),
		)),
		Expr::Let(_) => Err(Error::Unimplemented(
			"LET statements cannot be used in expression context".to_string(),
		)),
		Expr::Explain {
			..
		} => Err(Error::Unimplemented(
			"EXPLAIN statements cannot be used in expression context".to_string(),
		)),

		// Value expressions - not yet implemented
		Expr::Mock(_) => Err(Error::Unimplemented(
			"Mock expressions not yet supported in execution plans".to_string(),
		)),
		Expr::Block(block) => {
			// Deferred planning: wrap the block without converting inner expressions.
			// The BlockPhysicalExpr will plan and execute each expression at evaluation
			// time, allowing LET bindings to inform subsequent expression planning.
			Ok(Arc::new(BlockPhysicalExpr {
				block: *block,
			}))
		}
		Expr::Throw(_) => Err(Error::Unimplemented(
			"THROW expressions not yet supported in execution plans".to_string(),
		)),

		// DML subqueries - not yet implemented
		Expr::Create(_) => Err(Error::Unimplemented(
			"CREATE subqueries not yet supported in execution plans".to_string(),
		)),
		Expr::Update(_) => Err(Error::Unimplemented(
			"UPDATE subqueries not yet supported in execution plans".to_string(),
		)),
		Expr::Upsert(_) => Err(Error::Unimplemented(
			"UPSERT subqueries not yet supported in execution plans".to_string(),
		)),
		Expr::Delete(_) => Err(Error::Unimplemented(
			"DELETE subqueries not yet supported in execution plans".to_string(),
		)),
		Expr::Relate(_) => Err(Error::Unimplemented(
			"RELATE subqueries not yet supported in execution plans".to_string(),
		)),
		Expr::Insert(_) => Err(Error::Unimplemented(
			"INSERT subqueries not yet supported in execution plans".to_string(),
		)),
	}
}

/// Convert a RecordIdKeyLit to a RecordIdKey for range bounds
fn convert_record_key_lit(
	key_lit: &crate::expr::record_id::RecordIdKeyLit,
) -> Result<crate::val::RecordIdKey, Error> {
	use crate::expr::record_id::RecordIdKeyLit;
	use crate::val::RecordIdKey;

	match key_lit {
		RecordIdKeyLit::Number(n) => Ok(RecordIdKey::Number(*n)),
		RecordIdKeyLit::String(s) => Ok(RecordIdKey::String(s.clone())),
		RecordIdKeyLit::Uuid(u) => Ok(RecordIdKey::Uuid(*u)),
		RecordIdKeyLit::Generate(generator) => Ok(generator.compute()),
		RecordIdKeyLit::Array(_) => Err(Error::Unimplemented(
			"Array record keys not yet supported in execution plans".to_string(),
		)),
		RecordIdKeyLit::Object(_) => Err(Error::Unimplemented(
			"Object record keys not yet supported in execution plans".to_string(),
		)),
		RecordIdKeyLit::Range(_) => Err(Error::Unimplemented(
			"Nested range record keys not supported in execution plans".to_string(),
		)),
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
		// RecordId literals - convert to RecordId value for Scan operator
		Literal::RecordId(rid_lit) => {
			use std::ops::Bound;

			use crate::expr::record_id::RecordIdKeyLit;
			use crate::val::{RecordId, RecordIdKey, RecordIdKeyRange};

			let key = match &rid_lit.key {
				RecordIdKeyLit::Number(n) => RecordIdKey::Number(*n),
				RecordIdKeyLit::String(s) => RecordIdKey::String(s.clone()),
				RecordIdKeyLit::Uuid(u) => RecordIdKey::Uuid(*u),
				RecordIdKeyLit::Generate(generator) => generator.compute(),
				RecordIdKeyLit::Range(range_lit) => {
					// Convert RecordIdKeyRangeLit to RecordIdKeyRange
					let start = match &range_lit.start {
						Bound::Unbounded => Bound::Unbounded,
						Bound::Included(key_lit) => {
							Bound::Included(convert_record_key_lit(key_lit)?)
						}
						Bound::Excluded(key_lit) => {
							Bound::Excluded(convert_record_key_lit(key_lit)?)
						}
					};
					let end = match &range_lit.end {
						Bound::Unbounded => Bound::Unbounded,
						Bound::Included(key_lit) => {
							Bound::Included(convert_record_key_lit(key_lit)?)
						}
						Bound::Excluded(key_lit) => {
							Bound::Excluded(convert_record_key_lit(key_lit)?)
						}
					};
					RecordIdKey::Range(Box::new(RecordIdKeyRange {
						start,
						end,
					}))
				}
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
			};

			Ok(Value::RecordId(RecordId {
				table: rid_lit.table,
				key,
			}))
		}
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

/// Plan an expression into an executable operator tree.
///
/// This is the main entry point for the planner. It delegates to `Planner::plan()`
/// which handles all top-level statement types.
///
/// # Errors
///
/// Returns `Error::Unimplemented` for statements not yet supported in the
/// streaming executor.
pub(crate) fn try_plan_expr(
	expr: Expr,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	Planner::new(ctx).plan(expr)
}

/// Check if an expression contains KNN (vector search) operators.
///
/// KNN operators require special index infrastructure to evaluate. The streaming
/// executor doesn't support KNN yet, so we need to fall back to the old execution path.
/// Note: MATCHES (full-text search) is now supported via FullTextScan operator.
fn contains_knn_operator(expr: &Expr) -> bool {
	match expr {
		Expr::Binary {
			left,
			op,
			right,
		} => {
			// Check for KNN operators only (MATCHES is now supported)
			if matches!(op, BinaryOperator::NearestNeighbor(_)) {
				return true;
			}
			// Recursively check children
			contains_knn_operator(left) || contains_knn_operator(right)
		}
		Expr::Prefix {
			expr: inner,
			..
		} => contains_knn_operator(inner),
		// For other expression types, no KNN operators
		_ => false,
	}
}

/// Check if an expression contains MATCHES operators that cannot be indexed.
///
/// MATCHES operators can be indexed via FullTextScan when they're at the top level
/// or combined with AND. However, when MATCHES is inside an OR branch, the index
/// analyzer cannot use an index for it, so we would need to evaluate MATCHES at
/// runtime - which the streaming executor doesn't support.
///
/// Returns true if MATCHES appears within an OR subtree.
fn contains_non_indexable_matches(expr: &Expr) -> bool {
	contains_matches_in_or(expr, false)
}

/// Helper function to track if we're inside an OR branch while looking for MATCHES.
fn contains_matches_in_or(expr: &Expr, inside_or: bool) -> bool {
	match expr {
		Expr::Binary {
			left,
			op,
			right,
		} => {
			// If we're inside an OR and find MATCHES, that's non-indexable
			if inside_or && matches!(op, BinaryOperator::Matches(_)) {
				return true;
			}

			// For OR, mark that we're inside an OR branch
			let new_inside_or = inside_or || matches!(op, BinaryOperator::Or);

			// Recursively check children
			contains_matches_in_or(left, new_inside_or)
				|| contains_matches_in_or(right, new_inside_or)
		}
		Expr::Prefix {
			expr: inner,
			..
		} => contains_matches_in_or(inner, inside_or),
		// For other expression types, no MATCHES operators
		_ => false,
	}
}

/// Plan a SELECT statement
///
/// The operator pipeline is built in this order:
/// 1. Scan/Union (source from FROM clause)
/// 2. Filter (WHERE)
/// 3. Split (SPLIT BY)
/// 4. Aggregate (GROUP BY)
/// 5. Sort (ORDER BY)
/// 6. Limit (LIMIT/START)
/// 7. Fetch (FETCH)
/// 8. Project (SELECT fields) or ProjectValue (SELECT VALUE)
/// 9. Timeout (TIMEOUT)
fn plan_select(
	select: crate::expr::statements::SelectStatement,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	let crate::expr::statements::SelectStatement {
		mut fields,
		omit,
		only,
		what,
		with,
		cond,
		split,
		group,
		order,
		limit,
		start,
		fetch,
		version,
		timeout,
		explain,
		tempfiles,
	} = select;

	// ONLY clause (unwraps single results)
	if only {
		return Err(Error::Unimplemented(
			"SELECT ... ONLY not yet supported in execution plans".to_string(),
		));
	}

	// EXPLAIN clause (query explain output)
	if explain.is_some() {
		return Err(Error::Unimplemented(
			"SELECT ... EXPLAIN not yet supported in execution plans".to_string(),
		));
	}

	// Check for KNN operators in WHERE clause - these require index executor
	// context that the streaming pipeline doesn't yet support. Fall back to old path.
	// Note: MATCHES (full-text search) is now supported via FullTextScan operator.
	if let Some(ref c) = cond {
		if contains_knn_operator(&c.0) {
			return Err(Error::Unimplemented(
				"WHERE clause with KNN operators not yet supported in streaming executor"
					.to_string(),
			));
		}
		// Check for MATCHES operators within OR conditions - these cannot be indexed
		// because the index analyzer doesn't support OR across different index types.
		// The streaming executor can't evaluate MATCHES at runtime, so fall back.
		if contains_non_indexable_matches(&c.0) {
			return Err(Error::Unimplemented(
				"WHERE clause with MATCHES in OR conditions not yet supported in streaming executor"
					.to_string(),
			));
		}
	}

	// Extract VERSION timestamp if present (for time-travel queries)
	let version = extract_version(version)?;

	// Check if all sources are "value sources" (arrays, primitives) before consuming `what`.
	// This affects projection behavior: `SELECT $this FROM [1,2,3]` should return raw values,
	// while `SELECT $this FROM table` should wrap in `{ this: ... }`.
	let is_value_source = all_value_sources(&what);

	// Build the source plan from `what` (FROM clause)
	// Pass cond, order, and with for index selection in Scan operator
	let source =
		plan_select_sources(what, version, cond.as_ref(), order.as_ref(), with.as_ref(), ctx)?;

	// Build pipeline configuration
	let config = SelectPipelineConfig {
		cond,
		split,
		group,
		order,
		limit,
		start,
		omit,
		is_value_source,
		tempfiles,
	};

	// Apply the shared pipeline: Filter -> Split -> Aggregate -> Sort -> Limit -> Project
	let projected = plan_select_pipeline(source, Some(fields.clone()), config, ctx)?;

	// Apply FETCH if present - after projections so it can expand record IDs
	// in computed fields like graph traversals
	let fetched = plan_fetch(fetch, projected, &mut fields)?;

	// Apply TIMEOUT if present (timeout is always Expr but may be Literal::None)
	let timed = match timeout {
		Expr::Literal(Literal::None) => fetched,
		timeout_expr => {
			let timeout_phys = expr_to_physical_expr(timeout_expr, ctx)?;
			Arc::new(Timeout {
				input: fetched,
				timeout: Some(timeout_phys),
			}) as Arc<dyn ExecOperator>
		}
	};

	Ok(timed)
}

/// Plan the SELECT pipeline after the source is determined.
///
/// This applies the standard query pipeline: Filter -> Split -> Aggregate -> Sort -> Limit ->
/// Project. Used by both `plan_select` and `plan_lookup` to share the common operator chain.
///
/// # Parameters
///
/// - `source`: The already-planned source operator (Scan, GraphEdgeScan, Union, etc.)
/// - `fields`: Optional fields for projection. If None, passes through all fields.
/// - `config`: Pipeline configuration (WHERE, SPLIT, GROUP, ORDER, LIMIT, START, OMIT, etc.)
/// - `ctx`: The frozen context
fn plan_select_pipeline(
	source: Arc<dyn ExecOperator>,
	fields: Option<Fields>,
	config: SelectPipelineConfig,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	// Destructure config for easier access
	let SelectPipelineConfig {
		cond,
		split,
		group,
		order,
		limit,
		start,
		omit,
		is_value_source,
		tempfiles,
	} = config;

	// Apply WHERE clause if present
	let filtered = if let Some(cond) = cond {
		let predicate = expr_to_physical_expr(cond.0, ctx)?;
		Arc::new(Filter {
			input: source,
			predicate,
		}) as Arc<dyn ExecOperator>
	} else {
		source
	};

	// Apply SPLIT BY if present
	let split_op = if let Some(splits) = split {
		let idioms: Vec<_> = splits.into_iter().map(|s| s.0).collect();
		Arc::new(Split {
			input: filtered,
			idioms,
		}) as Arc<dyn ExecOperator>
	} else {
		filtered
	};

	// Get fields or use default (SELECT *)
	let fields = fields.unwrap_or_else(Fields::all);

	// Apply GROUP BY if present
	let (grouped, skip_projections) = if let Some(groups) = group {
		let group_by: Vec<_> = groups.0.into_iter().map(|g| g.0).collect();

		// Validate: $this and $parent are invalid in GROUP BY context
		check_forbidden_group_by_params(&fields)?;

		// Build aggregate fields and group-by expressions from the SELECT expression
		let (aggregates, group_by_exprs) =
			plan_aggregation_with_group_exprs(&fields, &group_by, ctx)?;

		// For GROUP BY, the Aggregate operator handles projections internally
		(
			Arc::new(Aggregate {
				input: split_op,
				group_by,
				group_by_exprs,
				aggregates,
			}) as Arc<dyn ExecOperator>,
			true,
		)
	} else {
		(split_op, false)
	};

	// Apply ORDER BY if present
	let (sorted, sort_only_omits) = if let Some(ref order) = order {
		if skip_projections {
			// GROUP BY present - use legacy approach (Aggregate handles expressions)
			(plan_sort(grouped, order, &start, &limit, tempfiles, ctx)?, vec![])
		} else {
			// No GROUP BY - use consolidated approach
			plan_sort_consolidated(grouped, order, &fields, &start, &limit, tempfiles, ctx)?
		}
	} else {
		(grouped, vec![])
	};

	// Apply LIMIT/START if present
	let limited = if limit.is_some() || start.is_some() {
		let limit_expr = if let Some(ref limit) = limit {
			Some(expr_to_physical_expr(limit.0.clone(), ctx)?)
		} else {
			None
		};
		let offset_expr = if let Some(ref start) = start {
			Some(expr_to_physical_expr(start.0.clone(), ctx)?)
		} else {
			None
		};
		Arc::new(Limit {
			input: sorted,
			limit: limit_expr,
			offset: offset_expr,
		}) as Arc<dyn ExecOperator>
	} else {
		sorted
	};

	// Combine user-specified OMIT with sort-only computed fields
	let mut all_omit = omit;
	for field_name in sort_only_omits {
		all_omit.push(Expr::Idiom(Idiom::field(field_name)));
	}

	// Apply projections (SELECT fields or SELECT VALUE)
	// Skip if GROUP BY is present (handled by Aggregate operator)
	let projected = if skip_projections {
		// GROUP BY case - skip projections but apply OMIT if needed
		if !all_omit.is_empty() {
			let omit_fields = plan_omit_fields(all_omit, ctx)?;
			Arc::new(Project {
				input: limited,
				fields: vec![],
				omit: omit_fields,
				include_all: true,
			}) as Arc<dyn ExecOperator>
		} else {
			limited
		}
	} else {
		plan_projections(&fields, &all_omit, limited, ctx, is_value_source)?
	};

	Ok(projected)
}

/// Check if a source expression represents a "value source" (array, primitive)
/// as opposed to a "record source" (table, record ID).
///
/// Value sources should use raw value projection for `SELECT $this`,
/// while record sources should wrap in `{ this: ... }`.
fn is_value_source_expr(expr: &Expr) -> bool {
	match expr {
		// Array literals are value sources - elements are iterated directly
		Expr::Literal(Literal::Array(_)) => true,
		// String, number, etc. are value sources
		Expr::Literal(Literal::String(_))
		| Expr::Literal(Literal::Integer(_))
		| Expr::Literal(Literal::Float(_))
		| Expr::Literal(Literal::Decimal(_))
		| Expr::Literal(Literal::Bool(_))
		| Expr::Literal(Literal::None)
		| Expr::Literal(Literal::Null) => true,
		// Tables are record sources
		Expr::Table(_) => false,
		// Record IDs are record sources
		Expr::Literal(Literal::RecordId(_)) => false,
		// Parameters might be anything - conservatively treat as record source
		// unless we can resolve them at planning time
		Expr::Param(_) => false,
		// Subqueries return records
		Expr::Select(_) => false,
		// Other expressions - conservatively treat as record source
		_ => false,
	}
}

/// Check if ALL source expressions are value sources.
fn all_value_sources(sources: &[Expr]) -> bool {
	!sources.is_empty() && sources.iter().all(is_value_source_expr)
}

/// Plan projections (SELECT fields or SELECT VALUE)
///
/// This handles:
/// - `SELECT *` - pass through without projection
/// - `SELECT * OMIT field` - use Project with empty fields and omit populated
/// - `SELECT VALUE expr` - use ProjectValue operator
/// - `SELECT field1, field2` - use Project operator
/// - `SELECT field1, *, field2` - mixed wildcards (returns Unimplemented for now)
///
/// When `is_value_source` is true (source is array/primitive), single `$this` or `$param`
/// projections without explicit aliases use ProjectValue (raw values) instead of Project.
fn plan_projections(
	fields: &Fields,
	omit: &[Expr],
	input: Arc<dyn ExecOperator>,
	ctx: &FrozenContext,
	is_value_source: bool,
) -> Result<Arc<dyn ExecOperator>, Error> {
	match fields {
		// SELECT VALUE expr - return raw values (OMIT doesn't make sense here)
		Fields::Value(selector) => {
			if !omit.is_empty() {
				return Err(Error::Unimplemented(
					"OMIT clause with SELECT VALUE not supported".to_string(),
				));
			}
			let expr = expr_to_physical_expr(selector.expr.clone(), ctx)?;
			Ok(Arc::new(ProjectValue {
				input,
				expr,
			}) as Arc<dyn ExecOperator>)
		}

		// SELECT field1, field2, ... or SELECT *
		Fields::Select(field_list) => {
			// Check if this is just SELECT * (all fields, no specific fields)
			let is_select_all =
				field_list.len() == 1 && matches!(field_list.first(), Some(Field::All));

			if is_select_all {
				// SELECT * - use Project operator to handle RecordId dereferencing
				// and apply OMIT if present
				let omit_fields = if !omit.is_empty() {
					plan_omit_fields(omit.to_vec(), ctx)?
				} else {
					vec![]
				};
				return Ok(Arc::new(Project {
					input,
					fields: vec![], // No specific fields - pass through
					omit: omit_fields,
					include_all: true,
				}) as Arc<dyn ExecOperator>);
			}

			// Check for wildcards mixed with specific fields
			let has_wildcard = field_list.iter().any(|f| matches!(f, Field::All));

			// OMIT doesn't make sense with specific field projections (without wildcard)
			if !omit.is_empty() && !has_wildcard {
				return Err(Error::Unimplemented(
					"OMIT clause with specific field projections not supported".to_string(),
				));
			}

			// Special case: For value sources (arrays, primitives), a single `$this` or `$param`
			// without an explicit alias should return raw values (like SELECT VALUE).
			// This matches legacy behavior where `SELECT $this FROM [1,2,3]` returns `[1,2,3]`.
			if is_value_source
				&& !has_wildcard
				&& field_list.len() == 1
				&& let Some(Field::Single(selector)) = field_list.first()
			{
				// Check if it's a bare parameter reference without alias
				if selector.alias.is_none()
					&& let Expr::Param(_) = &selector.expr
				{
					// Use ProjectValue to return raw values
					let expr = expr_to_physical_expr(selector.expr.clone(), ctx)?;
					return Ok(Arc::new(ProjectValue {
						input,
						expr,
					}) as Arc<dyn ExecOperator>);
				}
			}

			// Build field selections for specific fields (skip wildcards)
			let mut field_selections = Vec::with_capacity(field_list.len());

			for field in field_list {
				if let Field::Single(selector) = field {
					// Convert expression to physical
					let expr = expr_to_physical_expr(selector.expr.clone(), ctx)?;

					// Determine the output name and whether it's an explicit alias
					let field_selection = if let Some(alias) = &selector.alias {
						// User provided explicit alias - use it
						let output_name = idiom_to_field_name(alias);
						FieldSelection::with_alias(output_name, expr)
					} else {
						// No alias - derive output path from expression
						// For idioms with graph traversals, this creates nested output
						match &selector.expr {
							Expr::Idiom(idiom) => {
								let output_path = idiom_to_field_path(idiom);
								FieldSelection::from_field_path(output_path, expr)
							}
							_ => {
								// Non-idiom expressions use flat field name
								let output_name = derive_field_name(&selector.expr);
								FieldSelection::new(output_name, expr)
							}
						}
					};

					field_selections.push(field_selection);
				}
				// Skip Field::All - handled by include_all flag
			}

			// Handle OMIT if present (only valid with wildcards)
			let omit_fields = if has_wildcard && !omit.is_empty() {
				plan_omit_fields(omit.to_vec(), ctx)?
			} else {
				vec![]
			};

			Ok(Arc::new(Project {
				input,
				fields: field_selections,
				omit: omit_fields,
				include_all: has_wildcard,
			}) as Arc<dyn ExecOperator>)
		}
	}
}

/// Plan OMIT fields - convert expressions to idioms
fn plan_omit_fields(
	omit: Vec<Expr>,
	_ctx: &FrozenContext,
) -> Result<Vec<crate::expr::idiom::Idiom>, Error> {
	let mut fields = Vec::with_capacity(omit.len());

	for expr in omit {
		match expr {
			Expr::Idiom(idiom) => {
				fields.push(idiom);
			}
			_ => {
				// Only simple idiom references are supported for OMIT
				return Err(Error::Unimplemented(
					"OMIT with non-idiom expressions not supported in execution plans".to_string(),
				));
			}
		}
	}

	Ok(fields)
}

/// Derive a field name from an expression for projection output
fn derive_field_name(expr: &Expr) -> String {
	match expr {
		// Simple field reference - extract the raw field name
		Expr::Idiom(idiom) => idiom_to_field_name(idiom),
		// Parameter reference - use the parameter name (without $)
		// e.g., $this -> "this", $parent -> "parent"
		Expr::Param(param) => param.as_str().to_string(),
		// Function call - use the function's idiom representation (name without arguments)
		Expr::FunctionCall(call) => {
			let idiom: crate::expr::idiom::Idiom = call.receiver.to_idiom();
			idiom_to_field_name(&idiom)
		}
		// For other expressions, use the SQL representation
		_ => {
			use surrealdb_types::ToSql;
			expr.to_sql()
		}
	}
}

/// Extract a field name from an idiom, preferring raw names for simple idioms.
///
/// This mirrors the legacy behavior where the idiom is simplified (removing
/// Destructure, All, Where, etc.) before deriving the field name.
///
/// For graph traversal aliases like `->(bought AS purchases)`, the alias is used.
fn idiom_to_field_name(idiom: &crate::expr::idiom::Idiom) -> String {
	use surrealdb_types::ToSql;

	use crate::expr::part::Part;

	// Check for graph traversal alias first
	// For expressions like `->(bought AS purchases)`, use the alias as the field name
	for part in idiom.0.iter() {
		if let Part::Lookup(lookup) = part
			&& let Some(alias) = &lookup.alias
		{
			// Recursively extract field name from alias
			return idiom_to_field_name(alias);
		}
	}

	// Simplify the idiom first - this removes Destructure, All, Where, etc.
	// and keeps only Field, Start, and Lookup parts
	let simplified = idiom.simplify();

	// For simple single-part idioms, use the raw field name
	if simplified.len() == 1
		&& let Some(Part::Field(name)) = simplified.first()
	{
		return name.clone();
	}
	// For complex idioms, use the SQL representation of the simplified idiom
	simplified.to_sql()
}

/// Extract a field path from an idiom for nested output construction.
///
/// For graph traversals without aliases, this returns a path that splits by Lookup parts
/// to create nested output structure. For example, `->reports_to->person` becomes
/// Returns a `FieldPath` for the given idiom.
///
/// For idioms with aliases or without Lookup parts, returns a simple field path.
/// For idioms with unaliased graph traversals like `->reports_to->person`,
/// returns a FieldPath with Lookup parts for proper nested output.
fn idiom_to_field_path(idiom: &crate::expr::idiom::Idiom) -> FieldPath {
	use surrealdb_types::ToSql;

	use crate::expr::part::Part;

	// Check for graph traversal alias first - if any Lookup has an alias, use flat output
	for part in idiom.0.iter() {
		if let Part::Lookup(lookup) = part
			&& lookup.alias.is_some()
		{
			// Has explicit alias - use flat output (single field name)
			return FieldPath::field(idiom_to_field_name(idiom));
		}
	}

	// Check if this idiom contains any Lookup parts (graph traversals)
	let has_lookups = idiom.0.iter().any(|p| matches!(p, Part::Lookup(_)));

	if !has_lookups {
		// No graph traversals - use standard field name (which handles dot-separated paths)
		let name = idiom_to_field_name(idiom);
		if name.contains('.') && !name.contains(['[', '(', ' ']) {
			return FieldPath(
				name.split('.').map(|s| FieldPathPart::Field(s.to_string())).collect(),
			);
		}
		return FieldPath::field(name);
	}

	// Has Lookup parts without aliases - split into nested path
	// Each Lookup part becomes a FieldPathPart::Lookup, fields become FieldPathPart::Field
	let mut parts = Vec::new();

	for part in idiom.0.iter() {
		match part {
			Part::Lookup(lookup) => {
				// Add the lookup as its own path component
				// Use to_sql() to get the full representation including any subquery clauses
				// (ORDER BY, WHERE, GROUP BY, LIMIT, etc.)
				let lookup_key = lookup.to_sql();
				parts.push(FieldPathPart::Lookup(lookup_key));
			}
			Part::Field(name) => {
				// Regular field - add as Field part
				parts.push(FieldPathPart::Field(name.clone()));
			}
			// Skip other parts (Destructure, All, Where, etc.) for path construction
			// These affect evaluation but not the output field structure
			_ => {}
		}
	}

	// If no path was built (shouldn't happen), fall back to flat name
	if parts.is_empty() {
		return FieldPath::field(idiom.to_sql());
	}

	FieldPath(parts)
}

/// Plan FETCH clause
fn plan_fetch(
	fetch: Option<crate::expr::fetch::Fetchs>,
	input: Arc<dyn ExecOperator>,
	_projection: &mut Fields,
) -> Result<Arc<dyn ExecOperator>, Error> {
	let Some(fetchs) = fetch else {
		return Ok(input);
	};

	// Convert fetch expressions to idioms
	// We only support simple idiom fetches for now
	let mut fields = Vec::with_capacity(fetchs.len());
	for fetch_item in fetchs {
		// The Fetch struct wraps an Expr in field .0
		match fetch_item.0 {
			Expr::Idiom(idiom) => {
				fields.push(idiom.clone());
				// Note: We don't add FETCH fields to the projection list.
				// FETCH expands record references in-place but doesn't affect
				// which fields appear in the final output - that's determined
				// solely by the SELECT field list.
			}
			_ => {
				// Complex fetch expressions (params, function calls) not yet supported
				return Err(Error::Unimplemented(
					"FETCH with non-idiom expressions not yet supported in execution plans"
						.to_string(),
				));
			}
		}
	}

	Ok(Arc::new(Fetch {
		input,
		fields,
	}) as Arc<dyn ExecOperator>)
}

/// Plan aggregation fields from SELECT expression and GROUP BY.
///
/// This extracts:
/// - Group-by keys (passed through unchanged)
/// - Aggregate functions (detected via the function registry)
/// - Other expressions (evaluated with the first value in the group)
///
/// Also returns the physical expressions for computing group keys.
/// GROUP BY aliases are expanded to their actual expressions.
fn plan_aggregation_with_group_exprs(
	fields: &Fields,
	group_by: &[crate::expr::idiom::Idiom],
	ctx: &FrozenContext,
) -> Result<(Vec<AggregateField>, Vec<Arc<dyn crate::exec::PhysicalExpr>>), Error> {
	use surrealdb_types::ToSql;

	// Get the function registry from the context
	let registry = ctx.function_registry();

	// Build a map of alias -> expression from the SELECT fields
	// This allows us to expand GROUP BY aliases to actual expressions
	let mut alias_to_expr: std::collections::HashMap<String, Expr> =
		std::collections::HashMap::new();
	match fields {
		Fields::Value(selector) => {
			if let Some(alias) = &selector.alias {
				alias_to_expr.insert(alias.to_sql(), selector.expr.clone());
			}
		}
		Fields::Select(field_list) => {
			for field in field_list {
				if let Field::Single(selector) = field
					&& let Some(alias) = &selector.alias
				{
					alias_to_expr.insert(alias.to_sql(), selector.expr.clone());
				}
			}
		}
	}

	// Build group-by expressions, expanding aliases where needed
	let mut group_by_exprs = Vec::with_capacity(group_by.len());
	for idiom in group_by {
		let idiom_str = idiom.to_sql();
		// Check if this idiom is an alias for a SELECT expression
		let expr = if let Some(select_expr) = alias_to_expr.get(&idiom_str) {
			// Alias found - use the actual expression
			select_expr.clone()
		} else {
			// Not an alias - use the idiom directly
			Expr::Idiom(idiom.clone())
		};
		let physical_expr = expr_to_physical_expr(expr, ctx)?;
		group_by_exprs.push(physical_expr);
	}

	match fields {
		// SELECT VALUE with GROUP BY - the VALUE expression may contain aggregates
		Fields::Value(selector) => {
			// Check if the VALUE expression is a group-by key
			let group_key_index =
				find_group_key_index(&selector.expr, selector.alias.as_ref(), group_by);
			let is_group_key = group_key_index.is_some();

			let (aggregate_expr_info, fallback_expr) = if is_group_key {
				// Group-by key - no aggregate, no fallback expr needed
				(None, None)
			} else {
				// Try to extract aggregate function info
				extract_aggregate_info(&selector.expr, &registry, ctx)?
			};

			// For VALUE, we use an empty name since the result isn't wrapped in an object
			Ok((
				vec![AggregateField::new(
					String::new(),
					is_group_key,
					group_key_index,
					aggregate_expr_info,
					fallback_expr,
				)],
				group_by_exprs,
			))
		}

		// SELECT field1, field2, ... with GROUP BY
		Fields::Select(field_list) => {
			let mut aggregates = Vec::with_capacity(field_list.len());

			for field in field_list {
				match field {
					Field::All => {
						// SELECT * with GROUP BY doesn't make sense
						return Err(Error::Unimplemented(
							"SELECT * with GROUP BY not supported in execution plans".to_string(),
						));
					}
					Field::Single(selector) => {
						// Determine the output name
						let output_name = if let Some(alias) = &selector.alias {
							idiom_to_field_name(alias)
						} else {
							derive_field_name(&selector.expr)
						};

						// Check if this is a group-by key
						let group_key_index =
							find_group_key_index(&selector.expr, selector.alias.as_ref(), group_by);
						let is_group_key = group_key_index.is_some();

						let (aggregate_expr_info, fallback_expr) = if is_group_key {
							// Group-by key - no aggregate, no fallback expr needed
							(None, None)
						} else {
							// Try to extract aggregate function info
							extract_aggregate_info(&selector.expr, &registry, ctx)?
						};

						aggregates.push(AggregateField::new(
							output_name,
							is_group_key,
							group_key_index,
							aggregate_expr_info,
							fallback_expr,
						));
					}
				}
			}

			Ok((aggregates, group_by_exprs))
		}
	}
}

/// Find the index of the group-by key for an expression.
///
/// Returns the index if:
/// 1. The expression is a simple idiom that matches a group-by idiom
/// 2. The expression has an alias that matches a group-by idiom
fn find_group_key_index(
	expr: &Expr,
	alias: Option<&Idiom>,
	group_by: &[crate::expr::idiom::Idiom],
) -> Option<usize> {
	use surrealdb_types::ToSql;

	// Check if the expression itself is a simple idiom that matches
	if let Expr::Idiom(idiom) = expr
		&& let Some(idx) = group_by.iter().position(|g| g.to_sql() == idiom.to_sql())
	{
		return Some(idx);
	}

	// Check if the alias matches a group-by idiom
	// This handles cases like `time::year(time) AS year` with `GROUP BY year`
	if let Some(alias) = alias
		&& let Some(idx) = group_by.iter().position(|g| g.to_sql() == alias.to_sql())
	{
		return Some(idx);
	}

	None
}

// ============================================================================
// Aggregate Extraction using MutVisitor
// ============================================================================

/// Visitor that extracts aggregate functions from an expression.
///
/// Walks the expression tree, finds all aggregate function calls, and replaces
/// them with synthetic field references (`_a0`, `_a1`, etc.). The extracted
/// aggregates are stored in the visitor for later processing.
///
/// Supports multiple aggregates in a single expression (e.g., `SUM(a) + AVG(a)`).
struct AggregateExtractor<'a> {
	/// The function registry to look up aggregate functions.
	registry: &'a crate::exec::function::FunctionRegistry,
	/// Extracted aggregates with their function names and calls.
	aggregates: Vec<(String, FunctionCall)>,
	/// Counter for generating unique synthetic field names.
	aggregate_count: usize,
	/// Track if we're inside an aggregate's arguments (to detect nesting).
	inside_aggregate: bool,
	/// Error encountered during traversal (nested aggregates).
	error: Option<Error>,
}

impl<'a> AggregateExtractor<'a> {
	fn new(registry: &'a crate::exec::function::FunctionRegistry) -> Self {
		Self {
			registry,
			aggregates: Vec::new(),
			aggregate_count: 0,
			inside_aggregate: false,
			error: None,
		}
	}

	/// Check if an expression directly contains an aggregate function call at the top level.
	/// Used to determine if array::distinct should be treated as an aggregate or scalar.
	fn contains_aggregate_call(&self, expr: &Expr) -> bool {
		if let Expr::FunctionCall(func_call) = expr
			&& let Function::Normal(name) = &func_call.receiver
		{
			return self.registry.get_aggregate(name.as_str()).is_some();
		}
		false
	}
}

impl MutVisitor for AggregateExtractor<'_> {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		// Don't continue if we've already encountered an error
		if self.error.is_some() {
			return Ok(());
		}

		// Check if this is an aggregate function call that we need to replace
		// Note: We handle FunctionCall here because after visiting, we need to
		// replace the entire Expr, not just the FunctionCall inside it
		if let Expr::FunctionCall(func_call) = expr
			&& let Function::Normal(name) = &func_call.receiver
		{
			// Special handling for array::distinct:
			// - When its argument is another aggregate function, treat it as a scalar (e.g.,
			//   array::distinct(array::group(name)) - array::distinct is scalar)
			// - When its argument is NOT an aggregate function, treat it as an aggregate (e.g.,
			//   array::distinct(name) - array::distinct collects unique values)
			if name.as_str() == "array::distinct"
				&& !func_call.arguments.is_empty()
				&& self.contains_aggregate_call(&func_call.arguments[0])
			{
				// Argument contains an aggregate - treat array::distinct as scalar
				// Just visit children to process the nested aggregate
				return expr.visit_mut(self);
			}
			// Fall through to normal aggregate handling

			if self.registry.get_aggregate(name.as_str()).is_some() {
				// Found an aggregate function
				if self.inside_aggregate {
					// Nested aggregates are not allowed
					self.error = Some(Error::Unimplemented(
						"Nested aggregate functions are not supported".to_string(),
					));
					return Ok(());
				}

				// Visit arguments to check for nested aggregates
				self.inside_aggregate = true;
				for arg in &mut func_call.arguments {
					arg.visit_mut(self)?;
				}
				self.inside_aggregate = false;

				// Check if visiting arguments found an error
				if self.error.is_some() {
					return Ok(());
				}

				// Store this aggregate and replace with field reference
				let field_name = aggregate_field_name(self.aggregate_count);
				self.aggregates.push((name.clone(), func_call.as_ref().clone()));
				self.aggregate_count += 1;

				// Replace the aggregate with a field reference idiom
				*expr = Expr::Idiom(Idiom::field(field_name));
				return Ok(());
			}
		}

		// Continue visiting children for non-aggregate expressions
		expr.visit_mut(self)
	}

	// Override visit_mut_function_call to ensure arguments go through visit_mut_expr.
	// This is critical for detecting aggregates nested inside scalar functions
	// (e.g., `array::distinct(array::group(name))`). By calling visit_mut_expr
	// directly, we ensure aggregate detection and replacement happens properly.
	fn visit_mut_function_call(&mut self, f: &mut FunctionCall) -> Result<(), Self::Error> {
		if self.error.is_some() {
			return Ok(());
		}

		// Visit all arguments through visit_mut_expr to ensure aggregate detection
		// Note: We don't handle aggregates here - that's done in visit_mut_expr
		// which can replace the entire Expr
		for arg in &mut f.arguments {
			self.visit_mut_expr(arg)?;
		}
		Ok(())
	}

	// Override to prevent descending into subqueries
	// (aggregates in subqueries belong to a different context)
	fn visit_mut_select(
		&mut self,
		_s: &mut crate::expr::statements::SelectStatement,
	) -> Result<(), Self::Error> {
		// Don't visit into SELECT subqueries - they have their own aggregate context
		Ok(())
	}

	fn visit_mut_create(
		&mut self,
		_s: &mut crate::expr::statements::CreateStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}

	fn visit_mut_update(
		&mut self,
		_s: &mut crate::expr::statements::UpdateStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}

	fn visit_mut_delete(
		&mut self,
		_s: &mut crate::expr::statements::DeleteStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}
}

/// Extract aggregate function information from an expression.
///
/// Uses the MutVisitor pattern to walk the expression tree and find all
/// aggregate functions. Supports:
/// - Direct aggregate calls: `math::mean(v)`
/// - Nested expressions: `math::mean(v) + 1`
/// - Multiple aggregates: `SUM(a) + AVG(a)`
///
/// If no aggregates are found, uses implicit `array::group` aggregation
/// (SurrealDB's default GROUP BY behavior for non-aggregate fields).
fn extract_aggregate_info(
	expr: &Expr,
	registry: &crate::exec::function::FunctionRegistry,
	ctx: &FrozenContext,
) -> Result<(Option<AggregateExprInfo>, Option<Arc<dyn crate::exec::PhysicalExpr>>), Error> {
	// Clone the expression for mutation
	let mut expr_clone = expr.clone();

	// Run the extractor - must call visit_mut_expr directly, not visit_mut,
	// so that our override is called first (otherwise VisitMut dispatches
	// to visit_mut_function_call for FunctionCall expressions)
	let mut extractor = AggregateExtractor::new(registry);
	let _ = extractor.visit_mut_expr(&mut expr_clone);

	// Check for errors (nested aggregates)
	if let Some(err) = extractor.error {
		return Err(err);
	}

	if extractor.aggregates.is_empty() {
		// No aggregates found - use implicit array::group aggregation
		// This collects all values into an array (SurrealDB's default GROUP BY behavior)
		let argument_expr = expr_to_physical_expr(expr.clone(), ctx)?;
		let array_group = registry
			.get_aggregate("array::group")
			.expect("array::group should always be registered")
			.clone();
		return Ok((
			Some(AggregateExprInfo {
				aggregates: vec![ExtractedAggregate {
					function: array_group,
					argument_expr,
					extra_args: vec![],
				}],
				post_expr: None,
			}),
			None,
		));
	}

	// Convert extracted aggregates to ExtractedAggregate structs
	let extracted_aggregates = extractor
		.aggregates
		.into_iter()
		.map(|(name, call)| {
			// Special handling for count: count() vs count(expr)
			// - count() with no arguments counts all rows (uses Count)
			// - count(expr) with arguments counts truthy values (uses CountField)
			let func = if name.as_str() == "count" {
				registry.get_count_aggregate(!call.arguments.is_empty())
			} else {
				registry.get_aggregate(&name).expect("aggregate function should exist").clone()
			};

			// Extract the argument expression
			let argument_expr = if call.arguments.is_empty() {
				expr_to_physical_expr(Expr::Literal(Literal::None), ctx)
			} else {
				expr_to_physical_expr(call.arguments[0].clone(), ctx)
			}?;

			// Extract extra arguments (for functions like array::join)
			let extra_args = if call.arguments.len() > 1 {
				call.arguments[1..]
					.iter()
					.map(|arg| expr_to_physical_expr(arg.clone(), ctx))
					.collect::<Result<Vec<_>, _>>()?
			} else {
				vec![]
			};

			Ok(ExtractedAggregate {
				function: func,
				argument_expr,
				extra_args,
			})
		})
		.collect::<Result<Vec<_>, Error>>()?;

	// Determine if we need a post-expression
	// If it's a direct single aggregate (the transformed expr is just `_a0`),
	// we don't need a post-expression
	let is_direct_single_aggregate = extracted_aggregates.len() == 1 && {
		use surrealdb_types::ToSql;
		matches!(&expr_clone, Expr::Idiom(i) if i.to_sql() == "_a0")
	};

	let post_expr = if is_direct_single_aggregate {
		None
	} else {
		// Convert the transformed expression (with _a0, _a1 references)
		Some(expr_to_physical_expr(expr_clone, ctx)?)
	};

	Ok((
		Some(AggregateExprInfo {
			aggregates: extracted_aggregates,
			post_expr,
		}),
		None,
	))
}

/// Extract version timestamp from VERSION clause expression.
/// Currently only supports literal Datetime values.
fn extract_version(version_expr: Expr) -> Result<Option<u64>, Error> {
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

/// Check if an expression contains `$this` or `$parent` parameters.
/// These are invalid in GROUP BY context since there's no single document to reference.
fn check_forbidden_group_by_params(fields: &Fields) -> Result<(), Error> {
	match fields {
		Fields::Value(selector) => check_expr_for_forbidden_params(&selector.expr),
		Fields::Select(field_list) => {
			for field in field_list {
				match field {
					Field::All => {}
					Field::Single(selector) => {
						check_expr_for_forbidden_params(&selector.expr)?;
					}
				}
			}
			Ok(())
		}
	}
}

/// Recursively check an expression for `$this` or `$parent` parameters.
fn check_expr_for_forbidden_params(expr: &Expr) -> Result<(), Error> {
	match expr {
		Expr::Param(param) => {
			let name = param.as_str();
			if name == "this" || name == "self" {
				return Err(Error::Query {
					message: "Found a `$this` parameter refering to the document of a group by select statement\nSelect statements with a group by currently have no defined document to refer to".to_string(),
				});
			}
			if name == "parent" {
				return Err(Error::Query {
					message: "Found a `$parent` parameter refering to the document of a GROUP select statement\nSelect statements with a GROUP BY or GROUP ALL currently have no defined document to refer to".to_string(),
				});
			}
			Ok(())
		}
		Expr::Binary {
			left,
			right,
			..
		} => {
			check_expr_for_forbidden_params(left)?;
			check_expr_for_forbidden_params(right)
		}
		Expr::Prefix {
			expr,
			..
		} => check_expr_for_forbidden_params(expr),
		Expr::Postfix {
			expr,
			..
		} => check_expr_for_forbidden_params(expr),
		Expr::FunctionCall(fc) => {
			for arg in &fc.arguments {
				check_expr_for_forbidden_params(arg)?;
			}
			Ok(())
		}
		Expr::Literal(Literal::Array(elements)) => {
			for elem in elements {
				check_expr_for_forbidden_params(elem)?;
			}
			Ok(())
		}
		Expr::Literal(Literal::Object(entries)) => {
			for entry in entries {
				check_expr_for_forbidden_params(&entry.value)?;
			}
			Ok(())
		}
		Expr::Select(select) => {
			// Check fields in subqueries
			match &select.fields {
				Fields::Value(selector) => check_expr_for_forbidden_params(&selector.expr),
				Fields::Select(field_list) => {
					for field in field_list {
						if let Field::Single(selector) = field {
							check_expr_for_forbidden_params(&selector.expr)?;
						}
					}
					Ok(())
				}
			}
		}
		Expr::Block(block) => {
			for stmt in &block.0 {
				check_expr_for_forbidden_params(stmt)?;
			}
			Ok(())
		}
		Expr::IfElse(ifelse) => {
			for (cond, body) in &ifelse.exprs {
				check_expr_for_forbidden_params(cond)?;
				check_expr_for_forbidden_params(body)?;
			}
			if let Some(close) = &ifelse.close {
				check_expr_for_forbidden_params(close)?;
			}
			Ok(())
		}
		Expr::Closure(closure) => check_expr_for_forbidden_params(&closure.body),
		// These don't contain nested expressions with params
		Expr::Literal(_)
		| Expr::Constant(_)
		| Expr::Table(_)
		| Expr::Idiom(_)
		| Expr::Break
		| Expr::Continue => Ok(()),
		// Other expressions that might contain nested params
		_ => Ok(()),
	}
}

/// Plan the FROM sources - handles multiple targets with Union
///
/// The `version` parameter is an optional timestamp for time-travel queries (VERSION clause).
/// The `cond`, `order`, and `with` parameters are passed to Scan for index selection.
fn plan_select_sources(
	what: Vec<Expr>,
	version: Option<u64>,
	cond: Option<&Cond>,
	order: Option<&crate::expr::order::Ordering>,
	with: Option<&crate::expr::with::With>,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	if what.is_empty() {
		return Err(Error::Unimplemented("SELECT requires at least one source".to_string()));
	}

	// Convert each source to a plan
	let mut source_plans = Vec::with_capacity(what.len());
	for expr in what {
		let plan = plan_single_source(expr, version, cond, order, with, ctx)?;
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
///
/// The `version` parameter is an optional timestamp for time-travel queries (VERSION clause).
/// The `cond`, `order`, and `with` parameters are passed to Scan for index selection.
/// Scan handles KV store sources: table names, record IDs (point or range).
/// SourceExpr handles value sources: arrays, scalars, computed expressions.
fn plan_single_source(
	expr: Expr,
	version: Option<u64>,
	cond: Option<&Cond>,
	order: Option<&crate::expr::order::Ordering>,
	with: Option<&crate::expr::with::With>,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	use crate::val::Value;

	match expr {
		// Table name: SELECT * FROM users
		Expr::Table(table_name) => {
			// Convert table name to a literal string for the physical expression
			let table_expr = expr_to_physical_expr(
				Expr::Literal(crate::expr::literal::Literal::String(
					table_name.as_str().to_string(),
				)),
				ctx,
			)?;
			Ok(Arc::new(Scan {
				source: table_expr,
				version,
				cond: cond.cloned(),
				order: order.cloned(),
				with: with.cloned(),
			}) as Arc<dyn ExecOperator>)
		}

		// Record ID literal: SELECT * FROM users:123
		// Scan handles record IDs internally via ScanTarget::RecordId
		// No index selection needed for point lookups
		Expr::Literal(crate::expr::literal::Literal::RecordId(record_id_lit)) => {
			// Convert the record ID literal to an expression that Scan can evaluate
			// Scan will handle point lookups and range scans internally
			let table_expr = expr_to_physical_expr(
				Expr::Literal(crate::expr::literal::Literal::RecordId(record_id_lit)),
				ctx,
			)?;
			Ok(Arc::new(Scan {
				source: table_expr,
				version,
				cond: None, // No index selection for record IDs
				order: None,
				with: None,
			}) as Arc<dyn ExecOperator>)
		}

		// Subquery: SELECT * FROM (SELECT * FROM table)
		Expr::Select(inner_select) => {
			// Recursively plan the inner SELECT
			plan_select(*inner_select, ctx)
		}

		// Array literal: SELECT * FROM [1, 2, 3]
		Expr::Literal(crate::expr::literal::Literal::Array(_)) => {
			// Convert to SourceExpr which will unnest the array elements
			let phys_expr = expr_to_physical_expr(expr, ctx)?;
			Ok(Arc::new(SourceExpr {
				expr: phys_expr,
			}) as Arc<dyn ExecOperator>)
		}

		// Parameter: SELECT * FROM $param
		// Inspect the parameter value to determine if it's a KV source or value source
		Expr::Param(param) => {
			match ctx.value(param.as_str()) {
				Some(Value::Table(_)) => {
					// Table source → Scan with index selection
					let table_expr = expr_to_physical_expr(Expr::Param(param.clone()), ctx)?;
					Ok(Arc::new(Scan {
						source: table_expr,
						version,
						cond: cond.cloned(),
						order: order.cloned(),
						with: with.cloned(),
					}) as Arc<dyn ExecOperator>)
				}
				Some(Value::RecordId(_)) => {
					// Record ID source → Scan without index selection
					let table_expr = expr_to_physical_expr(Expr::Param(param.clone()), ctx)?;
					Ok(Arc::new(Scan {
						source: table_expr,
						version,
						cond: None,
						order: None,
						with: None,
					}) as Arc<dyn ExecOperator>)
				}
				Some(_) | None => {
					// Array, scalar, subquery, etc. → SourceExpr
					let phys_expr = expr_to_physical_expr(Expr::Param(param), ctx)?;
					Ok(Arc::new(SourceExpr {
						expr: phys_expr,
					}) as Arc<dyn ExecOperator>)
				}
			}
		}

		// Other expressions (strings, objects, etc.) → SourceExpr
		other => {
			let phys_expr = expr_to_physical_expr(other, ctx)?;
			Ok(Arc::new(SourceExpr {
				expr: phys_expr,
			}) as Arc<dyn ExecOperator>)
		}
	}
}

/// Convert a LET statement to an execution plan
fn convert_let_statement(
	let_stmt: crate::expr::statements::SetStatement,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	let crate::expr::statements::SetStatement {
		name,
		what,
		kind: _,
	} = let_stmt;

	// Determine if the expression is a query or scalar
	let value: Arc<dyn ExecOperator> = match what {
		// SELECT produces a stream that gets collected into an array
		Expr::Select(select) => plan_select(*select, ctx)?,

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
			let expr = expr_to_physical_expr(other, ctx)?;

			// Validate: LET expressions can't reference current row
			if expr.references_current_value() {
				return Err(Error::Unimplemented(
					"LET expression cannot reference current row context".to_string(),
				));
			}

			Arc::new(ExprPlan {
				expr,
			}) as Arc<dyn ExecOperator>
		}
	};

	Ok(Arc::new(LetPlan {
		name,
		value,
	}) as Arc<dyn ExecOperator>)
}

/// Plan ORDER BY clause by selecting the appropriate sort operator.
///
/// This function chooses the optimal sort operator based on query characteristics:
/// - `RandomShuffle`: for ORDER BY RAND()
/// - `ExternalSort`: when TEMPFILES is specified (disk-based sorting)
/// - `SortTopK`: when limit is small (heap-based top-k selection)
/// - `Sort`: default full in-memory sort with parallel sorting
fn plan_sort(
	input: Arc<dyn ExecOperator>,
	order: &crate::expr::order::Ordering,
	start: &Option<crate::expr::start::Start>,
	limit: &Option<crate::expr::limit::Limit>,
	#[allow(unused)] tempfiles: bool,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	use crate::expr::order::Ordering;

	match order {
		Ordering::Random => {
			// ORDER BY RAND() - use RandomShuffle operator
			// Try to get effective limit if both start and limit are literals
			let effective_limit = get_effective_limit_literal(start, limit);
			Ok(Arc::new(RandomShuffle {
				input,
				limit: effective_limit,
			}) as Arc<dyn ExecOperator>)
		}
		Ordering::Order(order_list) => {
			// Convert order list to OrderByField vec
			let order_by = convert_order_list(order_list, ctx)?;

			// Check if we should use ExternalSort (TEMPFILES specified)
			#[cfg(storage)]
			if tempfiles && let Some(temp_dir) = ctx.temporary_directory() {
				return Ok(Arc::new(ExternalSort {
					input,
					order_by,
					temp_dir: temp_dir.to_path_buf(),
				}) as Arc<dyn ExecOperator>);
			}

			// Check if we should use SortTopK (small limit)
			if let Some(effective_limit) = get_effective_limit_literal(start, limit)
				&& effective_limit <= *MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE as usize
			{
				return Ok(Arc::new(SortTopK {
					input,
					order_by,
					limit: effective_limit,
				}) as Arc<dyn ExecOperator>);
			}

			// Default: full in-memory sort with parallel sorting
			Ok(Arc::new(Sort {
				input,
				order_by,
			}) as Arc<dyn ExecOperator>)
		}
	}
}

/// Convert an OrderList to a Vec of OrderByField.
fn convert_order_list(
	order_list: &crate::expr::order::OrderList,
	ctx: &FrozenContext,
) -> Result<Vec<OrderByField>, Error> {
	let mut fields = Vec::with_capacity(order_list.len());
	for order_field in order_list.iter() {
		// Convert idiom to physical expression
		let expr: Arc<dyn crate::exec::PhysicalExpr> =
			convert_idiom_to_physical_expr(&order_field.value, ctx)?;

		let direction = if order_field.direction {
			SortDirection::Asc
		} else {
			SortDirection::Desc
		};

		fields.push(OrderByField {
			expr,
			direction,
			collate: order_field.collate,
			numeric: order_field.numeric,
		});
	}
	Ok(fields)
}

/// Try to get the effective limit (start + limit) if both are literals.
///
/// Returns None if either value is not a literal or cannot be evaluated at plan time.
fn get_effective_limit_literal(
	start: &Option<crate::expr::start::Start>,
	limit: &Option<crate::expr::limit::Limit>,
) -> Option<usize> {
	// Get limit value if it's a literal
	let limit_val = limit.as_ref().and_then(|l| match &l.0 {
		Expr::Literal(Literal::Integer(n)) if *n >= 0 => Some(*n as usize),
		Expr::Literal(Literal::Float(n)) if *n >= 0.0 => Some(*n as usize),
		_ => None,
	})?;

	// Get start value if it's a literal (default to 0)
	let start_val = start
		.as_ref()
		.map(|s| match &s.0 {
			Expr::Literal(Literal::Integer(n)) if *n >= 0 => Some(*n as usize),
			Expr::Literal(Literal::Float(n)) if *n >= 0.0 => Some(*n as usize),
			_ => None,
		})
		.unwrap_or(Some(0))?;

	Some(start_val + limit_val)
}

// ============================================================================
// Consolidated Expression Evaluation Support
// ============================================================================

use crate::exec::expression_registry::{ComputePoint, ExpressionRegistry, resolve_order_by_alias};
use crate::exec::operators::{Compute, Projection, SelectProject, SortByKey, SortKey};

/// Plan ORDER BY with consolidated expression evaluation.
///
/// This is the new approach that:
/// 1. Resolves ORDER BY aliases to SELECT expressions
/// 2. Tries to convert idioms to `FieldPath` for direct extraction
/// 3. Falls back to Compute operator for complex expressions
/// 4. Uses SortByKey with FieldPath for efficient nested field sorting
///
/// Returns a tuple of:
/// - The input operator wrapped with Compute (if needed) and Sort
/// - A list of synthetic field names that were computed only for sorting (for OMIT)
pub(crate) fn plan_sort_consolidated(
	input: Arc<dyn ExecOperator>,
	order: &crate::expr::order::Ordering,
	fields: &Fields,
	start: &Option<crate::expr::start::Start>,
	limit: &Option<crate::expr::limit::Limit>,
	#[allow(unused)] tempfiles: bool,
	ctx: &FrozenContext,
) -> Result<(Arc<dyn ExecOperator>, Vec<String>), Error> {
	use crate::expr::order::Ordering;
	use crate::expr::part::Part;

	match order {
		Ordering::Random => {
			// ORDER BY RAND() - use RandomShuffle operator (no expression eval needed)
			let effective_limit = get_effective_limit_literal(start, limit);
			Ok((
				Arc::new(RandomShuffle {
					input,
					limit: effective_limit,
				}) as Arc<dyn ExecOperator>,
				vec![],
			))
		}
		Ordering::Order(order_list) => {
			// Build expression registry to collect expressions that need computation
			let mut registry = ExpressionRegistry::new();
			let mut sort_keys = Vec::with_capacity(order_list.len());
			// Track fields computed only for sorting (not in SELECT) - need OMIT
			let mut sort_only_fields: Vec<String> = Vec::new();

			// Process each ORDER BY field
			for order_field in order_list.iter() {
				let idiom = &order_field.value;

				// Try to resolve as a SELECT alias first
				let field_path = if let Some((resolved_expr, alias)) =
					resolve_order_by_alias(idiom, fields)
				{
					// ORDER BY references a SELECT alias - use the underlying expression
					match &resolved_expr {
						Expr::Idiom(inner_idiom) => {
							// Check if this idiom has graph traversals (Lookups)
							// Graph traversals require evaluation - can't extract directly
							let has_lookups =
								inner_idiom.0.iter().any(|p| matches!(p, Part::Lookup(_)));

							if has_lookups {
								// Graph traversal - must compute before sorting
								let name = registry.register(
									&resolved_expr,
									ComputePoint::BeforeSort,
									Some(alias.clone()),
									ctx,
								)?;
								FieldPath::field(name)
							} else {
								// Simple field access - try to convert to FieldPath
								match FieldPath::try_from(inner_idiom) {
									Ok(path) => path,
									Err(_) => {
										// Complex idiom - register for computation
										let name = registry.register(
											&resolved_expr,
											ComputePoint::BeforeSort,
											Some(alias.clone()),
											ctx,
										)?;
										FieldPath::field(name)
									}
								}
							}
						}
						_ => {
							// Non-idiom expression (function call, etc.) - register for computation
							let name = registry.register(
								&resolved_expr,
								ComputePoint::BeforeSort,
								Some(alias.clone()),
								ctx,
							)?;
							FieldPath::field(name)
						}
					}
				} else {
					// Not an alias - try direct conversion to FieldPath
					match FieldPath::try_from(idiom) {
						Ok(path) => path,
						Err(_) => {
							// Complex idiom (graph traversal, etc.) - register for computation
							let expr = Expr::Idiom(idiom.clone());
							let name =
								registry.register(&expr, ComputePoint::BeforeSort, None, ctx)?;
							// Track that this field wasn't in SELECT - needs OMIT
							sort_only_fields.push(name.clone());
							FieldPath::field(name)
						}
					}
				};

				// Build SortKey with FieldPath
				let direction = if order_field.direction {
					SortDirection::Asc
				} else {
					SortDirection::Desc
				};

				let mut key = SortKey::new(field_path);
				key.direction = direction;
				key.collate = order_field.collate;
				key.numeric = order_field.numeric;
				sort_keys.push(key);
			}

			// Insert Compute operator if there are expressions to compute
			let computed = if registry.has_expressions_for_point(ComputePoint::BeforeSort) {
				let compute_fields = registry.get_expressions_for_point(ComputePoint::BeforeSort);
				Arc::new(Compute::new(input, compute_fields)) as Arc<dyn ExecOperator>
			} else {
				input
			};

			// Check if we should use SortTopK (small limit) - only if no complex expressions
			// For now, always use SortByKey for consolidated approach
			// TODO: Add SortTopKByKey for optimized top-k with pre-computed fields

			// Create SortByKey operator
			Ok((
				Arc::new(SortByKey::new(computed, sort_keys)) as Arc<dyn ExecOperator>,
				sort_only_fields,
			))
		}
	}
}

/// Plan SELECT projections with consolidated approach.
///
/// This version uses SelectProject when expressions have been pre-computed,
/// avoiding duplicate evaluation.
#[allow(dead_code)] // Used for future expansion
pub(crate) fn plan_projections_consolidated(
	input: Arc<dyn ExecOperator>,
	fields: &Fields,
	omit: &[Expr],
	computed_fields: &[(String, String)], // (internal_name, output_name) pairs
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	match fields {
		// SELECT VALUE - still needs expression evaluation (not consolidated yet)
		Fields::Value(selector) => {
			if !omit.is_empty() {
				return Err(Error::Unimplemented(
					"OMIT clause with SELECT VALUE not supported".to_string(),
				));
			}
			let expr = expr_to_physical_expr(selector.expr.clone(), ctx)?;
			Ok(Arc::new(ProjectValue {
				input,
				expr,
			}) as Arc<dyn ExecOperator>)
		}

		Fields::Select(field_list) => {
			// Check if this is just SELECT * (all fields, no specific fields)
			let is_select_all =
				field_list.len() == 1 && matches!(field_list.first(), Some(Field::All));

			if is_select_all {
				// SELECT * - use SelectProject to handle RecordId dereferencing
				// and apply OMIT if present
				let omit_names: Vec<String> = omit
					.iter()
					.filter_map(|e| {
						if let Expr::Idiom(idiom) = e {
							Some(idiom_to_field_name(idiom))
						} else {
							None
						}
					})
					.collect();
				let projections: Vec<Projection> = std::iter::once(Projection::All)
					.chain(omit_names.into_iter().map(Projection::Omit))
					.collect();
				return Ok(
					Arc::new(SelectProject::new(input, projections)) as Arc<dyn ExecOperator>
				);
			}

			// Build projections
			let mut projections = Vec::with_capacity(field_list.len());
			let has_wildcard = field_list.iter().any(|f| matches!(f, Field::All));

			if has_wildcard {
				projections.push(Projection::All);
			}

			for field in field_list {
				match field {
					Field::All => {
						// Already handled above
					}
					Field::Single(selector) => {
						// Determine output name
						let output_name = if let Some(alias) = &selector.alias {
							idiom_to_field_name(alias)
						} else {
							derive_field_name(&selector.expr)
						};

						// Check if this field was pre-computed
						let maybe_computed =
							computed_fields.iter().find(|(_, out)| out == &output_name);

						if let Some((internal_name, _)) = maybe_computed {
							if internal_name != &output_name {
								// Need to rename from internal to output
								projections.push(Projection::Rename {
									from: internal_name.clone(),
									to: output_name,
								});
							} else {
								// Names match - just include
								projections.push(Projection::Include(output_name));
							}
						} else {
							// Not pre-computed - include by output name
							// (for simple fields that don't need computation)
							projections.push(Projection::Include(output_name));
						}
					}
				}
			}

			// Apply OMIT
			if !omit.is_empty() {
				for e in omit {
					if let Expr::Idiom(idiom) = e {
						projections.push(Projection::Omit(idiom_to_field_name(idiom)));
					}
				}
			}

			Ok(Arc::new(SelectProject::new(input, projections)) as Arc<dyn ExecOperator>)
		}
	}
}

// ============================================================================
// Idiom Conversion Functions
// ============================================================================

use crate::exec::physical_expr::IdiomExpr;
use crate::exec::physical_part::{
	LookupDirection, PhysicalDestructurePart, PhysicalLookup, PhysicalPart, PhysicalRecurse,
	PhysicalRecurseInstruction,
};
use crate::expr::part::{DestructurePart, Part, RecurseInstruction};

/// Convert an idiom to a physical expression.
///
/// All idioms are converted to `IdiomExpr` which handles runtime type checking
/// (e.g., fetching records when accessing fields on RecordIds).
fn convert_idiom_to_physical_expr(
	idiom: &crate::expr::idiom::Idiom,
	ctx: &FrozenContext,
) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
	// Always convert all parts - runtime handles type-specific behavior
	let physical_parts = convert_parts_to_physical(&idiom.0, ctx)?;
	Ok(Arc::new(IdiomExpr::new(idiom.clone(), physical_parts)))
}

/// Convert idiom parts to physical parts.
fn convert_parts_to_physical(
	parts: &[Part],
	ctx: &FrozenContext,
) -> Result<Vec<PhysicalPart>, Error> {
	let mut physical_parts = Vec::with_capacity(parts.len());

	for part in parts {
		let physical_part = convert_single_part(part, ctx)?;
		physical_parts.push(physical_part);
	}

	Ok(physical_parts)
}

/// Convert a single Part to a PhysicalPart.
fn convert_single_part(part: &Part, ctx: &FrozenContext) -> Result<PhysicalPart, Error> {
	match part {
		Part::Field(name) => Ok(PhysicalPart::Field(name.clone())),

		Part::Value(expr) => {
			let phys_expr = expr_to_physical_expr(expr.clone(), ctx)?;
			Ok(PhysicalPart::Index(phys_expr))
		}

		Part::All => Ok(PhysicalPart::All),
		Part::Flatten => Ok(PhysicalPart::Flatten),
		Part::First => Ok(PhysicalPart::First),
		Part::Last => Ok(PhysicalPart::Last),
		Part::Optional => Ok(PhysicalPart::Optional),

		Part::Where(expr) => {
			let phys_expr = expr_to_physical_expr(expr.clone(), ctx)?;
			Ok(PhysicalPart::Where(phys_expr))
		}

		Part::Method(name, args) => {
			let mut phys_args = Vec::with_capacity(args.len());
			for arg in args {
				phys_args.push(expr_to_physical_expr(arg.clone(), ctx)?);
			}
			Ok(PhysicalPart::Method {
				name: name.clone(),
				args: phys_args,
			})
		}

		Part::Destructure(parts) => {
			let phys_parts = convert_destructure_parts(parts, ctx)?;
			Ok(PhysicalPart::Destructure(phys_parts))
		}

		Part::Start(_) => {
			// Start parts are handled at the idiom level, not as individual parts
			Err(Error::Unimplemented(
				"Start parts should be handled at the idiom level".to_string(),
			))
		}

		Part::Lookup(lookup) => {
			// Lookups need special handling - create a plan
			let plan = plan_lookup(lookup, ctx)?;
			let direction = match &lookup.kind {
				crate::expr::lookup::LookupKind::Graph(dir) => LookupDirection::from(dir),
				crate::expr::lookup::LookupKind::Reference => LookupDirection::Reference,
			};
			// Extract edge tables from the lookup subjects
			let edge_tables: Vec<_> = lookup
				.what
				.iter()
				.map(|s| match s {
					crate::expr::lookup::LookupSubject::Table {
						table,
						..
					} => table.clone(),
					crate::expr::lookup::LookupSubject::Range {
						table,
						..
					} => table.clone(),
				})
				.collect();
			Ok(PhysicalPart::Lookup(PhysicalLookup {
				direction,
				edge_tables,
				plan,
			}))
		}

		Part::Recurse(recurse, inner_path, instruction) => {
			let (min_depth, max_depth) = match recurse {
				crate::expr::part::Recurse::Fixed(n) => (*n, Some(*n)),
				crate::expr::part::Recurse::Range(min, max) => (min.unwrap_or(1), *max),
			};

			let path = if let Some(p) = inner_path {
				convert_parts_to_physical(&p.0, ctx)?
			} else {
				vec![]
			};

			let instr = convert_recurse_instruction(instruction, ctx)?;

			Ok(PhysicalPart::Recurse(PhysicalRecurse {
				min_depth,
				max_depth,
				path,
				instruction: instr,
				inclusive: matches!(
					instruction,
					Some(RecurseInstruction::Path {
						inclusive: true,
						..
					}) | Some(RecurseInstruction::Collect {
						inclusive: true,
						..
					}) | Some(RecurseInstruction::Shortest {
						inclusive: true,
						..
					})
				),
			}))
		}

		Part::Doc => {
			// Doc ($) refers to the document, which is the current value
			// This should be handled at the idiom level
			Ok(PhysicalPart::Field("id".to_string()))
		}

		Part::RepeatRecurse => {
			// RepeatRecurse (@) is handled within recursion context
			Err(Error::Unimplemented(
				"RepeatRecurse should be handled within recursion context".to_string(),
			))
		}
	}
}

/// Convert destructure parts to physical destructure parts.
fn convert_destructure_parts(
	parts: &[DestructurePart],
	ctx: &FrozenContext,
) -> Result<Vec<PhysicalDestructurePart>, Error> {
	let mut physical_parts = Vec::with_capacity(parts.len());

	for part in parts {
		let phys_part = match part {
			DestructurePart::All(field) => PhysicalDestructurePart::All(field.clone()),
			DestructurePart::Field(field) => PhysicalDestructurePart::Field(field.clone()),
			DestructurePart::Aliased(field, idiom) => {
				let path = convert_parts_to_physical(&idiom.0, ctx)?;
				PhysicalDestructurePart::Aliased {
					field: field.clone(),
					path,
				}
			}
			DestructurePart::Destructure(field, nested) => {
				let nested_parts = convert_destructure_parts(nested, ctx)?;
				PhysicalDestructurePart::Nested {
					field: field.clone(),
					parts: nested_parts,
				}
			}
		};
		physical_parts.push(phys_part);
	}

	Ok(physical_parts)
}

/// Convert a RecurseInstruction to a PhysicalRecurseInstruction.
fn convert_recurse_instruction(
	instruction: &Option<RecurseInstruction>,
	ctx: &FrozenContext,
) -> Result<PhysicalRecurseInstruction, Error> {
	match instruction {
		None => Ok(PhysicalRecurseInstruction::Default),
		Some(RecurseInstruction::Collect {
			..
		}) => Ok(PhysicalRecurseInstruction::Collect),
		Some(RecurseInstruction::Path {
			..
		}) => Ok(PhysicalRecurseInstruction::Path),
		Some(RecurseInstruction::Shortest {
			expects,
			..
		}) => {
			let target = expr_to_physical_expr(expects.clone(), ctx)?;
			Ok(PhysicalRecurseInstruction::Shortest {
				target,
			})
		}
	}
}

/// Special parameter name for passing the lookup source at execution time.
/// This parameter is bound by `evaluate_lookup_for_rid` before executing the plan.
pub(crate) const LOOKUP_SOURCE_PARAM: &str = "__lookup_source__";

/// Plan a Lookup operation, creating the operator tree.
///
/// This function creates a plan for graph edge or reference traversals.
/// For simple lookups (no subquery clauses), it returns target IDs directly.
/// For complex lookups (GROUP BY, explicit fields, etc.), it uses `plan_select_pipeline`
/// to apply the standard query operators (Filter, Split, Aggregate, Sort, Limit, Project).
fn plan_lookup(
	lookup: &crate::expr::lookup::Lookup,
	ctx: &FrozenContext,
) -> Result<Arc<dyn ExecOperator>, Error> {
	use crate::exec::operators::{GraphEdgeScan, GraphScanOutput, Limit, ReferenceScan};

	// The source expression reads from a special parameter that will be bound at execution time.
	// When evaluate_lookup_for_rid executes this plan, it creates an ExecutionContext with
	// __lookup_source__ set to the actual RecordId.
	let source_expr: Arc<dyn crate::exec::PhysicalExpr> =
		Arc::new(crate::exec::physical_expr::Param(LOOKUP_SOURCE_PARAM.into()));

	// Determine if we need the full pipeline with projection/aggregation
	// Use the pipeline when:
	// - There's an explicit SELECT clause (expr)
	// - There's GROUP BY (needs aggregation)
	// For simple lookups (just filter/sort/limit), we can skip projection
	let needs_full_pipeline = lookup.expr.is_some() || lookup.group.is_some();

	// Determine the output mode based on whether we need full edge records
	let needs_full_records = needs_full_pipeline || lookup.cond.is_some() || lookup.split.is_some();
	let output_mode = if needs_full_records {
		GraphScanOutput::FullEdge
	} else {
		GraphScanOutput::TargetId
	};

	// Create the base scan operator
	let base_scan: Arc<dyn ExecOperator> = match &lookup.kind {
		crate::expr::lookup::LookupKind::Graph(dir) => {
			// Convert lookup subjects to table names
			let edge_tables: Vec<_> = lookup
				.what
				.iter()
				.map(|s| match s {
					crate::expr::lookup::LookupSubject::Table {
						table,
						..
					} => table.clone(),
					crate::expr::lookup::LookupSubject::Range {
						table,
						..
					} => table.clone(),
				})
				.collect();

			Arc::new(GraphEdgeScan {
				source: source_expr,
				direction: LookupDirection::from(dir),
				edge_tables,
				output_mode,
			})
		}
		crate::expr::lookup::LookupKind::Reference => {
			// For references, we need the referencing table
			let (referencing_table, referencing_field) = lookup
				.what
				.first()
				.map(|s| match s {
					crate::expr::lookup::LookupSubject::Table {
						table,
						referencing_field,
					} => (table.clone(), referencing_field.clone()),
					crate::expr::lookup::LookupSubject::Range {
						table,
						referencing_field,
						..
					} => (table.clone(), referencing_field.clone()),
				})
				.unwrap_or_else(|| ("unknown".into(), None));

			Arc::new(ReferenceScan {
				source: source_expr,
				referencing_table,
				referencing_field,
			})
		}
	};

	if needs_full_pipeline {
		// Use the shared pipeline: Filter -> Split -> Aggregate -> Sort -> Limit -> Project
		// This enables full support for subquery clauses like GROUP BY
		let config = SelectPipelineConfig {
			cond: lookup.cond.clone(),
			split: lookup.split.clone(),
			group: lookup.group.clone(),
			order: lookup.order.clone(),
			limit: lookup.limit.clone(),
			start: lookup.start.clone(),
			omit: vec![],           // No OMIT for lookups
			is_value_source: false, // Lookups are not value sources
			tempfiles: false,       // No TEMPFILES for lookups
		};
		plan_select_pipeline(base_scan, lookup.expr.clone(), config, ctx)
	} else {
		// Simple lookup without projection - apply filter/split/sort/limit manually
		// This preserves the original behavior of returning target IDs or filtered edges

		// Apply filter if present
		let filtered: Arc<dyn ExecOperator> = if let Some(cond) = &lookup.cond {
			let predicate = expr_to_physical_expr(cond.0.clone(), ctx)?;
			Arc::new(Filter {
				input: base_scan,
				predicate,
			})
		} else {
			base_scan
		};

		// Apply split if SPLIT is present
		let split_op: Arc<dyn ExecOperator> = if let Some(splits) = &lookup.split {
			Arc::new(crate::exec::operators::Split {
				input: filtered,
				idioms: splits.0.iter().map(|s| s.0.clone()).collect(),
			})
		} else {
			filtered
		};

		// Apply sort if ORDER BY is present
		let sorted: Arc<dyn ExecOperator> =
			if let Some(crate::expr::order::Ordering::Order(order_list)) = &lookup.order {
				let order_by = convert_order_list(order_list, ctx)?;
				Arc::new(crate::exec::operators::Sort {
					input: split_op,
					order_by,
				})
			} else {
				split_op
			};

		// Apply limit if present
		let limited: Arc<dyn ExecOperator> = if lookup.limit.is_some() || lookup.start.is_some() {
			let limit_expr = lookup
				.limit
				.as_ref()
				.map(|l| expr_to_physical_expr(l.0.clone(), ctx))
				.transpose()?;
			let offset_expr = lookup
				.start
				.as_ref()
				.map(|s| expr_to_physical_expr(s.0.clone(), ctx))
				.transpose()?;
			Arc::new(Limit {
				input: sorted,
				limit: limit_expr,
				offset: offset_expr,
			})
		} else {
			sorted
		};

		Ok(limited)
	}
}

#[cfg(test)]
mod planner_tests {
	use super::*;
	use crate::ctx::Context;

	#[test]
	fn test_planner_creates_let_operator() {
		let expr = Expr::Let(Box::new(crate::expr::statements::SetStatement {
			name: "x".to_string(),
			what: Expr::Literal(crate::expr::literal::Literal::Integer(42)),
			kind: None,
		}));

		let ctx = Arc::new(Context::background());
		let plan = try_plan_expr(expr, &ctx).expect("Planning failed");

		assert_eq!(plan.name(), "Let");
		assert!(plan.mutates_context());
	}

	#[test]
	fn test_planner_creates_scalar_plan() {
		// Test a simple literal
		let expr = Expr::Literal(crate::expr::literal::Literal::Integer(42));

		let ctx = Arc::new(Context::background());
		let plan = try_plan_expr(expr, &ctx).expect("Planning failed");

		assert_eq!(plan.name(), "Expr");
		assert!(plan.is_scalar());
	}
}
