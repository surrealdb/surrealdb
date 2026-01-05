//! ScriptExecutor - parallel executor for script plans.
//!
//! The executor runs all statements concurrently, respecting the DAG ordering
//! defined by `context_source` and `wait_for` dependencies.
//!
//! Context-mutating operators (USE, LET, BEGIN, COMMIT, CANCEL) implement
//! `mutates_context() = true` and provide `output_context()` to compute
//! the modified context after execution.
//!
//! ## Block Execution Model
//!
//! The executor supports the unified block execution model:
//! - Top-level scripts (Collect mode)
//! - FOR loop bodies (Discard mode)
//! - IF/ELSE branches (Discard mode)
//! - Control flow signals (BREAK, CONTINUE, RETURN, THROW)

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::dbs::{QueryResult, QueryType};
use crate::err::Error;
use crate::exec::block::{
	BlockOutputMode, BlockPlan, BlockResult, ForPlan, IfPlan, LetValueSource, PlannedStatement,
	StatementOperation,
};
use crate::exec::completion_map::{CompletionError, CompletionMap};
use crate::exec::context::{DatabaseContext, NamespaceContext, Parameters, RootContext};
use crate::exec::planner::logical_plan_to_execution_plan;
use crate::exec::statement::{ExecutionPlan, StatementId, StatementOutput, StatementResult};
use crate::exec::{EvalContext, ExecutionContext, OperatorPlan};
use crate::expr::LogicalPlan;
use crate::iam::Auth;
use crate::kvs::{Datastore, LockType, TransactionType};
use crate::rpc::DbResultError;
use crate::sql::expression::convert_public_value_to_internal;
use crate::types::PublicVariables;
use crate::val::{Array, Value, convert_value_to_public_value};

/// Error during script execution
#[derive(Debug)]
pub enum ScriptExecutionError {
	/// Multiple statements failed
	MultipleStatementErrors(Vec<(StatementId, String)>),
	/// A task panicked
	TaskPanicked(String),
	/// Completion map error
	CompletionError(CompletionError),
	/// Internal error
	Internal(String),
}

impl std::fmt::Display for ScriptExecutionError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::MultipleStatementErrors(errors) => {
				write!(f, "Multiple statement errors: ")?;
				for (id, msg) in errors {
					write!(f, "[{}: {}] ", id, msg)?;
				}
				Ok(())
			}
			Self::TaskPanicked(msg) => write!(f, "Task panicked: {}", msg),
			Self::CompletionError(e) => write!(f, "Completion error: {}", e),
			Self::Internal(msg) => write!(f, "Internal error: {}", msg),
		}
	}
}

impl std::error::Error for ScriptExecutionError {}

impl From<CompletionError> for ScriptExecutionError {
	fn from(e: CompletionError) -> Self {
		Self::CompletionError(e)
	}
}

impl From<Error> for ScriptExecutionError {
	fn from(e: Error) -> Self {
		Self::Internal(e.to_string())
	}
}

/// Convert a StatementOutput to a QueryResult.
///
/// This handles the conversion from internal Value types to PublicValue.
/// Scalar expressions return their value directly; queries wrap results in an array.
/// Statement errors are converted to QueryResult with error result.
fn statement_output_to_query_result(output: StatementOutput) -> Result<QueryResult, anyhow::Error> {
	let result = match output.result {
		StatementResult::Err(msg) => {
			// Statement-level error - convert to QueryResult error
			Err(DbResultError::InternalError(msg))
		}
		StatementResult::Ok(results) => {
			// Convert Vec<Value> to PublicValue
			let public_value = if output.is_scalar && results.len() == 1 {
				// Scalar expressions return single value directly
				convert_value_to_public_value(results.into_iter().next().unwrap())?
			} else if results.len() == 1 && matches!(results[0], Value::None) {
				// Context-mutating operators (LET, USE, BEGIN, etc.) return NONE directly
				convert_value_to_public_value(results.into_iter().next().unwrap())?
			} else {
				// Queries wrap results in an array
				let converted: Result<Vec<_>, _> =
					results.into_iter().map(convert_value_to_public_value).collect();
				surrealdb_types::Value::Array(surrealdb_types::Array::from(converted?))
			};
			Ok(public_value)
		}
	};

	Ok(QueryResult {
		time: output.duration,
		result,
		query_type: QueryType::Other,
	})
}

/// Executes script plans with parallel statement execution.
///
/// The executor spawns all statements concurrently and uses the DAG structure
/// to ensure correct ordering. Statements wait for their dependencies before
/// executing, and signal completion to unblock dependent statements.
pub struct StreamExecutor {
	/// The initial execution context for the script
	initial_context: ExecutionContext,
}

impl StreamExecutor {
	/// Create a new executor with the given initial context.
	pub fn new(initial_context: ExecutionContext) -> Self {
		Self {
			initial_context,
		}
	}

	/// Execute a logical plan, returning results for all statements.
	///
	/// This is the main entry point that mirrors `StreamExecutor::execute_collected`.
	/// It handles:
	/// - Converting LogicalPlan to ScriptPlan
	/// - Building the initial ExecutionContext from session state
	/// - Executing all statements with parallel execution where possible
	/// - Converting StatementOutput to QueryResult
	///
	/// # Parameters
	/// - `ds`: The datastore to execute against
	/// - `plan`: The logical plan to execute
	/// - `initial_ns`: Optional namespace name to initialize the session with
	/// - `initial_db`: Optional database name to initialize the session with
	/// - `auth`: The authentication context for this session
	/// - `auth_enabled`: Whether authentication is enabled on the datastore
	/// - `session_values`: Session-based parameters ($auth, $access, $token, $session)
	/// - `query_vars`: Optional query-level variables (e.g., $_record_id, $_table from SDK)
	pub(crate) async fn execute_collected(
		ds: &Datastore,
		execution_plan: ExecutionPlan,
		initial_ns: Option<&str>,
		initial_db: Option<&str>,
		auth: Arc<Auth>,
		auth_enabled: bool,
		session_values: Vec<(&'static str, Value)>,
		query_vars: Option<PublicVariables>,
	) -> Result<Vec<QueryResult>, anyhow::Error> {
		// Create a read transaction for the initial context
		let txn = Arc::new(ds.transaction(TransactionType::Read, LockType::Optimistic).await?);

		// Initialize parameters with session values ($auth, $access, $token, $session)
		let mut initial_params = Parameters::new();
		for (name, value) in session_values {
			initial_params.insert(Cow::Borrowed(name), Arc::new(value));
		}

		// Add query-level variables (e.g., $_record_id, $_table from SDK CRUD operations)
		if let Some(vars) = query_vars {
			for (name, value) in vars {
				initial_params
					.insert(Cow::Owned(name), Arc::new(convert_public_value_to_internal(value)));
			}
		}

		let params = Arc::new(initial_params);

		// Build the initial ExecutionContext based on ns/db selection
		// Note: datastore is None because we only have a borrowed reference.
		// Root-level operations that need datastore access (e.g., BEGIN) will need
		// to be handled differently.
		let initial_context = if let Some(ns_name) = initial_ns {
			// Look up namespace definition
			let ns = txn
				.expect_ns_by_name(ns_name)
				.await
				.map_err(|e| anyhow::anyhow!("Failed to look up namespace '{}': {}", ns_name, e))?;

			if let Some(db_name) = initial_db {
				// Look up database definition
				let db = txn.expect_db_by_name(ns_name, db_name).await.map_err(|e| {
					anyhow::anyhow!("Failed to look up database '{}': {}", db_name, e)
				})?;

				// Database-level context
				ExecutionContext::Database(DatabaseContext {
					ns_ctx: NamespaceContext {
						root: RootContext {
							datastore: None,
							params: params.clone(),
							cancellation: CancellationToken::new(),
							auth,
							auth_enabled,
							txn: txn.clone(),
						},
						ns,
					},
					db,
				})
			} else {
				// Namespace-level context
				ExecutionContext::Namespace(NamespaceContext {
					root: RootContext {
						datastore: None,
						params: params.clone(),
						cancellation: CancellationToken::new(),
						auth,
						auth_enabled,
						txn: txn.clone(),
					},
					ns,
				})
			}
		} else {
			// Root-level context
			ExecutionContext::Root(RootContext {
				datastore: None,
				params,
				cancellation: CancellationToken::new(),
				auth,
				auth_enabled,
				txn: txn.clone(),
			})
		};

		// Create executor and run
		let executor = StreamExecutor::new(initial_context);
		let outputs = executor.execute(execution_plan).await?;

		// Convert StatementOutput to QueryResult
		let results = outputs
			.into_iter()
			.map(|output| statement_output_to_query_result(output))
			.collect::<Result<Vec<_>, _>>()?;

		Ok(results)
	}

	/// Execute a script plan, returning results for all statements.
	///
	/// Statements are executed in parallel where possible, respecting the
	/// DAG ordering defined by `context_source` and `wait_for`.
	///
	/// Statement-level errors are captured in `StatementOutput::result` rather than
	/// failing the entire execution. Only infrastructure errors (task panics,
	/// invalid statement IDs) cause execution to fail.
	pub async fn execute(
		&self,
		script: ExecutionPlan,
	) -> Result<Vec<StatementOutput>, ScriptExecutionError> {
		let n = script.statements.len();
		if n == 0 {
			return Ok(vec![]);
		}

		let completed = Arc::new(CompletionMap::new(n));
		let script = Arc::new(script);

		// Spawn all statements concurrently
		let mut join_set = JoinSet::new();

		for stmt in &script.statements {
			let stmt_id = stmt.id;
			let context_source = stmt.context_source;
			let wait_for = stmt.wait_for.clone();
			let plan = stmt.plan.clone();

			let completed = Arc::clone(&completed);
			let initial = self.initial_context.clone();

			join_set.spawn(async move {
				let result = execute_statement(
					stmt_id,
					context_source,
					wait_for,
					plan,
					&completed,
					&initial,
				)
				.await;

				match result {
					Ok(output) => {
						// Statement executed (may have error in result, but infrastructure
						// succeeded)
						completed.complete(stmt_id, output);
						Ok(stmt_id)
					}
					Err(e) => {
						// Infrastructure error (bad dependency, etc.) - create error output
						let error_output = StatementOutput {
							context: initial.clone(),
							result: StatementResult::Err(e.to_string()),
							duration: std::time::Duration::ZERO,
							is_scalar: false,
						};
						completed.complete(stmt_id, error_output);
						Ok(stmt_id)
					}
				}
			});
		}

		// Wait for all statements to complete
		let mut infrastructure_errors = Vec::new();
		while let Some(result) = join_set.join_next().await {
			match result {
				Ok(Ok(_stmt_id)) => {
					// Statement completed (result may be success or error, but it's captured)
				}
				Ok(Err(e)) => {
					// This shouldn't happen now since we handle errors above
					infrastructure_errors.push((StatementId(usize::MAX), e));
				}
				Err(join_error) => {
					// Task panic - this is a fatal infrastructure error
					infrastructure_errors
						.push((StatementId(usize::MAX), format!("Task panicked: {}", join_error)));
				}
			}
		}

		// Report infrastructure errors (task panics only)
		if !infrastructure_errors.is_empty() {
			return Err(ScriptExecutionError::MultipleStatementErrors(infrastructure_errors));
		}

		// Collect results in order
		let mut outputs = Vec::with_capacity(n);
		for i in 0..n {
			let output = completed.wait_for(StatementId(i)).await?;
			outputs.push(output);
		}

		Ok(outputs)
	}
}

/// Execute a single statement with dependency handling.
async fn execute_statement(
	id: StatementId,
	context_source: Option<StatementId>,
	wait_for: Vec<StatementId>,
	plan: Arc<dyn OperatorPlan>,
	completed: &CompletionMap,
	initial: &ExecutionContext,
) -> Result<StatementOutput, anyhow::Error> {
	let start = Instant::now();

	// 1. Wait for all ordering dependencies to complete
	for dep_id in &wait_for {
		completed.wait_for(*dep_id).await.map_err(|e| {
			anyhow::anyhow!("Statement {} failed waiting for {}: {}", id, dep_id, e)
		})?;
	}

	// 2. Resolve context from context_source (or use initial)
	let input_ctx = match context_source {
		None => initial.clone(),
		Some(source_id) => {
			let source_output = completed.wait_for(source_id).await.map_err(|e| {
				anyhow::anyhow!("Statement {} failed getting context from {}: {}", id, source_id, e)
			})?;
			source_output.context
		}
	};

	// 3. Execute the plan
	let (result, output_ctx, is_scalar) = execute_plan(plan.as_ref(), &input_ctx).await;

	let duration = start.elapsed();

	Ok(StatementOutput {
		context: output_ctx,
		result,
		duration,
		is_scalar,
	})
}

/// Execute an operator plan and return result, output context, and whether it's a scalar.
///
/// For context-mutating operators (USE, LET, BEGIN, COMMIT, CANCEL),
/// this calls `output_context()` to get the modified context.
/// Returns (result, output_context, is_scalar).
///
/// Statement-level errors are captured in the result rather than propagated up,
/// matching SurrealDB's per-statement error semantics.
async fn execute_plan(
	plan: &dyn OperatorPlan,
	ctx: &ExecutionContext,
) -> (StatementResult, ExecutionContext, bool) {
	let is_scalar = plan.is_scalar();

	// Execute the operator
	let stream = match plan.execute(ctx) {
		Ok(s) => s,
		Err(e) => return (StatementResult::Err(e.to_string()), ctx.clone(), is_scalar),
	};

	let results = match collect_stream(stream).await {
		Ok(r) => r,
		Err(e) => return (StatementResult::Err(e.to_string()), ctx.clone(), is_scalar),
	};

	// Compute output context if this operator mutates it
	let output_ctx = if plan.mutates_context() {
		match plan.output_context(ctx).await {
			Ok(c) => c,
			Err(e) => {
				// Return the error message directly to preserve the error type formatting
				return (StatementResult::Err(e.to_string()), ctx.clone(), is_scalar);
			}
		}
	} else {
		ctx.clone()
	};

	(StatementResult::Ok(results), output_ctx, is_scalar)
}

/// Collect all values from a stream into a Vec
async fn collect_stream(
	stream: crate::exec::ValueBatchStream,
) -> Result<Vec<Value>, anyhow::Error> {
	let mut results = Vec::new();
	futures::pin_mut!(stream);

	while let Some(batch_result) = stream.next().await {
		match batch_result {
			Ok(batch) => results.extend(batch.values),
			Err(ctrl) => {
				use crate::expr::ControlFlow;
				match ctrl {
					ControlFlow::Break | ControlFlow::Continue => continue,
					ControlFlow::Return(v) => {
						results.push(v);
						break;
					}
					ControlFlow::Err(e) => {
						return Err(e);
					}
				}
			}
		}
	}

	Ok(results)
}

// ============================================================================
// Block Execution Model
// ============================================================================

/// Execute a block plan with the given initial context.
///
/// The block executes statements sequentially, handling:
/// - Context propagation (LET, USE)
/// - Dependency ordering (wait_for)
/// - Control flow signals (BREAK, CONTINUE, RETURN, THROW)
/// - Output mode (Collect vs Discard)
///
/// Note: This function returns a boxed future to allow recursive calls
/// (FOR loops and IF statements can nest blocks).
pub fn execute_block(
	block: &BlockPlan,
	ctx: ExecutionContext,
) -> std::pin::Pin<
	Box<dyn std::future::Future<Output = Result<BlockResult, anyhow::Error>> + Send + '_>,
> {
	Box::pin(execute_block_inner(block, ctx))
}

async fn execute_block_inner(
	block: &BlockPlan,
	ctx: ExecutionContext,
) -> Result<BlockResult, anyhow::Error> {
	let mut current_ctx = ctx;
	let mut results: Vec<Value> = Vec::new();
	let mut last_value = Value::None;

	for stmt in &block.statements {
		// Execute the statement
		let (stmt_results, new_ctx, signal) = execute_block_statement(stmt, &current_ctx).await?;

		// Handle control flow signals
		if let Some(signal) = signal {
			return Ok(signal);
		}

		// Update context
		current_ctx = new_ctx;

		// Handle results based on output mode
		match block.output_mode {
			BlockOutputMode::Collect => {
				// Collect all results
				if stmt_results.len() == 1 {
					results.push(stmt_results.into_iter().next().unwrap());
				} else if !stmt_results.is_empty() {
					results.push(Value::Array(Array(stmt_results)));
				} else {
					results.push(Value::None);
				}
			}
			BlockOutputMode::Discard => {
				// Only keep last value
				if let Some(v) = stmt_results.into_iter().last() {
					last_value = v;
				}
			}
		}
	}

	// Return results based on output mode
	match block.output_mode {
		BlockOutputMode::Collect => Ok(BlockResult::Completed(results)),
		BlockOutputMode::Discard => Ok(BlockResult::Completed(vec![last_value])),
	}
}

/// Execute a single block statement and return results, new context, and optional control signal.
async fn execute_block_statement(
	stmt: &PlannedStatement,
	ctx: &ExecutionContext,
) -> Result<(Vec<Value>, ExecutionContext, Option<BlockResult>), anyhow::Error> {
	match &stmt.operation {
		StatementOperation::Operator(plan) => {
			// Execute the operator
			let stream =
				plan.execute(ctx).map_err(|e| anyhow::anyhow!("Query execution error: {}", e))?;
			let results = collect_stream(stream).await?;

			// Compute output context if this operator mutates it
			let output_ctx = if plan.mutates_context() {
				plan.output_context(ctx)
					.await
					.map_err(|e| anyhow::anyhow!("Context mutation error: {}", e))?
			} else {
				ctx.clone()
			};

			Ok((results, output_ctx, None))
		}

		StatementOperation::Let {
			name,
			value,
		} => {
			// Evaluate the value
			let computed_value = match value {
				LetValueSource::Scalar(expr) => {
					let eval_ctx = EvalContext::from_exec_ctx(ctx);
					expr.evaluate(eval_ctx).await.map_err(|e| anyhow::anyhow!("{}", e))?
				}
				LetValueSource::Query(plan) => {
					let stream = plan
						.execute(ctx)
						.map_err(|e| anyhow::anyhow!("LET query execution error: {}", e))?;
					let results = collect_stream(stream).await?;
					Value::Array(Array(results))
				}
			};

			// Add the parameter to the context
			let output_ctx = ctx.with_param(name.clone(), computed_value);
			Ok((vec![], output_ctx, None))
		}

		StatementOperation::Use {
			ns,
			db,
		} => {
			// Build a UsePlan and execute it for context mutation
			let use_plan = crate::exec::operators::UsePlan {
				ns: ns.clone(),
				db: db.clone(),
			};
			// Cast to trait to call output_context
			let op: &dyn crate::exec::OperatorPlan = &use_plan;
			let output_ctx =
				op.output_context(ctx).await.map_err(|e| anyhow::anyhow!("USE error: {}", e))?;
			Ok((vec![], output_ctx, None))
		}

		StatementOperation::For(for_plan) => {
			let (results, signal) = execute_for(for_plan, ctx).await?;
			Ok((results, ctx.clone(), signal))
		}

		StatementOperation::If(if_plan) => {
			let (results, signal) = execute_if(if_plan, ctx).await?;
			Ok((results, ctx.clone(), signal))
		}

		StatementOperation::Break => Ok((vec![], ctx.clone(), Some(BlockResult::Break))),

		StatementOperation::Continue => Ok((vec![], ctx.clone(), Some(BlockResult::Continue))),

		StatementOperation::Return(expr) => {
			let eval_ctx = EvalContext::from_exec_ctx(ctx);
			let value = expr.evaluate(eval_ctx).await.map_err(|e| anyhow::anyhow!("{}", e))?;
			Ok((vec![], ctx.clone(), Some(BlockResult::Return(value))))
		}

		StatementOperation::Throw(expr) => {
			let eval_ctx = EvalContext::from_exec_ctx(ctx);
			let value = expr.evaluate(eval_ctx).await.map_err(|e| anyhow::anyhow!("{}", e))?;
			Ok((vec![], ctx.clone(), Some(BlockResult::Throw(value))))
		}

		StatementOperation::Sleep(duration) => {
			tokio::time::sleep(*duration).await;
			Ok((vec![], ctx.clone(), None))
		}
	}
}

/// Execute a FOR loop.
async fn execute_for(
	for_plan: &ForPlan,
	ctx: &ExecutionContext,
) -> Result<(Vec<Value>, Option<BlockResult>), anyhow::Error> {
	// Evaluate the iterable
	let eval_ctx = EvalContext::from_exec_ctx(ctx);
	let iterable_value =
		for_plan.iterable.evaluate(eval_ctx).await.map_err(|e| anyhow::anyhow!("{}", e))?;

	// Convert to iterable
	let items: Vec<Value> = match iterable_value {
		Value::Array(arr) => arr.0,
		Value::Range(range) => {
			// Convert range to array of values
			range_to_values(&range)?
		}
		other => {
			// Single value iteration
			vec![other]
		}
	};

	let mut results = Vec::new();

	for item in items {
		// Create context with loop variable
		let loop_ctx = ctx.with_param(for_plan.variable.clone(), item);

		// Execute the body
		match execute_block(&for_plan.body, loop_ctx).await? {
			BlockResult::Completed(body_results) => {
				results.extend(body_results);
			}
			BlockResult::Break => {
				// BREAK exits the loop
				break;
			}
			BlockResult::Continue => {
				// CONTINUE skips to next iteration
				continue;
			}
			BlockResult::Return(v) => {
				// RETURN propagates up
				return Ok((vec![], Some(BlockResult::Return(v))));
			}
			BlockResult::Throw(v) => {
				// THROW propagates up
				return Ok((vec![], Some(BlockResult::Throw(v))));
			}
		}
	}

	Ok((results, None))
}

/// Execute an IF/ELSE statement.
async fn execute_if(
	if_plan: &IfPlan,
	ctx: &ExecutionContext,
) -> Result<(Vec<Value>, Option<BlockResult>), anyhow::Error> {
	// Evaluate branches in order
	for branch in &if_plan.branches {
		let eval_ctx = EvalContext::from_exec_ctx(ctx);
		let condition_value =
			branch.condition.evaluate(eval_ctx).await.map_err(|e| anyhow::anyhow!("{}", e))?;

		if condition_value.is_truthy() {
			// Execute this branch
			return match execute_block(&branch.body, ctx.clone()).await? {
				BlockResult::Completed(results) => {
					// Return last value from branch
					let value = results.into_iter().last().unwrap_or(Value::None);
					Ok((vec![value], None))
				}
				signal => Ok((vec![], Some(signal))),
			};
		}
	}

	// No branch matched - execute else branch if present
	if let Some(else_branch) = &if_plan.else_branch {
		return match execute_block(else_branch, ctx.clone()).await? {
			BlockResult::Completed(results) => {
				let value = results.into_iter().last().unwrap_or(Value::None);
				Ok((vec![value], None))
			}
			signal => Ok((vec![], Some(signal))),
		};
	}

	// No branch executed, return NONE
	Ok((vec![Value::None], None))
}

/// Convert a Range to a vector of values.
fn range_to_values(range: &crate::val::Range) -> Result<Vec<Value>, anyhow::Error> {
	use std::ops::Bound;

	// Extract numeric bounds
	let start = match &range.start {
		Bound::Included(v) => value_to_i64(v)?,
		Bound::Excluded(v) => value_to_i64(v)? + 1,
		Bound::Unbounded => return Err(anyhow::anyhow!("FOR loop range must have a start bound")),
	};

	let end = match &range.end {
		Bound::Included(v) => value_to_i64(v)? + 1,
		Bound::Excluded(v) => value_to_i64(v)?,
		Bound::Unbounded => return Err(anyhow::anyhow!("FOR loop range must have an end bound")),
	};

	// Check for reasonable range size
	let size = (end - start).max(0) as usize;
	if size > 1_000_000 {
		return Err(anyhow::anyhow!("FOR loop range too large: {} elements", size));
	}

	Ok((start..end).map(|i| Value::Number(crate::val::Number::Int(i))).collect())
}

/// Convert a Value to i64 for range bounds.
fn value_to_i64(v: &Value) -> Result<i64, anyhow::Error> {
	match v {
		Value::Number(n) => Ok(n.clone().to_int()),
		_ => Err(anyhow::anyhow!("Range bound must be a number, got {:?}", v)),
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::exec::OperatorPlan;
	use crate::exec::operators::{BeginPlan, CancelPlan, CommitPlan, ExprPlan, LetPlan, UsePlan};
	use crate::exec::physical_expr::Literal;
	use crate::exec::statement::{StatementKind, StatementPlan};

	#[test]
	fn test_script_plan_creation() {
		let plan = ExecutionPlan::new();
		assert!(plan.is_empty());
		assert_eq!(plan.len(), 0);
	}

	#[test]
	fn test_use_plan_mutates_context() {
		let use_plan = UsePlan {
			ns: Some(Arc::new(Literal(crate::val::Value::String("test".to_string())))),
			db: None,
		};
		assert!(use_plan.mutates_context());
		assert_eq!(use_plan.name(), "Use");
	}

	#[test]
	fn test_let_plan_mutates_context() {
		let let_plan = LetPlan {
			name: "x".to_string(),
			value: Arc::new(ExprPlan {
				expr: Arc::new(Literal(crate::val::Value::Number(crate::val::Number::Int(42)))),
			}) as Arc<dyn OperatorPlan>,
		};
		assert!(let_plan.mutates_context());
		assert_eq!(let_plan.name(), "Let");
	}

	#[test]
	fn test_begin_plan_mutates_context() {
		let begin_plan = BeginPlan;
		assert!(begin_plan.mutates_context());
		assert_eq!(begin_plan.name(), "Begin");
	}

	#[test]
	fn test_commit_plan_mutates_context() {
		let commit_plan = CommitPlan;
		assert!(commit_plan.mutates_context());
		assert_eq!(commit_plan.name(), "Commit");
	}

	#[test]
	fn test_cancel_plan_mutates_context() {
		let cancel_plan = CancelPlan;
		assert!(cancel_plan.mutates_context());
		assert_eq!(cancel_plan.name(), "Cancel");
	}

	#[test]
	fn test_statement_kind_full_barrier() {
		assert!(!StatementKind::ContextMutation.is_full_barrier());
		assert!(!StatementKind::PureRead.is_full_barrier());
		assert!(StatementKind::DataMutation.is_full_barrier());
		assert!(StatementKind::Transaction.is_full_barrier());
		assert!(StatementKind::Schema.is_full_barrier());
	}

	#[test]
	fn test_statement_kind_mutates_context() {
		assert!(StatementKind::ContextMutation.mutates_context());
		assert!(StatementKind::Transaction.mutates_context());
		assert!(!StatementKind::PureRead.mutates_context());
		assert!(!StatementKind::DataMutation.mutates_context());
		assert!(!StatementKind::Schema.mutates_context());
	}

	#[test]
	fn test_statement_plan_with_use_operator() {
		let use_plan = Arc::new(UsePlan {
			ns: Some(Arc::new(Literal(crate::val::Value::String("test".to_string())))),
			db: None,
		}) as Arc<dyn OperatorPlan>;

		let stmt = StatementPlan {
			id: StatementId(0),
			context_source: None,
			wait_for: vec![],
			plan: use_plan,
			kind: StatementKind::ContextMutation,
		};

		assert!(stmt.mutates_context());
		assert!(!stmt.is_full_barrier());
	}

	#[test]
	fn test_statement_plan_with_transaction_operator() {
		let begin_plan = Arc::new(BeginPlan) as Arc<dyn OperatorPlan>;

		let stmt = StatementPlan {
			id: StatementId(0),
			context_source: None,
			wait_for: vec![],
			plan: begin_plan,
			kind: StatementKind::Transaction,
		};

		assert!(stmt.mutates_context());
		assert!(stmt.is_full_barrier());
	}

	#[test]
	fn test_script_plan_dag_structure() {
		// Create a simple DAG: USE NS test; LET $x = 1; SELECT ...
		let use_plan = Arc::new(UsePlan {
			ns: Some(Arc::new(Literal(crate::val::Value::String("test".to_string())))),
			db: None,
		}) as Arc<dyn OperatorPlan>;

		let let_plan = Arc::new(LetPlan {
			name: "x".to_string(),
			value: Arc::new(ExprPlan {
				expr: Arc::new(Literal(crate::val::Value::Number(crate::val::Number::Int(1)))),
			}) as Arc<dyn OperatorPlan>,
		}) as Arc<dyn OperatorPlan>;

		let stmt0 = StatementPlan {
			id: StatementId(0),
			context_source: None, // Uses initial context
			wait_for: vec![],
			plan: use_plan,
			kind: StatementKind::ContextMutation,
		};

		let stmt1 = StatementPlan {
			id: StatementId(1),
			context_source: Some(StatementId(0)), // Gets context from USE
			wait_for: vec![],                     // No ordering dependency
			plan: let_plan,
			kind: StatementKind::ContextMutation,
		};

		let exec_plan = ExecutionPlan {
			statements: vec![stmt0, stmt1],
		};

		assert_eq!(exec_plan.len(), 2);
		assert!(!exec_plan.is_empty());
		assert!(exec_plan.get(StatementId(0)).is_some());
		assert!(exec_plan.get(StatementId(1)).is_some());
		assert!(exec_plan.get(StatementId(2)).is_none());

		// Verify DAG structure
		let stmt0_ref = exec_plan.get(StatementId(0)).unwrap();
		assert!(stmt0_ref.context_source.is_none());

		let stmt1_ref = exec_plan.get(StatementId(1)).unwrap();
		assert_eq!(stmt1_ref.context_source, Some(StatementId(0)));
	}
}
