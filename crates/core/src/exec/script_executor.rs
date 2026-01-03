//! ScriptExecutor - parallel executor for script plans.
//!
//! The executor runs all statements concurrently, respecting the DAG ordering
//! defined by `context_source` and `wait_for` dependencies.

use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use tokio::task::JoinSet;

use crate::exec::completion_map::{CompletionError, CompletionMap};
use crate::exec::statement::{
	ScriptPlan, StatementContent, StatementId, StatementLetValue, StatementOutput,
};
use crate::exec::{EvalContext, ExecutionContext};
use crate::val::Value;

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

/// Executes script plans with parallel statement execution.
///
/// The executor spawns all statements concurrently and uses the DAG structure
/// to ensure correct ordering. Statements wait for their dependencies before
/// executing, and signal completion to unblock dependent statements.
pub struct ScriptExecutor {
	/// The initial execution context for the script
	initial_context: ExecutionContext,
}

impl ScriptExecutor {
	/// Create a new executor with the given initial context.
	pub fn new(initial_context: ExecutionContext) -> Self {
		Self {
			initial_context,
		}
	}

	/// Execute a script plan, returning results for all statements.
	///
	/// Statements are executed in parallel where possible, respecting the
	/// DAG ordering defined by `context_source` and `wait_for`.
	pub async fn execute(
		&self,
		script: ScriptPlan,
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
			let content = stmt.content.clone();
			let mutates_ctx = stmt.mutates_context();

			let completed = Arc::clone(&completed);
			let initial = self.initial_context.clone();

			join_set.spawn(async move {
				let result = execute_statement(
					stmt_id,
					context_source,
					wait_for,
					content,
					mutates_ctx,
					&completed,
					&initial,
				)
				.await;

				match result {
					Ok(output) => {
						completed.complete(stmt_id, output);
						(stmt_id, Ok(()))
					}
					Err(e) => {
						let error_msg = e.to_string();
						completed.fail(stmt_id, error_msg.clone());
						(stmt_id, Err(error_msg))
					}
				}
			});
		}

		// Wait for all statements to complete
		let mut errors = Vec::new();
		while let Some(result) = join_set.join_next().await {
			match result {
				Ok((stmt_id, Err(e))) => {
					errors.push((stmt_id, e));
				}
				Err(join_error) => {
					errors.push((StatementId(usize::MAX), join_error.to_string()));
				}
				Ok((_, Ok(()))) => {
					// Statement completed successfully
				}
			}
		}

		// Report errors if any
		if !errors.is_empty() {
			return Err(ScriptExecutionError::MultipleStatementErrors(errors));
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
	content: StatementContent,
	mutates_ctx: bool,
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

	// 3. Execute the statement content
	let (results, output_ctx) =
		execute_content(&content, &input_ctx, mutates_ctx).await?;

	let duration = start.elapsed();

	Ok(StatementOutput {
		context: output_ctx,
		results,
		duration,
	})
}

/// Execute statement content and return results and output context.
async fn execute_content(
	content: &StatementContent,
	ctx: &ExecutionContext,
	mutates_ctx: bool,
) -> Result<(Vec<Value>, ExecutionContext), anyhow::Error> {
	match content {
		StatementContent::Query(plan) => {
			let stream = plan
				.execute(ctx)
				.map_err(|e| anyhow::anyhow!("Query execution error: {}", e))?;

			let results = collect_stream(stream).await?;
			Ok((results, ctx.clone()))
		}

		StatementContent::Scalar(expr) => {
			let eval_ctx = EvalContext::from_exec_ctx(ctx);
			let value = expr
				.evaluate(eval_ctx)
				.await
				.map_err(|e| anyhow::anyhow!("Scalar evaluation error: {}", e))?;
			Ok((vec![value], ctx.clone()))
		}

		StatementContent::Let {
			name,
			value,
		} => {
			let computed_value = match value {
				StatementLetValue::Scalar(expr) => {
					let eval_ctx = EvalContext::from_exec_ctx(ctx);
					expr.evaluate(eval_ctx)
						.await
						.map_err(|e| anyhow::anyhow!("LET expression error: {}", e))?
				}
				StatementLetValue::Query(plan) => {
					let stream = plan
						.execute(ctx)
						.map_err(|e| anyhow::anyhow!("LET query error: {}", e))?;
					let results = collect_stream(stream).await?;
					Value::Array(crate::val::Array(results))
				}
			};

			// Create new context with the parameter
			let new_ctx = ctx.with_param(name.clone(), computed_value);
			Ok((vec![Value::None], new_ctx))
		}

		StatementContent::Use {
			ns: _ns,
			db: _db,
		} => {
			// USE NS/DB - this would need to resolve namespace/database definitions
			// For now, return an error as this requires transaction access
			// The actual implementation would look up the namespace/database from the transaction
			Err(anyhow::anyhow!(
				"USE statement execution not yet implemented in ScriptExecutor"
			))
		}

		StatementContent::Begin => {
			// BEGIN - transaction control
			// This would need to start a new transaction
			Err(anyhow::anyhow!(
				"BEGIN statement execution not yet implemented in ScriptExecutor"
			))
		}

		StatementContent::Commit => {
			// COMMIT - transaction control
			Err(anyhow::anyhow!(
				"COMMIT statement execution not yet implemented in ScriptExecutor"
			))
		}

		StatementContent::Cancel => {
			// CANCEL - transaction control
			Err(anyhow::anyhow!(
				"CANCEL statement execution not yet implemented in ScriptExecutor"
			))
		}
	}
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

#[cfg(test)]
mod tests {
	use super::*;

	// Note: These tests would require setting up proper execution contexts
	// with datastores and transactions. For now, we'll just test the basic
	// structure compiles.

	#[test]
	fn test_script_plan_creation() {
		let plan = ScriptPlan::new();
		assert!(plan.is_empty());
		assert_eq!(plan.len(), 0);
	}
}

