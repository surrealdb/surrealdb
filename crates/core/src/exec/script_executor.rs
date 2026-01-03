//! ScriptExecutor - parallel executor for script plans.
//!
//! The executor runs all statements concurrently, respecting the DAG ordering
//! defined by `context_source` and `wait_for` dependencies.
//!
//! Context-mutating operators (USE, LET, BEGIN, COMMIT, CANCEL) implement
//! `mutates_context() = true` and provide `output_context()` to compute
//! the modified context after execution.

use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use tokio::task::JoinSet;

use crate::exec::completion_map::{CompletionError, CompletionMap};
use crate::exec::statement::{ScriptPlan, StatementContent, StatementId, StatementOutput};
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
	let (results, output_ctx) = execute_content(&content, &input_ctx, mutates_ctx).await?;

	let duration = start.elapsed();

	Ok(StatementOutput {
		context: output_ctx,
		results,
		duration,
	})
}

/// Execute statement content and return results and output context.
///
/// For context-mutating operators (USE, LET, BEGIN, COMMIT, CANCEL),
/// this calls `output_context()` to get the modified context.
async fn execute_content(
	content: &StatementContent,
	ctx: &ExecutionContext,
	_mutates_ctx: bool,
) -> Result<(Vec<Value>, ExecutionContext), anyhow::Error> {
	match content {
		StatementContent::Query(plan) => {
			// Execute the operator
			let stream =
				plan.execute(ctx).map_err(|e| anyhow::anyhow!("Query execution error: {}", e))?;

			let results = collect_stream(stream).await?;

			// Compute output context if this operator mutates it
			let output_ctx = if plan.mutates_context() {
				plan.output_context(ctx)
					.map_err(|e| anyhow::anyhow!("Context mutation error: {}", e))?
			} else {
				ctx.clone()
			};

			Ok((results, output_ctx))
		}

		StatementContent::Scalar(expr) => {
			let eval_ctx = EvalContext::from_exec_ctx(ctx);
			let value = expr
				.evaluate(eval_ctx)
				.await
				.map_err(|e| anyhow::anyhow!("Scalar evaluation error: {}", e))?;
			// Scalar expressions don't mutate context
			Ok((vec![value], ctx.clone()))
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
	use std::sync::Arc;

	use super::*;
	use crate::exec::OperatorPlan;
	use crate::exec::operators::{BeginPlan, CancelPlan, CommitPlan, LetPlan, LetValue, UsePlan};
	use crate::exec::physical_expr::Literal;
	use crate::exec::statement::{StatementContent, StatementKind, StatementPlan};

	#[test]
	fn test_script_plan_creation() {
		let plan = ScriptPlan::new();
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
			value: LetValue::Scalar(Arc::new(Literal(crate::val::Value::Number(
				crate::val::Number::Int(42),
			)))),
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
			content: StatementContent::Query(use_plan),
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
			content: StatementContent::Query(begin_plan),
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
			value: LetValue::Scalar(Arc::new(Literal(crate::val::Value::Number(
				crate::val::Number::Int(1),
			)))),
		}) as Arc<dyn OperatorPlan>;

		let stmt0 = StatementPlan {
			id: StatementId(0),
			context_source: None, // Uses initial context
			wait_for: vec![],
			content: StatementContent::Query(use_plan),
			kind: StatementKind::ContextMutation,
		};

		let stmt1 = StatementPlan {
			id: StatementId(1),
			context_source: Some(StatementId(0)), // Gets context from USE
			wait_for: vec![],                     // No ordering dependency
			content: StatementContent::Query(let_plan),
			kind: StatementKind::ContextMutation,
		};

		let plan = ScriptPlan {
			statements: vec![stmt0, stmt1],
		};

		assert_eq!(plan.len(), 2);
		assert!(!plan.is_empty());
		assert!(plan.get(StatementId(0)).is_some());
		assert!(plan.get(StatementId(1)).is_some());
		assert!(plan.get(StatementId(2)).is_none());

		// Verify DAG structure
		let stmt0_ref = plan.get(StatementId(0)).unwrap();
		assert!(stmt0_ref.context_source.is_none());

		let stmt1_ref = plan.get(StatementId(1)).unwrap();
		assert_eq!(stmt1_ref.context_source, Some(StatementId(0)));
	}
}
