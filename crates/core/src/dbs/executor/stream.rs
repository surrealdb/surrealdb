//! Stream executor for executing a sequence of statements and collecting the results.
//!
//! Hierarchy of contexts:
//! * RootContext - always available, no ns/db
//! * NamespaceContext - root + namespace + transaction
//! * DatabaseContext - namespace + database
//!
//! Statement examples:
//! * `USE NS ns`: RootContext -> NamespaceContext
//! * `USE NS ns DB db`: RootContext -> DatabaseContext
//! * `INFO FOR ROOT`: RootContext
//! * `INFO FOR NS`: NamespaceContext
//! * `INFO FOR DB`: DatabaseContext
//!
//! * `DEFINE USER user ON ROOT PASSWORD 'pass'`: RootContext
//! * `DEFINE USER user ON NS PASSWORD 'pass'`: NamespaceContext
//! * `DEFINE USER user ON DB PASSWORD 'pass'`: DatabaseContext
//!
//! * `DEFINE TABLE table`: DatabaseContext
//! * `SELECT * FROM table`: DatabaseContext

use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use surrealdb_types::{Array, Value};
use tokio_util::sync::CancellationToken;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider, NamespaceProvider};
use crate::catalog::{DatabaseDefinition, NamespaceDefinition};
use crate::dbs::response::QueryResult;
use crate::dbs::{QueryResultBuilder, QueryType};
use crate::err::Error;
use crate::exec::{
	ContextLevel, DatabaseContext, EvalContext, ExecutionContext, ExecutionPlan, LetValue,
	NamespaceContext, Parameters, PlannedStatement, RootContext, SessionCommand, ValueBatchStream,
};
use crate::expr::{ControlFlow, FlowResult};
use crate::iam::Auth;
use crate::kvs::{Datastore, LockType, Transaction, TransactionType};
use crate::rpc::DbResultError;
use crate::types::PublicValue;
use crate::val::convert_value_to_public_value;

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert a `crate::val::Value` to `Result<PublicValue, DbResultError>`.
fn to_public_result(value: crate::val::Value) -> Result<PublicValue, DbResultError> {
	convert_value_to_public_value(value).map_err(|e| DbResultError::InternalError(e.to_string()))
}

/// Convert any displayable error to `DbResultError::InternalError`.
fn to_db_error(e: impl std::fmt::Display) -> DbResultError {
	DbResultError::InternalError(e.to_string())
}

/// Evaluate a `LetValue` (scalar expression or query) to a `crate::val::Value`.
async fn evaluate_let_value(
	exec_ctx: &ExecutionContext,
	value: &LetValue,
) -> Result<crate::val::Value, anyhow::Error> {
	match value {
		LetValue::Scalar(expr) => {
			let eval_ctx = EvalContext::from_exec_ctx(exec_ctx);
			expr.evaluate(eval_ctx).await
		}
		LetValue::Query(plan) => {
			let output_stream = plan.execute(exec_ctx)?;
			let mut results = Vec::new();
			futures::pin_mut!(output_stream);
			while let Some(batch_result) = output_stream.next().await {
				match batch_result {
					Ok(batch) => {
						results.extend(batch.values);
					}
					Err(ctrl) => match ctrl {
						ControlFlow::Break | ControlFlow::Continue => continue,
						ControlFlow::Return(v) => {
							results.push(v);
							break;
						}
						ControlFlow::Err(e) => {
							return Err(e);
						}
					},
				}
			}
			Ok(crate::val::Value::Array(crate::val::Array(results)))
		}
	}
}

// ============================================================================
// Transaction Helper Functions
// ============================================================================

/// Error message for queries not executed due to a failed/cancelled transaction.
const TXN_FAILED_MSG: &str = "The query was not executed due to a failed transaction";
const TXN_CANCELLED_MSG: &str = "The query was not executed due to a cancelled transaction";

/// Skip remaining statements until we reach COMMIT or CANCEL, pushing not-executed results.
fn skip_to_transaction_end<'a>(
	statements: &mut impl Iterator<Item = &'a PlannedStatement>,
	outputs: &mut Vec<QueryResult>,
) {
	for stmt in statements.by_ref() {
		if matches!(
			stmt,
			PlannedStatement::SessionCommand(SessionCommand::Commit)
				| PlannedStatement::SessionCommand(SessionCommand::Cancel)
		) {
			break;
		}
		outputs.push(QueryResult {
			time: Duration::ZERO,
			result: Err(DbResultError::QueryNotExecuted(TXN_CANCELLED_MSG.to_string())),
			query_type: QueryType::Other,
		});
	}
}

/// Mark all results in the given slice as failed due to transaction cancellation.
fn mark_results_cancelled(outputs: &mut [QueryResult]) {
	for res in outputs {
		res.result = Err(DbResultError::QueryNotExecuted(TXN_CANCELLED_MSG.to_string()));
	}
}

/// Mark all results in the given slice as failed with a custom error.
fn mark_results_failed(outputs: &mut [QueryResult], error: &str) {
	for res in outputs {
		res.result = Err(DbResultError::QueryNotExecuted(error.to_string()));
	}
}

// ============================================================================
// Statement Executor
// ============================================================================

/// Result of executing a single statement.
enum StatementOutcome {
	/// Statement executed successfully, produced a result.
	Ok(Result<PublicValue, DbResultError>),
	/// Statement encountered an error that should cancel the transaction.
	TransactionError(anyhow::Error),
	/// Query returned a control flow (break/continue/return).
	ControlFlow(ControlFlow),
	/// BEGIN statement encountered (needs special handling).
	BeginTransaction,
	/// COMMIT statement encountered.
	CommitTransaction,
	/// CANCEL statement encountered.
	CancelTransaction,
}

/// Executes statements within a given execution context.
///
/// This struct provides a unified interface for executing all statement types,
/// reducing code duplication between the main execution loop and transaction blocks.
struct StatementExecutor<'a> {
	session: &'a mut SessionState,
	root_ctx: &'a RootContext,
	txn: Arc<Transaction>,
}

impl<'a> StatementExecutor<'a> {
	fn new(
		session: &'a mut SessionState,
		root_ctx: &'a RootContext,
		txn: Arc<Transaction>,
	) -> Self {
		Self {
			session,
			root_ctx,
			txn,
		}
	}

	/// Build the current execution context based on session state.
	fn exec_ctx(&self) -> Result<ExecutionContext, Error> {
		build_execution_context_with_txn(self.session, self.root_ctx, self.txn.clone())
	}

	/// Execute a single statement and return its outcome.
	async fn execute(&mut self, statement: &PlannedStatement) -> StatementOutcome {
		match statement {
			PlannedStatement::SessionCommand(cmd) => self.execute_session_command(cmd).await,
			PlannedStatement::Query(plan) => self.execute_query(plan.as_ref()).await,
			PlannedStatement::Let {
				name,
				value,
			} => self.execute_let(name, value).await,
			PlannedStatement::Scalar(expr) => self.execute_scalar(expr.as_ref()).await,
			PlannedStatement::Explain {
				format: _,
				statement,
			} => self.execute_explain(statement.as_ref()),
		}
	}

	/// Execute a session command (USE, BEGIN, COMMIT, CANCEL).
	async fn execute_session_command(&mut self, cmd: &SessionCommand) -> StatementOutcome {
		match cmd {
			SessionCommand::Begin => StatementOutcome::BeginTransaction,
			SessionCommand::Commit => StatementOutcome::CommitTransaction,
			SessionCommand::Cancel => StatementOutcome::CancelTransaction,
			SessionCommand::Use {
				ns,
				db,
			} => {
				let exec_ctx = match self.exec_ctx() {
					Ok(ctx) => ctx,
					Err(e) => return StatementOutcome::Ok(Err(to_db_error(e))),
				};

				match handle_use_command(ns, db, self.session, &exec_ctx).await {
					Ok(value) => StatementOutcome::Ok(to_public_result(value)),
					// USE command errors are returned as result errors
					Err(e) => StatementOutcome::Ok(Err(to_db_error(e))),
				}
			}
		}
	}

	/// Execute a query plan.
	async fn execute_query(&mut self, plan: &dyn ExecutionPlan) -> StatementOutcome {
		// Pre-flight validation: check context requirements
		// These are fatal errors that should propagate
		let required = max_required_context(plan);
		let available = self.session.current_level();

		if available < required {
			let err = match required {
				ContextLevel::Namespace => Error::NsEmpty,
				ContextLevel::Database => Error::DbEmpty,
				ContextLevel::Root => unreachable!(),
			};
			return StatementOutcome::TransactionError(err.into());
		}

		let exec_ctx = match self.exec_ctx() {
			Ok(ctx) => ctx,
			Err(e) => return StatementOutcome::TransactionError(e.into()),
		};

		let output_stream = match plan.execute(&exec_ctx) {
			Ok(stream) => stream,
			// Plan setup errors are returned as result errors
			Err(e) => return StatementOutcome::Ok(Err(to_db_error(e))),
		};

		match collect_query_result(output_stream).await {
			Ok(query_result) => StatementOutcome::Ok(query_result.result),
			Err(ctrl) => StatementOutcome::ControlFlow(ctrl),
		}
	}

	/// Execute a LET statement.
	async fn execute_let(&mut self, name: &str, value: &LetValue) -> StatementOutcome {
		let exec_ctx = match self.exec_ctx() {
			Ok(ctx) => ctx,
			Err(e) => return StatementOutcome::TransactionError(e.into()),
		};

		match evaluate_let_value(&exec_ctx, value).await {
			Ok(val) => {
				self.session.set_param(name.to_string(), val);
				StatementOutcome::Ok(to_public_result(crate::val::Value::None))
			}
			// Evaluation errors are returned as error results, not fatal errors
			Err(e) => StatementOutcome::Ok(Err(to_db_error(e))),
		}
	}

	/// Execute a scalar expression as a top-level statement.
	async fn execute_scalar(&mut self, expr: &dyn crate::exec::PhysicalExpr) -> StatementOutcome {
		let exec_ctx = match self.exec_ctx() {
			Ok(ctx) => ctx,
			Err(e) => return StatementOutcome::TransactionError(e.into()),
		};

		let eval_ctx = EvalContext::from_exec_ctx(&exec_ctx);
		match expr.evaluate(eval_ctx).await {
			Ok(value) => StatementOutcome::Ok(to_public_result(value)),
			// Evaluation errors are returned as error results, not fatal errors
			Err(e) => StatementOutcome::Ok(Err(to_db_error(e))),
		}
	}

	/// Execute an EXPLAIN statement (format plan as text).
	fn execute_explain(&self, statement: &PlannedStatement) -> StatementOutcome {
		let plan_text = crate::exec::explain::format_planned_statement(statement);
		StatementOutcome::Ok(Ok(PublicValue::String(plan_text)))
	}
}

/// Handle USE NS/DB command - extracted for reuse.
async fn handle_use_command(
	ns: &Option<Arc<dyn crate::exec::PhysicalExpr>>,
	db: &Option<Arc<dyn crate::exec::PhysicalExpr>>,
	session: &mut SessionState,
	exec_ctx: &ExecutionContext,
) -> Result<crate::val::Value, Error> {
	let txn = exec_ctx.txn();

	// Evaluate NS expression if provided
	if let Some(ns_expr) = ns {
		let eval_ctx = EvalContext::from_exec_ctx(exec_ctx);
		let ns_value = ns_expr
			.evaluate(eval_ctx)
			.await
			.map_err(|e| Error::Thrown(format!("Failed to evaluate NS: {}", e)))?;
		let ns_name = ns_value
			.coerce_to::<String>()
			.map_err(|e| Error::Thrown(format!("NS must be a string: {}", e)))?;
		let ns_def = txn
			.get_or_add_ns(None, &ns_name)
			.await
			.map_err(|e| Error::Thrown(format!("Failed to get namespace: {}", e)))?;

		session.ns = Some(ns_def);
		// Clear DB when NS changes
		session.db = None;
	}

	// Evaluate DB expression if provided
	if let Some(db_expr) = db {
		// DB requires NS to be set
		let ns_def = session.ns.as_ref().ok_or(Error::NsEmpty)?;

		// Build a namespace context for DB expression evaluation
		let ns_ctx = NamespaceContext {
			root: exec_ctx.root().clone(),
			ns: ns_def.clone(),
		};
		let ns_exec_ctx = ExecutionContext::Namespace(ns_ctx);
		let eval_ctx = EvalContext::from_exec_ctx(&ns_exec_ctx);

		let db_value = db_expr
			.evaluate(eval_ctx)
			.await
			.map_err(|e| Error::Thrown(format!("Failed to evaluate DB: {}", e)))?;
		let db_name = db_value
			.coerce_to::<String>()
			.map_err(|e| Error::Thrown(format!("DB must be a string: {}", e)))?;
		let db_def = txn
			.get_or_add_db(None, &ns_def.name, &db_name)
			.await
			.map_err(|e| Error::Thrown(format!("Failed to get database: {}", e)))?;
		session.db = Some(db_def);
	}

	Ok(crate::val::Value::None)
}

/// Session state tracked across statement execution.
///
/// This holds the current namespace and database selection, which can be
/// modified by USE statements, as well as parameters set by LET statements.
struct SessionState {
	/// The selected namespace (None = root level only)
	ns: Option<Arc<NamespaceDefinition>>,
	/// The selected database (None = namespace level only)
	db: Option<Arc<DatabaseDefinition>>,
	/// Parameters set by LET statements
	params: Parameters,
	/// Whether we are currently in an explicit transaction (BEGIN/COMMIT/CANCEL)
	in_transaction: bool,
}

impl SessionState {
	fn new(initial_params: Parameters) -> Self {
		Self {
			ns: None,
			db: None,
			params: initial_params,
			in_transaction: false,
		}
	}

	/// Initialize session state from existing ns/db definitions.
	fn with_ns_db(
		ns: Option<Arc<NamespaceDefinition>>,
		db: Option<Arc<DatabaseDefinition>>,
		initial_params: Parameters,
	) -> Self {
		Self {
			ns,
			db,
			params: initial_params,
			in_transaction: false,
		}
	}

	/// Get the current context level based on what's selected.
	fn current_level(&self) -> ContextLevel {
		match (&self.ns, &self.db) {
			(None, _) => ContextLevel::Root,
			(Some(_), None) => ContextLevel::Namespace,
			(Some(_), Some(_)) => ContextLevel::Database,
		}
	}

	/// Set a parameter value (from LET statements).
	fn set_param(&mut self, name: String, value: crate::val::Value) {
		self.params.insert(std::borrow::Cow::Owned(name), Arc::new(value));
	}
}

pub struct StreamExecutor {
	outputs: Vec<PlannedStatement>,
}

impl StreamExecutor {
	/// Creates a new stream executor.
	pub(crate) fn new(outputs: Vec<PlannedStatement>) -> Self {
		Self {
			outputs,
		}
	}

	/// Executes each output one at a time, in order and collects the results.
	///
	/// NOTE: This is not optimal, we should execute all outputs in parallel (as parallel as
	/// possible) and stream the results back rather than executing them sequentially and
	/// collecting the results.
	///
	/// # Parameters
	/// - `ds`: The datastore to execute against
	/// - `initial_ns`: Optional namespace name to initialize the session with
	/// - `initial_db`: Optional database name to initialize the session with
	/// - `auth`: The authentication context for this session
	/// - `auth_enabled`: Whether authentication is enabled on the datastore
	/// - `session_values`: Session-based parameters ($auth, $access, $token, $session)
	pub(crate) async fn execute_collected(
		self,
		ds: &Datastore,
		initial_ns: Option<&str>,
		initial_db: Option<&str>,
		auth: Arc<Auth>,
		auth_enabled: bool,
		session_values: Vec<(&'static str, crate::val::Value)>,
	) -> Result<Vec<QueryResult>, anyhow::Error> {
		let txn = Arc::new(ds.transaction(TransactionType::Read, LockType::Optimistic).await?);
		let mut outputs = Vec::with_capacity(self.outputs.len());

		// Initialize parameters with session values ($auth, $access, $token, $session)
		let mut initial_params = Parameters::new();
		for (name, value) in session_values {
			initial_params.insert(std::borrow::Cow::Borrowed(name), Arc::new(value));
		}
		let params = Arc::new(initial_params.clone());

		// Create root context (always available)
		// Note: datastore is None because we only have a borrowed reference.
		// Root-level operations that need datastore access will need to be handled differently.
		let root_ctx = RootContext {
			datastore: None,
			params: params.clone(),
			cancellation: CancellationToken::new(),
			auth,
			auth_enabled,
			txn: txn.clone(),
		};

		// Initialize session state from provided ns/db names
		let mut session = if let Some(ns_name) = initial_ns {
			// Look up namespace definition (already returns Arc<NamespaceDefinition>)
			let ns = txn
				.expect_ns_by_name(ns_name)
				.await
				.map_err(|e| anyhow::anyhow!("Failed to look up namespace '{}': {}", ns_name, e))?;

			// Optionally look up database definition (already returns Arc<DatabaseDefinition>)
			let db = if let Some(db_name) = initial_db {
				Some(txn.expect_db_by_name(ns_name, db_name).await.map_err(|e| {
					anyhow::anyhow!("Failed to look up database '{}': {}", db_name, e)
				})?)
			} else {
				None
			};

			SessionState::with_ns_db(Some(ns), db, initial_params)
		} else {
			SessionState::new(initial_params)
		};

		let mut stmt_iter = self.outputs.iter().peekable();
		while let Some(statement) = stmt_iter.next() {
			let query_result_builder = QueryResultBuilder::started_now();

			// Create executor for this statement (outside transaction)
			let mut executor = StatementExecutor::new(&mut session, &root_ctx, txn.clone());

			match executor.execute(statement).await {
				StatementOutcome::Ok(result) => {
					outputs.push(query_result_builder.finish_with_result(result));
				}
				StatementOutcome::TransactionError(e) => {
					// Outside transaction, errors propagate up
					return Err(e);
				}
				StatementOutcome::ControlFlow(ctrl) => match ctrl {
					ControlFlow::Break => break,
					ControlFlow::Continue => continue,
					ControlFlow::Return(value) => {
						outputs.push(QueryResult {
							time: Duration::ZERO,
							result: to_public_result(value),
							query_type: QueryType::Other,
						});
						return Ok(outputs);
					}
					ControlFlow::Err(e) => return Err(e),
				},
				StatementOutcome::BeginTransaction => {
					// BEGIN starts a transaction block - push result and delegate
					outputs.push(
						query_result_builder
							.finish_with_result(to_public_result(crate::val::Value::None)),
					);
					execute_transaction_block(
						ds,
						&mut session,
						&root_ctx,
						&mut stmt_iter,
						&mut outputs,
					)
					.await?;
				}
				StatementOutcome::CommitTransaction => {
					// COMMIT outside transaction is an error
					outputs.push(query_result_builder.finish_with_result(Err(to_db_error(
						"Invalid statement: Cannot COMMIT without starting a transaction",
					))));
				}
				StatementOutcome::CancelTransaction => {
					// CANCEL outside transaction is an error
					outputs.push(query_result_builder.finish_with_result(Err(to_db_error(
						"Invalid statement: Cannot CANCEL without starting a transaction",
					))));
				}
			}
		}
		Ok(outputs)
	}
}

/// Execute statements within a transaction block (from BEGIN to COMMIT/CANCEL).
async fn execute_transaction_block<'a>(
	ds: &Datastore,
	session: &mut SessionState,
	root_ctx: &RootContext,
	statements: &mut std::iter::Peekable<std::slice::Iter<'a, PlannedStatement>>,
	outputs: &mut Vec<QueryResult>,
) -> Result<(), anyhow::Error> {
	// Create a write transaction for the block
	let txn = Arc::new(ds.transaction(TransactionType::Write, LockType::Optimistic).await?);

	session.in_transaction = true;
	let start_results = outputs.len();

	// Process statements until we hit COMMIT or CANCEL
	while let Some(statement) = statements.next() {
		let query_result_builder = QueryResultBuilder::started_now();

		// Create executor with the write transaction
		let mut executor = StatementExecutor::new(session, root_ctx, txn.clone());

		match executor.execute(statement).await {
			StatementOutcome::Ok(result) => {
				let had_error = result.is_err();
				outputs.push(query_result_builder.finish_with_result(result));
				// Inside transaction, any error cancels the transaction
				if had_error {
					let _ = txn.cancel().await;
					session.in_transaction = false;
					// Mark results up to (but not including) the error result
					let error_idx = outputs.len() - 1;
					mark_results_cancelled(&mut outputs[start_results..error_idx]);
					skip_to_transaction_end(statements, outputs);
					return Ok(());
				}
			}
			StatementOutcome::TransactionError(e) => {
				// Fatal error in transaction - cancel, mark results, and skip to end
				let _ = txn.cancel().await;
				session.in_transaction = false;
				mark_results_cancelled(&mut outputs[start_results..]);
				outputs.push(query_result_builder.finish_with_result(Err(to_db_error(&e))));
				skip_to_transaction_end(statements, outputs);
				return Ok(());
			}
			StatementOutcome::ControlFlow(ctrl) => match ctrl {
				ControlFlow::Continue => continue,
				ControlFlow::Return(value) => {
					// RETURN within transaction - push result and continue
					outputs.push(query_result_builder.finish_with_result(to_public_result(value)));
				}
				ControlFlow::Break => {
					// BREAK not allowed in transaction context - treat as error
					let _ = txn.cancel().await;
					session.in_transaction = false;
					mark_results_cancelled(&mut outputs[start_results..]);
					outputs.push(query_result_builder.finish_with_result(Err(to_db_error(
						"BREAK not allowed in transaction context",
					))));
					skip_to_transaction_end(statements, outputs);
					return Ok(());
				}
				ControlFlow::Err(e) => {
					// Error in transaction - cancel, mark results, and skip to end
					let _ = txn.cancel().await;
					session.in_transaction = false;
					mark_results_cancelled(&mut outputs[start_results..]);
					outputs.push(query_result_builder.finish_with_result(Err(to_db_error(&e))));
					skip_to_transaction_end(statements, outputs);
					return Ok(());
				}
			},
			StatementOutcome::BeginTransaction => {
				// Nested BEGIN - error, cancel, and skip
				let _ = txn.cancel().await;
				session.in_transaction = false;
				mark_results_failed(&mut outputs[start_results..], TXN_FAILED_MSG);
				outputs.push(query_result_builder.finish_with_result(Err(to_db_error(
					"Cannot BEGIN a transaction within a transaction",
				))));
				skip_to_transaction_end(statements, outputs);
				return Ok(());
			}
			StatementOutcome::CommitTransaction => {
				// Commit the transaction
				if let Err(e) = txn.commit().await {
					// Failed to commit - mark all results as not executed
					mark_results_failed(
						&mut outputs[start_results..],
						&format!("Query not executed: {}", e),
					);
				}
				session.in_transaction = false;
				outputs.push(
					query_result_builder
						.finish_with_result(to_public_result(crate::val::Value::None)),
				);
				return Ok(());
			}
			StatementOutcome::CancelTransaction => {
				// Cancel the transaction
				let _ = txn.cancel().await;
				session.in_transaction = false;
				// Mark all results as cancelled (with QueryCancelled, not QueryNotExecuted)
				for res in &mut outputs[start_results..] {
					res.result = Err(DbResultError::QueryCancelled);
				}
				outputs.push(
					query_result_builder
						.finish_with_result(to_public_result(crate::val::Value::None)),
				);
				return Ok(());
			}
		}
	}

	// Ran out of statements without COMMIT or CANCEL - treat as implicit CANCEL
	let _ = txn.cancel().await;
	session.in_transaction = false;
	mark_results_failed(&mut outputs[start_results..], "Missing COMMIT statement");
	Ok(())
}

/// Build an ExecutionContext with a custom transaction.
///
/// This is used when executing statements within an explicit transaction block.
fn build_execution_context_with_txn(
	session: &SessionState,
	root_ctx: &RootContext,
	txn: Arc<Transaction>,
) -> Result<ExecutionContext, Error> {
	// Create a root context with the session's current parameters and the provided transaction
	let root_with_params = RootContext {
		datastore: root_ctx.datastore.clone(),
		params: Arc::new(session.params.clone()),
		cancellation: root_ctx.cancellation.clone(),
		auth: root_ctx.auth.clone(),
		auth_enabled: root_ctx.auth_enabled,
		txn,
	};

	match (&session.ns, &session.db) {
		(None, _) => {
			// Root level only
			Ok(ExecutionContext::Root(root_with_params))
		}
		(Some(ns), None) => {
			// Namespace level
			Ok(ExecutionContext::Namespace(NamespaceContext {
				root: root_with_params,
				ns: ns.clone(),
			}))
		}
		(Some(ns), Some(db)) => {
			// Database level
			Ok(ExecutionContext::Database(DatabaseContext {
				ns_ctx: NamespaceContext {
					root: root_with_params,
					ns: ns.clone(),
				},
				db: db.clone(),
			}))
		}
	}
}

/// Recursively find the maximum required context level in a plan tree.
fn max_required_context(plan: &dyn ExecutionPlan) -> ContextLevel {
	let mut max = plan.required_context();
	for child in plan.children() {
		max = max.max(max_required_context(child.as_ref()));
	}
	max
}

/// Validate that the current context level meets the plan's requirements.
#[allow(dead_code)]
fn validate_context_requirements(
	plan: &dyn ExecutionPlan,
	available: ContextLevel,
) -> Result<(), Error> {
	let required = max_required_context(plan);
	if available < required {
		return Err(match required {
			ContextLevel::Namespace => Error::NsEmpty,
			ContextLevel::Database => Error::DbEmpty,
			ContextLevel::Root => unreachable!(),
		});
	}
	Ok(())
}

async fn collect_query_result(mut stream: ValueBatchStream) -> FlowResult<QueryResult> {
	let mut values = Vec::new();
	while let Some(batch) = stream.try_next().await? {
		for value in batch.values {
			values.push(convert_value_to_public_value(value)?);
		}
	}

	// TODO: Fill in time and query type.
	Ok(QueryResult {
		time: Duration::ZERO,
		result: Ok(Value::Array(Array::from(values))),
		query_type: QueryType::Other,
	})
}
