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

use futures::TryStreamExt;
use surrealdb_types::{Array, Value};
use tokio_util::sync::CancellationToken;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider, NamespaceProvider};
use crate::catalog::{DatabaseDefinition, NamespaceDefinition};
use crate::dbs::response::QueryResult;
use crate::dbs::{QueryResultBuilder, QueryType};
use crate::err::Error;
use crate::exec::{
	ContextLevel, DatabaseContext, EvalContext, ExecutionContext, ExecutionPlan, NamespaceContext,
	Parameters, PlannedStatement, RootContext, SessionCommand, ValueBatchStream,
};
use crate::expr::{ControlFlow, FlowResult};
use crate::iam::Auth;
use crate::kvs::{Datastore, LockType, Transaction, TransactionType};
use crate::val::convert_value_to_public_value;

/// Session state tracked across statement execution.
///
/// This holds the current namespace and database selection, which can be
/// modified by USE statements.
struct SessionState {
	/// The selected namespace (None = root level only)
	ns: Option<Arc<NamespaceDefinition>>,
	/// The selected database (None = namespace level only)
	db: Option<Arc<DatabaseDefinition>>,
}

impl SessionState {
	fn new() -> Self {
		Self {
			ns: None,
			db: None,
		}
	}

	/// Initialize session state from existing ns/db definitions.
	fn with_ns_db(
		ns: Option<Arc<NamespaceDefinition>>,
		db: Option<Arc<DatabaseDefinition>>,
	) -> Self {
		Self {
			ns,
			db,
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
	pub(crate) async fn execute_collected(
		self,
		ds: &Datastore,
		initial_ns: Option<&str>,
		initial_db: Option<&str>,
		auth: Arc<Auth>,
		auth_enabled: bool,
	) -> Result<Vec<QueryResult>, anyhow::Error> {
		let txn = Arc::new(ds.transaction(TransactionType::Read, LockType::Optimistic).await?);
		let mut outputs = Vec::with_capacity(self.outputs.len());

		// Create empty parameters for now
		let params = Arc::new(Parameters::new());

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

			SessionState::with_ns_db(Some(ns), db)
		} else {
			SessionState::new()
		};

		for statement in self.outputs {
			let query_result_builder = QueryResultBuilder::started_now();

			match statement {
				PlannedStatement::SessionCommand(cmd) => {
					// Handle session commands (USE, BEGIN, COMMIT, CANCEL)
					// Build execution context for session command evaluation
					let exec_ctx = build_execution_context(&session, &root_ctx)?;
					let result = handle_session_command(cmd, &mut session, &exec_ctx).await;

					match result {
						Ok(value) => {
							outputs.push(query_result_builder.finish_with_result(Ok(value)));
						}
						Err(e) => {
							return Err(anyhow::anyhow!("Session command failed: {}", e));
						}
					}
				}
				PlannedStatement::Query(plan) => {
					// Pre-flight validation: check context requirements
					let required = max_required_context(plan.as_ref());
					let available = session.current_level();

					if available < required {
						let err = match required {
							ContextLevel::Namespace => Error::NsEmpty,
							ContextLevel::Database => Error::DbEmpty,
							ContextLevel::Root => unreachable!(),
						};
						return Err(anyhow::anyhow!("{}", err));
					}

					// Build the execution context based on current session state
					let exec_ctx = build_execution_context(&session, &root_ctx)?;

					// Execute the plan
					let output_stream = plan.execute(&exec_ctx)?;

					let query_result = match collect_query_result(output_stream).await {
						Ok(query_result) => query_result,
						Err(ctrl) => match ctrl {
							ControlFlow::Break => break,
							ControlFlow::Continue => continue,
							ControlFlow::Return(value) => {
								outputs.push(QueryResult {
									time: Duration::ZERO,
									result: Ok(convert_value_to_public_value(value)?),
									query_type: QueryType::Other,
								});
								return Ok(outputs);
							}
							ControlFlow::Err(e) => return Err(e),
						},
					};

					outputs.push(query_result);
				}
			}
		}
		Ok(outputs)
	}
}

/// Handle a session command (USE, BEGIN, COMMIT, CANCEL).
async fn handle_session_command(
	cmd: SessionCommand,
	session: &mut SessionState,
	exec_ctx: &ExecutionContext,
) -> Result<Value, Error> {
	// Extract txn from exec_ctx
	let txn = exec_ctx.txn();
	match cmd {
		SessionCommand::Use {
			ns,
			db,
		} => {
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

			Ok(Value::None)
		}
		SessionCommand::Begin => {
			Err(Error::InvalidStatement("BEGIN not yet supported in new executor".to_string()))
		}
		SessionCommand::Commit => {
			Err(Error::InvalidStatement("Cannot COMMIT without starting a transaction".to_string()))
		}
		SessionCommand::Cancel => {
			Err(Error::InvalidStatement("Cannot CANCEL without starting a transaction".to_string()))
		}
	}
}

/// Build an ExecutionContext from the current session state.
fn build_execution_context(
	session: &SessionState,
	root_ctx: &RootContext,
) -> Result<ExecutionContext, Error> {
	match (&session.ns, &session.db) {
		(None, _) => {
			// Root level only
			Ok(ExecutionContext::Root(root_ctx.clone()))
		}
		(Some(ns), None) => {
			// Namespace level
			Ok(ExecutionContext::Namespace(NamespaceContext {
				root: root_ctx.clone(),
				ns: ns.clone(),
			}))
		}
		(Some(ns), Some(db)) => {
			// Database level
			Ok(ExecutionContext::Database(DatabaseContext {
				ns_ctx: NamespaceContext {
					root: root_ctx.clone(),
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
