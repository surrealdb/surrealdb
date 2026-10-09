//! The engine's seam for an analyzer's `FUNCTION` clause.
//!
//! A full-text analyzer may pre-process each piece of text through a
//! user-defined function before tokenizing it. Running one needs the execution
//! environment, so [`crate::idx::ft::analyzer::AnalyzerFunction`] declares the
//! call and this is the engine's implementation of it: it holds an
//! [`ExecutionContext`], takes `fn::<name>($input)` from the compile cache
//! ([`crate::exec::vm::serve::expr`]) once per operation, and evaluates the
//! compiled call per piece of text with the text bound to `$input` — so the
//! analyzer names none of that.
//!
//! The call is compiled rather than interpreted, on the cache's miss, and is
//! taken once per indexing or query operation rather than once per piece of
//! text: an analyzer
//! runs its function over every field value it indexes, so the cost of
//! resolving the definition and its permission belongs to the operation and
//! not to the text.
//!
//! The stack the trait hands down is unused here: the compiled call grows none.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::bail;
use surrealdb_strand::Strand;

use crate::catalog::providers::DatabaseProvider as _;
use crate::exe::FlowResultExt as _;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{Error as ExecError, ExecutionContext};
use crate::expr::convert::analyzer_function::function_from_storage;
use crate::expr::{Expr, FunctionCall};
use crate::idx::ft::analyzer::{AnalyzerFunction, BoxAnalyzerFut};
use crate::sql::analyzer_function::qualified_name;
use crate::val::Value;

/// The parameter the text is bound to.
///
/// Bound as a local parameter of the call rather than in the context, so it is
/// visible to the argument and to nothing else — a function body naming this
/// reads its own caller's binding, not this one.
const INPUT: &str = "__surrealdb_analyzer_input";

/// The engine's [`AnalyzerFunction`], holding the frame the
/// analyzer's function is evaluated in.
///
/// Borrowed rather than owned: every call site already has the context in hand
/// for the duration of the indexing or query operation it is serving.
pub(crate) struct StreamingAnalyzerFunction<'a> {
	exec: &'a ExecutionContext,
	/// The compiled call, keyed by the name it calls. An analyzer names one
	/// function, so this holds one entry; keying it is what keeps that an
	/// observation rather than an assumption.
	///
	/// A `Mutex` rather than a `RefCell` so the whole is `Sync`: an index
	/// build runs on a spawned task, whose future must be `Send`, and holds
	/// one of these across the awaits that index a batch. The lock is never
	/// held across an await — a miss drops it, compiles, and takes it again —
	/// so a second compile of the same name is possible and writes the same
	/// expression.
	compiled: Mutex<HashMap<String, Arc<dyn PhysicalExpr>>>,
}

impl<'a> StreamingAnalyzerFunction<'a> {
	pub(crate) fn new(exec: &'a ExecutionContext) -> Self {
		Self {
			exec,
			compiled: Mutex::new(HashMap::new()),
		}
	}

	/// The compiled `fn::<name>($input)`, compiling it on first use.
	///
	/// Compiled against the transaction the operation already holds, so the
	/// definition and its permission resolve here rather than per piece of
	/// text, and under the caller's principal, so the function's own
	/// `PERMISSIONS` clause is judged for whoever is indexing or querying.
	async fn call_expr(&self, name: &str) -> anyhow::Result<Arc<dyn PhysicalExpr>> {
		if let Some(compiled) = self.locked().get(name) {
			return Ok(Arc::clone(compiled));
		}
		let receiver = function_from_storage(name);
		// A user-defined function the database does not define is reported as
		// the catalog names it, before anything is compiled for the call.
		if let crate::expr::Function::Custom(custom) = &receiver {
			let db_ctx = self.exec.database()?;
			let txn = self.exec.txn();
			crate::exec::vm::descriptor::object(txn.get_db_function(
				db_ctx.ns_ctx.ns.namespace_id,
				db_ctx.db.database_id,
				custom,
				self.exec.version_stamp(),
			))
			.await?;
		}
		let expr = Expr::FunctionCall(Box::new(FunctionCall {
			receiver,
			arguments: vec![Expr::Param(INPUT.to_owned().into())],
		}));
		let logical = crate::expr::logical::lower_expr(expr.into())?;
		// Served from the compile cache by the call as written
		// ([`crate::exec::vm::serve`]).
		let compiled = crate::exec::vm::serve::expr(
			self.exec,
			logical,
			Some(Arc::clone(&self.exec.root().auth)),
			0,
		)
		.await?;
		self.locked().insert(name.to_owned(), Arc::clone(&compiled));
		Ok(compiled)
	}

	/// The compiled-call map, recovered from a poisoned lock.
	///
	/// Nothing under the lock can panic — it is taken to read or write one
	/// entry and released — so poisoning reports a panic elsewhere on this
	/// task rather than a map left half-written.
	fn locked(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<dyn PhysicalExpr>>> {
		self.compiled.lock().unwrap_or_else(|e| e.into_inner())
	}
}

impl AnalyzerFunction for StreamingAnalyzerFunction<'_> {
	/// The function is called with no row: it transforms the text it is handed
	/// and has no record to read fields from. Compiling the call applies the
	/// session's capability check for the function, as the per-row engine's
	/// does.
	fn call<'a>(&'a self, name: &'a str, input: Strand) -> BoxAnalyzerFut<'a> {
		Box::pin(async move {
			let compiled = self.call_expr(name).await?;
			let mut params = HashMap::with_capacity(1);
			params.insert(Strand::new(INPUT), Value::String(input));
			let eval = EvalContext {
				local_params: Some(&params),
				..EvalContext::from_exec_ctx(self.exec)
			};
			let val = compiled.evaluate(eval).await.catch_return()?;
			if let Value::String(val) = val {
				Ok(val)
			} else {
				bail!(ExecError::InvalidFunction {
					name: qualified_name(name),
					message: "The function should return a string.".to_string(),
				});
			}
		})
	}
}
