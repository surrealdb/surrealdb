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
//! The call is compiled on a cache miss and resolved once per indexing or
//! query operation, not once per piece of text. An analyzer runs its function
//! over every field value it indexes, so the cost of resolving the definition
//! and its permission belongs to the operation, not to each piece of text.
//! The compiled call is kept in the operation's map and reused for every piece
//! of text the operation covers.
//!
//! The stack the trait hands down is unused here: the compiled call grows none.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::bail;
use surrealdb_strand::Strand;

use crate::catalog::providers::DatabaseProvider as _;
use crate::exe::FlowResultExt as _;
use crate::exec::{Clause, Error as ExecError, EvalContext, ExecutionContext};
use crate::expr::analyzer_function::{function_from_storage, qualified_name};
use crate::expr::{Expr, FunctionCall};
use crate::idx::ft::analyzer::{AnalyzerFunction, BoxAnalyzerFut};
use crate::val::Value;

/// The parameter the text is bound to.
///
/// Bound as a local parameter of the call rather than in the context, so it is
/// visible to the argument and to nothing else — a function body naming this
/// finds nothing under it, as a body reads no binding of its caller.
const INPUT: &str = "__surrealdb_analyzer_input";

/// The engine's [`AnalyzerFunction`], holding the frame the
/// analyzer's function is evaluated in.
///
/// Borrowed rather than owned: each caller keeps the context alive for the
/// whole indexing or query operation it serves.
pub(crate) struct StreamingAnalyzerFunction<'a> {
	exec: &'a ExecutionContext,
	/// The compiled call, keyed by the function name it calls. An analyzer
	/// usually names one function, so the map holds one entry. The key keeps
	/// a second name from being served the first name's compiled call.
	///
	/// A `Mutex` rather than a `RefCell`, so the struct is `Sync`: an index
	/// build runs on a spawned task whose future must be `Send`, and holds
	/// this across the awaits that index a batch. The lock is never held
	/// across an await. On a miss it is dropped, the call compiles, and the
	/// lock is taken again, so two tasks may compile the same name and both
	/// write the same expression.
	compiled: Mutex<HashMap<String, Clause>>,
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
	async fn call_expr(&self, name: &str) -> anyhow::Result<Clause> {
		if let Some(compiled) = self.locked().get(name) {
			return Ok(compiled.clone());
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
		let logical = crate::expr::logical::lower_expr(expr)?;
		// Served from the compile cache by the call as written
		// ([`crate::exec::vm::serve`]).
		let compiled = crate::exec::vm::serve::expr(
			self.exec,
			logical,
			Some(Arc::clone(&self.exec.root().auth)),
			0,
		)
		.await?;
		self.locked().insert(name.to_owned(), compiled.clone());
		Ok(compiled)
	}

	/// The compiled-call map, recovered from a poisoned lock.
	///
	/// Nothing under the lock can panic — it is taken to read or write one
	/// entry and released — so poisoning reports a panic elsewhere on this
	/// task rather than a map left half-written.
	fn locked(&self) -> std::sync::MutexGuard<'_, HashMap<String, Clause>> {
		self.compiled.lock().unwrap_or_else(|e| e.into_inner())
	}
}

impl AnalyzerFunction for StreamingAnalyzerFunction<'_> {
	/// The function is called with no row: it transforms the text it is handed
	/// and has no record to read fields from. Compiling the call applies the
	/// session's capability check for the function. The check runs at compile
	/// time, so it is not repeated for each piece of text.
	fn call<'a>(&'a self, name: &'a str, input: Strand) -> BoxAnalyzerFut<'a> {
		Box::pin(async move {
			let compiled = self.call_expr(name).await?;
			let mut params = HashMap::with_capacity(1);
			params.insert(Strand::new(INPUT), Value::String(input));
			let mut eval = EvalContext::from_exec_ctx(self.exec);
			eval.local_params = Some(&params);
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
