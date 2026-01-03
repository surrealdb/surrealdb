//! USE operator - switches namespace and/or database context.
//!
//! USE is a context-mutating operator that modifies the execution context
//! to include namespace and/or database definitions.

use std::sync::Arc;

use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{DatabaseDefinition, NamespaceDefinition};
use crate::err::Error;
use crate::exec::context::{ContextLevel, DatabaseContext, ExecutionContext, NamespaceContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{OperatorPlan, ValueBatchStream};

/// USE operator - switches namespace and/or database.
///
/// Implements `OperatorPlan` with `mutates_context() = true`.
/// The `output_context()` method evaluates the namespace/database expressions
/// and creates a new context with the resolved definitions.
#[derive(Debug)]
pub struct UsePlan {
	/// Namespace to switch to (optional)
	pub ns: Option<Arc<dyn PhysicalExpr>>,
	/// Database to switch to (optional)
	pub db: Option<Arc<dyn PhysicalExpr>>,
}

impl OperatorPlan for UsePlan {
	fn name(&self) -> &'static str {
		"Use"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = Vec::new();
		if self.ns.is_some() {
			attrs.push(("ns".to_string(), "dynamic".to_string()));
		}
		if self.db.is_some() {
			attrs.push(("db".to_string(), "dynamic".to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// USE can run at root level - it's how you get to namespace/database level
		ContextLevel::Root
	}

	fn execute(&self, _ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		// USE produces no data output - it only mutates context
		Ok(Box::pin(stream::empty()))
	}

	fn mutates_context(&self) -> bool {
		true
	}

	fn output_context(&self, input: &ExecutionContext) -> Result<ExecutionContext, Error> {
		// We need to evaluate expressions and look up definitions.
		// Since this may require async operations, we use futures::executor::block_on.
		// This is acceptable because output_context is called from the executor
		// which is already async.

		let txn = input.txn();
		let eval_ctx = EvalContext::from_exec_ctx(input);

		let mut result_ctx = input.clone();

		// Handle USE NS
		if let Some(ns_expr) = &self.ns {
			// Evaluate the namespace expression
			let ns_value = futures::executor::block_on(ns_expr.evaluate(eval_ctx.clone()))
				.map_err(|e| Error::Thrown(e.to_string()))?;

			let ns_name: String =
				ns_value.coerce_to::<String>().map_err(|e| Error::Thrown(e.to_string()))?;

			// Look up or create the namespace definition
			let ns_def: Arc<NamespaceDefinition> =
				futures::executor::block_on(txn.get_or_add_ns(None, &ns_name))
					.map_err(|e| Error::Thrown(e.to_string()))?;

			// Update context to namespace level
			result_ctx = ExecutionContext::Namespace(NamespaceContext {
				root: input.root().clone(),
				ns: ns_def,
			});
		}

		// Handle USE DB (requires namespace context)
		if let Some(db_expr) = &self.db {
			// Get the namespace name from the current context
			let ns_ctx = result_ctx.namespace().map_err(|_| {
				Error::Thrown("USE DB requires a namespace to be selected first".to_string())
			})?;
			let ns_name = ns_ctx.ns_name();

			// Evaluate the database expression
			let eval_ctx = EvalContext::from_exec_ctx(&result_ctx);
			let db_value = futures::executor::block_on(db_expr.evaluate(eval_ctx))
				.map_err(|e| Error::Thrown(e.to_string()))?;

			let db_name: String =
				db_value.coerce_to::<String>().map_err(|e| Error::Thrown(e.to_string()))?;

			// Look up or create the database definition
			let db_def: Arc<DatabaseDefinition> = futures::executor::block_on(
				txn.get_or_add_db_upwards(None, ns_name, &db_name, true),
			)
			.map_err(|e| Error::Thrown(e.to_string()))?;

			// Update context to database level
			result_ctx = ExecutionContext::Database(DatabaseContext {
				ns_ctx: ns_ctx.clone(),
				db: db_def,
			});
		}

		Ok(result_ctx)
	}
}

impl ToSql for UsePlan {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("USE");
		if self.ns.is_some() {
			f.push_str(" NS <expr>");
		}
		if self.db.is_some() {
			f.push_str(" DB <expr>");
		}
	}
}
