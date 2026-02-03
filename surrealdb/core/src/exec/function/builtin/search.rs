//! Search functions for the streaming executor.
//!
//! These functions provide full-text search capabilities including
//! analyzer operations and result fusion.

use std::pin::Pin;

use anyhow::Result;
use reblessive::tree::TreeStack;

use crate::catalog::providers::DatabaseProvider;
use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::physical_expr::EvalContext;
use crate::expr::Kind;
use crate::idx::ft::analyzer::Analyzer;
use crate::val::Value;

// =========================================================================
// search::analyze - Analyze text using a defined analyzer
// =========================================================================

/// Analyzes text using a specified analyzer.
///
/// Usage: `search::analyze('analyzer_name', 'text to analyze')`
///
/// Returns an array of tokens produced by the analyzer.
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchAnalyze;

impl ScalarFunction for SearchAnalyze {
	fn name(&self) -> &'static str {
		"search::analyze"
	}

	fn signature(&self) -> Signature {
		Signature::new()
			.arg("analyzer", Kind::String)
			.arg("value", Kind::String)
			.returns(Kind::Array(Box::new(Kind::Any), None))
	}

	fn is_pure(&self) -> bool {
		false // Depends on database state (analyzer definition)
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + 'a>> {
		Box::pin(async move {
			let mut args = args.into_iter();

			// Get analyzer name
			let az = match args.next() {
				Some(Value::String(s)) => s,
				Some(v) => {
					return Err(anyhow::anyhow!(
						"Function 'search::analyze' expects a string analyzer name, got: {}",
						v.kind_of()
					));
				}
				None => {
					return Err(anyhow::anyhow!(
						"Function 'search::analyze' expects two arguments: analyzer name and value"
					));
				}
			};

			// Get value to analyze
			let val = match args.next() {
				Some(Value::String(s)) => s,
				Some(v) => {
					return Err(anyhow::anyhow!(
						"Function 'search::analyze' expects a string value, got: {}",
						v.kind_of()
					));
				}
				None => {
					return Err(anyhow::anyhow!(
						"Function 'search::analyze' expects two arguments: analyzer name and value"
					));
				}
			};

			// Get the options - if not available, return NONE (matching original behavior)
			let opt = match ctx.exec_ctx.options() {
				Some(opt) => opt,
				None => return Ok(Value::None),
			};

			// Get database context - if not available, return NONE
			let db_ctx = match ctx.exec_ctx.database() {
				Ok(db_ctx) => db_ctx,
				Err(_) => return Ok(Value::None),
			};

			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;

			// Get the analyzer definition from the database
			let az_def = ctx
				.txn()
				.get_db_analyzer(ns_id, db_id, &az)
				.await
				.map_err(|e| anyhow::anyhow!("Analyzer '{}' not found: {}", az, e))?;

			// Create the analyzer
			let analyzer = Analyzer::new(ctx.exec_ctx.ctx().get_index_stores(), az_def)?;

			// Analyze the value using a TreeStack
			let frozen = ctx.exec_ctx.ctx();
			let mut stack = TreeStack::new();
			stack
				.enter(|stk| async move { analyzer.analyze(stk, frozen, opt, val).await })
				.finish()
				.await
		})
	}
}

// =========================================================================
// Registration
// =========================================================================

pub fn register(registry: &mut FunctionRegistry) {
	registry.register(SearchAnalyze);
}
