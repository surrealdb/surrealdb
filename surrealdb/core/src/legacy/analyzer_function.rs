//! The legacy evaluator's seam for an analyzer's `FUNCTION` clause.
//!
//! A full-text analyzer may pre-process each piece of text through a
//! user-defined function before tokenizing it. Running one needs the execution
//! environment, so [`crate::idx::ft::analyzer::AnalyzerFunction`] declares the
//! call and this is its one implementation: it holds the environment
//! (`FrozenContext`, `Options`), lowers the stored function reference into an
//! executable function, and evaluates it through the legacy compute path — so
//! the analyzer names none of that.
//!
//! It serves both executors: the legacy evaluator and the streaming executor
//! each build one for the indexing or query operation they are running, so an
//! analyzer's function behaves identically under either.

use anyhow::bail;
use reblessive::tree::Stk;
use surrealdb_strand::Strand;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::exe::FlowResultExt as _;
use crate::exec::Error as ExecError;
use crate::expr::convert::analyzer_function::function_from_storage;
use crate::idx::ft::analyzer::{AnalyzerFunction, BoxAnalyzerFut};
use crate::legacy::function_compute;
use crate::sql::analyzer_function::qualified_name;
use crate::val::Value;

/// The legacy evaluator's [`AnalyzerFunction`], holding the environment the
/// analyzer's function is evaluated against.
///
/// Borrowed rather than owned: every call site already has both in hand for
/// the duration of the indexing or query operation it is serving, so
/// constructing one costs two pointer copies and no allocation.
pub(crate) struct LegacyAnalyzerFunction<'a> {
	ctx: &'a FrozenContext,
	opt: &'a Options,
}

impl<'a> LegacyAnalyzerFunction<'a> {
	pub(crate) fn new(ctx: &'a FrozenContext, opt: &'a Options) -> Self {
		Self {
			ctx,
			opt,
		}
	}
}

impl AnalyzerFunction for LegacyAnalyzerFunction<'_> {
	/// The function is called with no cursor document: it transforms the text
	/// it is handed and has no record to read fields from. `function_compute`
	/// applies the session's capability check for the function before running
	/// it.
	fn call<'a>(&'a self, stk: &'a mut Stk, name: &'a str, input: Strand) -> BoxAnalyzerFut<'a> {
		Box::pin(async move {
			let val = function_compute(
				&function_from_storage(name),
				stk,
				self.ctx,
				self.opt,
				None,
				vec![Value::String(input)],
			)
			.await
			.catch_return()?;
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
