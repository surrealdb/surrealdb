//! A stored expression evaluated as a VM unit.
//!
//! An expression the engine evaluates per row outside a statement's own
//! program, such as an index column, a `COUNT` guard, a permission guard or a
//! computed field with no read unit to serve it, runs as the body of a closure
//! that takes no argument ([`ScalarUnit`]). The closure machinery lowers it for
//! the invoking frame and `VERSION` stamp. The compile cache keys it by the
//! expression as written ([`crate::exec::vm::closure`]).
//!
//! The unit reads the row it is evaluated over as its document, the
//! parameters the expression names from where it is evaluated — a block's
//! local, a write's binding (`$before`, `$after`, `$value`, `$input`,
//! `$event`) or a value of the context — and the session's own values
//! (`$auth`, `$access`, `$token`, `$session`) from its frame when it runs.

use std::sync::Arc;

use surrealdb_cnf::PROTECTED_PARAM_NAMES;
use surrealdb_types::ToSql;

use crate::dbs::Variables;
use crate::exec::EvalContext;
use crate::exec::vm::ClosureDigest;
use crate::exec::vm::calls::CompiledClosure;
use crate::expr::logical::{LogicalClosure, LogicalExpr};
use crate::expr::{ControlFlow, FlowResult};
use crate::val::Value;

/// One stored expression, evaluated as the body of a closure that takes no
/// argument.
#[derive(Debug, Clone)]
pub(crate) struct ScalarUnit {
	closure: Arc<LogicalClosure>,
	digest: ClosureDigest,
	/// The names the expression reads as parameters that no binding within it
	/// binds, protected names aside.
	params: Vec<String>,
	/// Whether the expression is a permission predicate, a barrier for `BREAK`
	/// and `CONTINUE`: one leaving it is an error, and never the predicate's
	/// verdict.
	predicate: bool,
	/// Whether the expression reads the row it is evaluated over.
	reads_row: bool,
}

impl ScalarUnit {
	/// The unit of `expr`.
	pub(crate) fn new(expr: LogicalExpr) -> Self {
		let body_sql: Arc<str> = Arc::from(expr.to_sql());
		let closure = LogicalClosure {
			args: Vec::new(),
			returns: None,
			body: expr,
			body_sql,
		};
		let params = crate::exec::ir::build::logical::free_params(&closure)
			.into_iter()
			.filter(|name| !PROTECTED_PARAM_NAMES.contains(&name.as_str()))
			.map(|name| name.as_str().to_owned())
			.collect();
		Self {
			digest: ClosureDigest::of(&closure),
			reads_row: crate::exec::ir::build::logical::closure_reads_row(&closure),
			closure: Arc::new(closure),
			params,
			predicate: false,
		}
	}

	/// The unit of permission predicate `expr`.
	pub(crate) fn predicate(expr: LogicalExpr) -> Self {
		Self {
			predicate: true,
			..Self::new(expr)
		}
	}

	/// The expression's value, evaluated in `ctx` over its document root, or
	/// over its current value when there is no document root. A `RETURN` that
	/// leaves the expression is its value, since the expression is a value
	/// scope. In a permission predicate, a `BREAK` or `CONTINUE` that leaves it
	/// is an invalid-control-flow error, because a predicate has no loop to
	/// target. Outside a predicate, it is the closure's own answer.
	pub(crate) async fn evaluate(&self, ctx: &EvalContext<'_>) -> FlowResult<Value> {
		let mut captures = Variables::new();
		for name in &self.params {
			let bound = ctx
				.local_params
				.and_then(|params| params.get(name.as_str()))
				.or_else(|| ctx.write_binding(name))
				.or_else(|| ctx.exec_ctx.value(name));
			if let Some(value) = bound {
				captures.insert(name.as_str(), value.clone());
			}
		}
		let value = CompiledClosure::planned(&self.closure, self.digest, captures)
			.without_call()
			.reading_row(self.reads_row);
		if !self.predicate {
			return Box::pin(value.invoke(ctx, Vec::new())).await;
		}
		predicate_outcome(Box::pin(value.evaluate_body(ctx, Vec::new())).await)
	}
}

/// The verdict of a permission predicate whose body ended in `ran`.
///
/// A `RETURN` is the predicate's value, as in any value scope. A `BREAK` or
/// `CONTINUE` has no loop to target in a predicate and is the
/// misplaced-control-flow error. An error passes through unchanged.
fn predicate_outcome(ran: FlowResult<Value>) -> FlowResult<Value> {
	match ran {
		Ok(value) | Err(ControlFlow::Return(value)) => Ok(value),
		Err(ControlFlow::Break | ControlFlow::Continue) => {
			Err(ControlFlow::Err(anyhow::Error::new(crate::exec::Error::InvalidControlFlow)))
		}
		Err(error) => Err(error),
	}
}

#[cfg(test)]
mod tests {
	use super::predicate_outcome;
	use crate::expr::ControlFlow;
	use crate::val::Value;

	#[test]
	fn a_predicate_block_returns_its_value_and_bars_loop_signals() {
		assert!(matches!(predicate_outcome(Ok(Value::Bool(true))), Ok(Value::Bool(true))));
		assert!(matches!(
			predicate_outcome(Err(ControlFlow::Return(Value::Bool(false)))),
			Ok(Value::Bool(false))
		));
		for signal in [ControlFlow::Break, ControlFlow::Continue] {
			let Err(ControlFlow::Err(error)) = predicate_outcome(Err(signal)) else {
				panic!("a loop signal must be an error");
			};
			assert!(matches!(
				error.downcast_ref::<crate::exec::Error>(),
				Some(crate::exec::Error::InvalidControlFlow)
			));
		}
		let Err(ControlFlow::Err(error)) =
			predicate_outcome(Err(ControlFlow::Err(anyhow::anyhow!("boom"))))
		else {
			panic!("an error must pass through");
		};
		assert_eq!(error.to_string(), "boom");
	}
}
