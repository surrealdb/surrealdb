//! Sequence functions

use std::pin::Pin;

use anyhow::Result;

use crate::err::Error;
use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::physical_expr::EvalContext;
use crate::expr::Kind;
use crate::val::Value;

// =========================================================================
// sequence::nextval - Get the next value from a sequence
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct SequenceNextval;

impl ScalarFunction for SequenceNextval {
	fn name(&self) -> &'static str {
		"sequence::nextval"
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("sequence", Kind::String).returns(Kind::Int)
	}

	fn is_pure(&self) -> bool {
		false
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
			let frozen = ctx.exec_ctx.ctx();
			let opt = ctx.exec_ctx.options().ok_or_else(|| {
				anyhow::anyhow!(Error::Internal(
					"No options available for sequence operation".to_string()
				))
			})?;

			// Get the sequence name from args
			let seq = args.into_iter().next().unwrap_or(Value::None);

			crate::fnc::sequence::nextval((frozen, opt), (seq,)).await
		})
	}
}

pub fn register(registry: &mut FunctionRegistry) {
	registry.register(SequenceNextval);
}
