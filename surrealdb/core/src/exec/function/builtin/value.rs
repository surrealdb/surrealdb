//! Value functions

use std::pin::Pin;

use anyhow::Result;
use reblessive::tree::TreeStack;

use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::physical_expr::EvalContext;
use crate::expr::Kind;
use crate::fnc::args::FromArgs;
use crate::val::Value;

// =========================================================================
// value::diff - Compute JSON patch diff between two values
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct ValueDiff;

impl ScalarFunction for ValueDiff {
	fn name(&self) -> &'static str {
		"value::diff"
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("val1", Kind::Any).arg("val2", Kind::Any).returns(Kind::Any)
	}

	fn is_pure(&self) -> bool {
		true
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		_ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + 'a>> {
		Box::pin(async move {
			let args = FromArgs::from_args("value::diff", args)?;
			crate::fnc::value::diff(args).await
		})
	}
}

// =========================================================================
// value::patch - Apply JSON patch to a value
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct ValuePatch;

impl ScalarFunction for ValuePatch {
	fn name(&self) -> &'static str {
		"value::patch"
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Any).arg("patch", Kind::Any).returns(Kind::Any)
	}

	fn is_pure(&self) -> bool {
		true
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		_ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + 'a>> {
		Box::pin(async move {
			let args = FromArgs::from_args("value::patch", args)?;
			crate::fnc::value::patch(args).await
		})
	}
}

// =========================================================================
// value::chain - Chain a value through a closure
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct ValueChain;

impl ScalarFunction for ValueChain {
	fn name(&self) -> &'static str {
		"value::chain"
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Any).arg("closure", Kind::Any).returns(Kind::Any)
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
			let args = FromArgs::from_args("value::chain", args)?;
			let frozen = ctx.exec_ctx.ctx();
			let opt = ctx.exec_ctx.options();
			// Note: CursorDoc is not available in the streaming executor context
			let doc = None;
			let mut stack = TreeStack::new();
			stack
				.enter(|stk| async move {
					crate::fnc::value::chain((stk, frozen, opt, doc), args).await
				})
				.finish()
				.await
		})
	}
}

pub fn register(registry: &mut FunctionRegistry) {
	registry.register(ValueDiff);
	registry.register(ValuePatch);
	registry.register(ValueChain);
}
