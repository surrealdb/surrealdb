//! Value functions

use anyhow::Result;
use reblessive::tree::TreeStack;

use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::physical_expr::EvalContext;
use crate::exec::physical_expr::function::StreamingClosureEvaluator;
use crate::expr::Kind;
use crate::expr::logical::EffectSet;
use crate::fnc::args::FromArgs;
use crate::val::{ClosureEvaluator, Value};

// =========================================================================
// value::diff - Compute JSON patch diff between two values
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct ValueDiff;

impl ScalarFunction for ValueDiff {
	fn name(&self) -> &'static str {
		"value::diff"
	}

	fn effects(&self) -> EffectSet {
		EffectSet::PURE
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
	) -> crate::exec::BoxFut<'a, Result<Value>> {
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

	fn effects(&self) -> EffectSet {
		EffectSet::PURE
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
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let args = FromArgs::from_args("value::patch", args)?;
			crate::fnc::value::patch(args).await
		})
	}
}

// =========================================================================
// value::chain - Chain a value through a closure
// =========================================================================
//
// `value::chain` and `value::expect` run the closure they are handed, in
// their second argument, against the row the call is evaluated against.
// Both declare that position through `closure_args`, so a closure arriving
// there as a value is judged as running a body the call cannot see; the
// builtins themselves are pure.

#[derive(Debug, Clone, Copy, Default)]
pub struct ValueChain;

impl ScalarFunction for ValueChain {
	fn name(&self) -> &'static str {
		"value::chain"
	}

	fn effects(&self) -> EffectSet {
		EffectSet::PURE
	}

	fn closure_args(&self) -> &'static [usize] {
		&[1]
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
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let args = FromArgs::from_args("value::chain", args)?;
			let evaluator = StreamingClosureEvaluator::new(ctx);
			let ev = Some(&evaluator as &dyn ClosureEvaluator);
			let mut stack = TreeStack::new();
			stack
				.enter(|stk| async move { crate::fnc::value::chain((stk, ev), args).await })
				.finish()
				.await
		})
	}
}

// value::expect: Assert a value with a closure returning bool
// Original value is passed on if closure returns true
#[derive(Debug, Clone, Copy, Default)]
pub struct ValueExpect;

impl ScalarFunction for ValueExpect {
	fn name(&self) -> &'static str {
		"value::expect"
	}

	fn effects(&self) -> EffectSet {
		EffectSet::PURE
	}

	fn closure_args(&self) -> &'static [usize] {
		&[1]
	}

	fn signature(&self) -> Signature {
		Signature::new()
			.arg("value", Kind::Any)
			.arg("closure", Kind::Any)
			.optional("message", Kind::Any)
			.returns(Kind::Any)
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
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let args = FromArgs::from_args("value::expect", args)?;
			let evaluator = StreamingClosureEvaluator::new(ctx);
			let ev = Some(&evaluator as &dyn ClosureEvaluator);
			let mut stack = TreeStack::new();
			stack
				.enter(|stk| async move { crate::fnc::value::expect((stk, ev), args).await })
				.finish()
				.await
		})
	}
}

pub fn register(registry: &mut FunctionRegistry) {
	registry.register(ValueDiff);
	registry.register(ValuePatch);
	registry.register(ValueChain);
	registry.register(ValueExpect);
}
