//! Sequence functions

use anyhow::Result;

use crate::err::EngineError;
use crate::exec::EvalContext;
use crate::exec::function::{ScalarFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::expr::Kind;
use crate::expr::logical::EffectSet;
use crate::fnc::args::FromArgs;
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

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Scalar, "sequence::nextval", Fallibility::DATA)
	}

	fn effects(&self) -> EffectSet {
		EffectSet::ADVANCES_STATE
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
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let frozen = ctx.exec_ctx.ctx();
			let opt = ctx.exec_ctx.options().ok_or_else(|| {
				anyhow::anyhow!(EngineError::Internal(
					"No options available for sequence operation".to_string()
				))
			})?;

			// Get the sequence name from args
			let args = FromArgs::from_args("sequence::nextval", args)?;

			crate::fnc::sequence::nextval((frozen, opt), args).await
		})
	}
}

// Each call advances the sequence once and returns the value it moved past,
// as `EffectSet::ADVANCES_STATE` declares. Callers must not duplicate or drop
// a call: a duplicate consumes an extra value, and a dropped call leaves the
// sequence where it stood.
/// The `sequence::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[&SequenceNextval];
