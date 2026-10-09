//! Stand-ins for a function family a cargo feature left out.
//!
//! Each keeps its name resolving, accepts any arguments, and fails every call
//! with an error that names the function and the feature the build lacks.

use anyhow::Result;

use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::expr::Kind;
use crate::expr::logical::EffectSet;
use crate::val::Value;

#[derive(Debug, Clone, Copy)]
struct Disabled {
	name: &'static str,
	feature: &'static str,
}

impl ScalarFunction for Disabled {
	fn name(&self) -> &'static str {
		self.name
	}

	fn signature(&self) -> Signature {
		Signature::new().variadic(Kind::Any).returns(Kind::Any)
	}

	fn is_pure(&self) -> bool {
		false
	}

	fn effects(&self) -> EffectSet {
		EffectSet::OPAQUE_BODY
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::Error::new(crate::exec::Error::InvalidFunction {
			name: self.name.to_owned(),
			message: format!(
				"This build was compiled without the `{}` feature, which this function requires.",
				self.feature
			),
		}))
	}
}

/// Registers a stand-in for each of `names`, all left out with `feature`.
pub(super) fn register(
	registry: &mut FunctionRegistry,
	feature: &'static str,
	names: &[&'static str],
) {
	for &name in names {
		registry.register(Disabled {
			name,
			feature,
		});
	}
}
