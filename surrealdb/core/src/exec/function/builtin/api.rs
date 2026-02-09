//! API functions for the streaming executor.

use std::pin::Pin;

use anyhow::Result;
use reblessive::TreeStack;

use crate::dbs::capabilities::ExperimentalTarget;
use crate::err::Error;
use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::physical_expr::EvalContext;
use crate::expr::Kind;
use crate::fnc::args::FromArgs;
use crate::val::Value;

// =========================================================================
// api::invoke - Invoke a defined API endpoint
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct ApiInvoke;

impl ScalarFunction for ApiInvoke {
	fn name(&self) -> &'static str {
		"api::invoke"
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("path", Kind::String).optional("request", Kind::Any).returns(Kind::Any)
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
			// Check DefineApi experimental capability
			let caps = ctx.capabilities();
			if !caps.allows_experimental(&ExperimentalTarget::DefineApi) {
				return Err(Error::InvalidFunction {
					name: "api::invoke".to_string(),
					message: format!(
						"Experimental feature {} is not enabled",
						ExperimentalTarget::DefineApi
					),
				}
				.into());
			}

			let frozen = ctx.exec_ctx.ctx();
			let opt = ctx.exec_ctx.options().ok_or_else(|| {
				anyhow::anyhow!(Error::Internal("No options available for api::invoke".to_string()))
			})?;

			// Convert args using FromArgs (same conversion the legacy dispatch uses)
			let args = FromArgs::from_args("api::invoke", args)?;

			// Create a TreeStack for the reblessive stack required by api::invoke
			let mut stack = TreeStack::new();
			stack
				.enter(|stk| async move { crate::fnc::api::invoke((stk, frozen, opt), args).await })
				.finish()
				.await
		})
	}
}

pub fn register(registry: &mut FunctionRegistry) {
	registry.register(ApiInvoke);
}
