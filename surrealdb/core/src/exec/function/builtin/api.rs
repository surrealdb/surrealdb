//! API functions for the streaming executor.

use anyhow::Result;
use reblessive::TreeStack;

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
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
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

// =========================================================================
// API middleware functions (api::req::*, api::res::*, api::timeout)
// =========================================================================

/// Helper macro for API middleware functions, which wrap the legacy
/// `fnc::api::*` implementations behind a reblessive `TreeStack`.
macro_rules! define_api_middleware_function {
	($struct_name:ident, $func_name:literal, $impl_path:path, $sig:expr) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl ScalarFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn signature(&self) -> Signature {
				$sig
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
					use crate::doc::CursorDoc;
					let frozen = ctx.exec_ctx.ctx();
					let opt = ctx.exec_ctx.options().ok_or_else(|| {
						anyhow::anyhow!(Error::Internal(format!(
							"No options available for {}",
							$func_name
						)))
					})?;
					let args = FromArgs::from_args($func_name, args)?;
					let doc = ctx
						.document_root
						.or(ctx.current_value)
						.map(|v| CursorDoc::new(None, None, v.clone()));
					let mut stack = TreeStack::new();
					stack
						.enter(|stk| async move {
							$impl_path((stk, frozen, opt, doc.as_ref()), args).await
						})
						.finish()
						.await
				})
			}
		}
	};
}

define_api_middleware_function!(
	ApiReqBody,
	"api::req::body",
	crate::fnc::api::req::body,
	Signature::new()
		.arg("request", Kind::Object)
		.arg("next", Kind::Function(None, None))
		.optional("strategy", Kind::String)
		.returns(Kind::Any)
);

define_api_middleware_function!(
	ApiResBody,
	"api::res::body",
	crate::fnc::api::res::body,
	Signature::new()
		.arg("request", Kind::Object)
		.arg("next", Kind::Function(None, None))
		.optional("strategy", Kind::String)
		.returns(Kind::Any)
);

define_api_middleware_function!(
	ApiResStatus,
	"api::res::status",
	crate::fnc::api::res::status,
	Signature::new()
		.arg("request", Kind::Any)
		.arg("next", Kind::Function(None, None))
		.arg("status", Kind::Int)
		.returns(Kind::Any)
);

define_api_middleware_function!(
	ApiResHeader,
	"api::res::header",
	crate::fnc::api::res::header,
	Signature::new()
		.arg("request", Kind::Any)
		.arg("next", Kind::Function(None, None))
		.arg("name", Kind::String)
		.optional("value", Kind::String)
		.returns(Kind::Any)
);

define_api_middleware_function!(
	ApiResHeaders,
	"api::res::headers",
	crate::fnc::api::res::headers,
	Signature::new()
		.arg("request", Kind::Any)
		.arg("next", Kind::Function(None, None))
		.arg("headers", Kind::Object)
		.returns(Kind::Any)
);

define_api_middleware_function!(
	ApiTimeout,
	"api::timeout",
	crate::fnc::api::timeout,
	Signature::new()
		.arg("request", Kind::Any)
		.arg("next", Kind::Function(None, None))
		.arg("timeout", Kind::Duration)
		.returns(Kind::Any)
);

pub fn register(registry: &mut FunctionRegistry) {
	registry.register(ApiInvoke);
	registry.register(ApiReqBody);
	registry.register(ApiResBody);
	registry.register(ApiResStatus);
	registry.register(ApiResHeader);
	registry.register(ApiResHeaders);
	registry.register(ApiTimeout);
}
