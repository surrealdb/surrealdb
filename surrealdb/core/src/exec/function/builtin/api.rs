//! API functions for the engine.

use anyhow::Result;

use crate::api::middleware::common::BodyStrategy;
use crate::api::request::ApiRequest;
use crate::err::EngineError;
use crate::exec::EvalContext;
use crate::exec::function::{ScalarFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::expr::Kind;
use crate::expr::logical::EffectSet;
use crate::fnc::args::{FromArgs, FromPublic, Optional};
use crate::val::{Closure, Value};

// =========================================================================
// api::invoke - Invoke a defined API endpoint
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct ApiInvoke;

impl ScalarFunction for ApiInvoke {
	fn name(&self) -> &'static str {
		"api::invoke"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(
			Scalar,
			"api::invoke",
			Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
		)
	}

	fn effects(&self) -> EffectSet {
		EffectSet::OPAQUE_BODY | EffectSet::WRITES_DATA
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
				anyhow::anyhow!(EngineError::Internal(
					"No options available for api::invoke".to_string()
				))
			})?;

			// Convert args using FromArgs (the same conversion the `fnc` dispatch uses)
			let (path, Optional(req)): (String, Optional<FromPublic<ApiRequest>>) =
				FromArgs::from_args("api::invoke", args)?;
			// The API is looked up by the path the call names, its definition is
			// loaded, and its middleware and handler are served from the compile
			// cache ([`crate::exec::vm::serve`]). All of this runs inside
			// [`crate::exec::compiles::seam`].
			let response = crate::exec::compiles::seam(async {
				anyhow::Ok(
					match crate::fnc::api::route(frozen, opt, &path, req.map(|x| x.0)).await? {
						crate::fnc::api::Routed::Found(api, req) => {
							crate::exec::api::process(frozen, opt, &api, req).await?
						}
						crate::fnc::api::Routed::NotFound(response) => response,
					},
				)
			})
			.await?;
			crate::fnc::api::invoked(response)
		})
	}
}

// =========================================================================
// API middleware functions (api::req::*, api::res::*, api::timeout)
// =========================================================================

/// Helper macro for API middleware functions: each converts its arguments as
/// the `fnc` dispatch does, does its work on the request or the response,
/// and runs the `next` continuation it is handed through the
/// engine's closure evaluator. A trailing `, fallibility: <class>` declares
/// [`ScalarFunction::fallibility`].
macro_rules! define_api_middleware_function {
	($struct_name:ident, $func_name:literal, effects: $effects:expr, $impl_path:path, $sig:expr, fallibility: $fallibility:expr) => {
		#[allow(non_upper_case_globals)]
		pub const $struct_name: $crate::exec::function::BuiltinScalarFn =
			$crate::exec::function::BuiltinScalarFn {
				name: $func_name,
				effects: $effects,
				fallibility: crate::declare_fallibility!(Scalar, $func_name, $fallibility),
				closure_args: &[],
				signature: || $sig,
				exec: $crate::exec::function::ScalarExec::Async(|ctx, args| {
					let decoded = match FromArgs::from_args($func_name, args) {
						Ok(v) => v,
						Err(e) => return $crate::exec::function::ready_boxed(Err(e)),
					};
					Box::pin($impl_path(ctx, decoded))
				}),
			};
	};
}

/// Run `next`, the rest of an API chain, with `req` in `ctx`.
async fn call_next(next: &Closure, req: Value, ctx: &EvalContext<'_>) -> Result<Value> {
	crate::exec::vm::calls::helpers::invoke_closure(next, vec![req], ctx).await.map_err(|signal| {
		signal.into_error_or(|other| {
			anyhow::anyhow!(EngineError::Internal(format!(
				"unexpected control flow from an API continuation: {other}"
			)))
		})
	})
}

async fn req_body(
	ctx: &EvalContext<'_>,
	(FromPublic(mut req), next, Optional(strategy)): (
		FromPublic<ApiRequest>,
		Box<Closure>,
		Optional<FromPublic<BodyStrategy>>,
	),
) -> Result<Value> {
	crate::fnc::api::req::parse_body(&mut req, strategy.map(|x| x.0), ctx.exec_ctx.ctx()).await?;
	call_next(&next, req.into(), ctx).await
}

async fn req_max_body(
	ctx: &EvalContext<'_>,
	(FromPublic(req), next, limit): (FromPublic<ApiRequest>, Box<Closure>, Value),
) -> Result<Value> {
	crate::fnc::api::req::check_body_size(&req, &limit)?;
	call_next(&next, req.into(), ctx).await
}

async fn res_body(
	ctx: &EvalContext<'_>,
	(FromPublic(req), next, Optional(strategy)): (
		FromPublic<ApiRequest>,
		Box<Closure>,
		Optional<FromPublic<BodyStrategy>>,
	),
) -> Result<Value> {
	let res = call_next(&next, req.clone().into(), ctx).await?;
	crate::fnc::api::res::encode_body(&req, res, strategy.map(|x| x.0))
}

async fn res_status(
	ctx: &EvalContext<'_>,
	(req, next, status): (Value, Box<Closure>, i64),
) -> Result<Value> {
	let res = call_next(&next, req, ctx).await?;
	crate::fnc::api::res::set_status(res, status)
}

async fn res_header(
	ctx: &EvalContext<'_>,
	(req, next, name, Optional(value)): (Value, Box<Closure>, String, Optional<String>),
) -> Result<Value> {
	let res = call_next(&next, req, ctx).await?;
	crate::fnc::api::res::set_header(res, &name, value)
}

async fn res_headers(
	ctx: &EvalContext<'_>,
	(req, next, headers): (Value, Box<Closure>, std::collections::BTreeMap<String, Option<String>>),
) -> Result<Value> {
	let res = call_next(&next, req, ctx).await?;
	crate::fnc::api::res::set_headers(res, headers)
}

/// `api::timeout`: the rest of the chain runs in a context whose deadline is
/// `timeout` from now.
async fn timeout(
	ctx: &EvalContext<'_>,
	(req, next, timeout): (Value, Box<Closure>, crate::val::Duration),
) -> Result<Value> {
	let mut bounded = crate::ctx::Context::new_isolated(ctx.exec_ctx.ctx());
	bounded.add_timeout(*timeout)?;
	let exec = ctx.exec_ctx.with_new_ctx(bounded.freeze());
	call_next(&next, req, &EvalContext::from_exec_ctx(&exec)).await
}

define_api_middleware_function!(
	ApiReqBody,
	"api::req::body",
	effects: EffectSet::OPAQUE_BODY,
	req_body,
	Signature::new()
		.arg("request", Kind::Object)
		.arg("next", Kind::Function(None, None))
		.optional("strategy", Kind::String)
		.returns(Kind::Any),
	fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
);

define_api_middleware_function!(
	ApiReqMaxBody,
	"api::req::max_body",
	effects: EffectSet::OPAQUE_BODY,
	req_max_body,
	Signature::new()
		.arg("request", Kind::Object)
		.arg("next", Kind::Function(None, None))
		.arg("limit", Kind::Any)
		.returns(Kind::Any),
	fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
);

define_api_middleware_function!(
	ApiResBody,
	"api::res::body",
	effects: EffectSet::OPAQUE_BODY,
	res_body,
	Signature::new()
		.arg("request", Kind::Object)
		.arg("next", Kind::Function(None, None))
		.optional("strategy", Kind::String)
		.returns(Kind::Any),
	fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
);

define_api_middleware_function!(
	ApiResStatus,
	"api::res::status",
	effects: EffectSet::OPAQUE_BODY,
	res_status,
	Signature::new()
		.arg("request", Kind::Any)
		.arg("next", Kind::Function(None, None))
		.arg("status", Kind::Int)
		.returns(Kind::Any),
	fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
);

define_api_middleware_function!(
	ApiResHeader,
	"api::res::header",
	effects: EffectSet::OPAQUE_BODY,
	res_header,
	Signature::new()
		.arg("request", Kind::Any)
		.arg("next", Kind::Function(None, None))
		.arg("name", Kind::String)
		.optional("value", Kind::String)
		.returns(Kind::Any),
	fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
);

define_api_middleware_function!(
	ApiResHeaders,
	"api::res::headers",
	effects: EffectSet::OPAQUE_BODY,
	res_headers,
	Signature::new()
		.arg("request", Kind::Any)
		.arg("next", Kind::Function(None, None))
		.arg("headers", Kind::Object)
		.returns(Kind::Any),
	fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
);

define_api_middleware_function!(
	ApiTimeout,
	"api::timeout",
	effects: EffectSet::OPAQUE_BODY,
	timeout,
	Signature::new()
		.arg("request", Kind::Any)
		.arg("next", Kind::Function(None, None))
		.arg("timeout", Kind::Duration)
		.returns(Kind::Any),
	fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
);

// Each middleware function runs the `next` continuation it is handed. The body
// of `next` is not visible from the call, so these functions declare
// `EffectSet::OPAQUE_BODY`. `api::invoke` runs a whole endpoint that may write,
// so it also declares `WRITES_DATA`. `is_writer_builtin` lists the same
// functions, and the registry's guard test checks that the two agree.
/// The `api::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&ApiInvoke,
	&ApiReqBody,
	&ApiReqMaxBody,
	&ApiResBody,
	&ApiResStatus,
	&ApiResHeader,
	&ApiResHeaders,
	&ApiTimeout,
];
