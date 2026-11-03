#![cfg_attr(not(feature = "api"), allow(unused_imports, dead_code))]

//! A `DEFINE API` request on the engine.
//!
//! A request to a defined endpoint runs as a program assembled from its
//! definition: the `PERMISSIONS` clauses that admit it, a chain of middleware
//! stages, and the handler at the end of the chain. [`process`] checks the
//! request is in the caller's tenant, selects the method's action, judges the
//! permissions, and runs the chain from its first stage.
//!
//! A stage is a function the definition names — a builtin such as
//! `api::req::body`, or a user-defined function — called with the request,
//! the continuation of the chain as `next`, and the arguments the definition
//! stores for it. The continuation is an [`ApiContinuation`]: a closure value
//! both engines invoke natively, running the chain from the stage after the
//! one that received it, so a middleware reaches the rest of the chain
//! exactly when it calls `next`. The handler is the last stage.
//!
//! # The frame each part runs in
//!
//! - The `PERMISSIONS` clauses, the method's first, then the endpoint's, then the database's `API`
//!   configuration's, in the caller's frame with permission enforcement off and writes refused, as
//!   every permission predicate is.
//! - The chain, under the caller's auth narrowed by the definition's `AUTH LIMIT`. A `SECURITY
//!   DEFINER` endpoint, the default, runs with permission enforcement off: it answers for what its
//!   owner defined, not for what its caller may read. A `SECURITY INVOKER` endpoint leaves the
//!   caller's permission state in force, so its queries are judged as the caller's own would be.
//! - Each stage in a context isolated from the stage that called it, so no parameter one stage
//!   binds is seen by another. The handler reads the request as `$request`.
//!
//! # What a stage answers
//!
//! A stage answers with a response. A middleware whose function fails, or
//! answers with something that is not a response, answers with the error's
//! response instead — the first stage of the chain masking an internal error,
//! which a caller must not see, as the endpoint's outermost answer. A handler
//! that fails, or answers with something that is not a response, answers with
//! the error's response. A stage fails, rather than answering, only when it
//! cannot read the request it was handed, or cannot close the savepoint below.
//!
//! The transaction the chain runs in commits whatever response it answers, so
//! a stage that answers with an error's response leaves none of what it wrote
//! behind: a middleware's function, with the rest of the chain it called, and
//! a handler that may write each run behind a savepoint of their own, rolled
//! back when the stage answers with an error's response and released when it
//! answers with its own ([`crate::exec::catch_scope`]).

use std::sync::Arc;

use anyhow::Result;
use http::HeaderValue;
use tracing::{debug, error, trace};

use crate::api::X_SURREAL_REQUEST_ID;
use crate::api::err::ApiError;
use crate::api::request::ApiRequest;
use crate::api::response::ApiResponse;
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{ApiDefinition, ApiSecurity, MiddlewareDefinition, Permission};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::exe::FlowResultExt as _;
use crate::exec::catch_scope::CatchScope;
use crate::exec::{EvalContext, ExecutionContext};
use crate::expr::{ControlFlow, Expr, FlowResult};
use crate::fnc::args::{FromArgs, FromPublic};
use crate::iam::{Action, AuthLimit};
use crate::val::closure::BuiltinClosureObj;
use crate::val::{Closure, Value};

/// Run `req` through `api`, the definition that handles it, in the caller's
/// frame `ctx` and `opt`; see the module documentation.
pub(crate) async fn process(
	ctx: &FrozenContext,
	opt: &Options,
	api: &ApiDefinition,
	req: ApiRequest,
) -> Result<ApiResponse> {
	#[cfg(feature = "api")]
	{
		run_chain(ctx, opt, api, req).await
	}
	#[cfg(not(feature = "api"))]
	{
		let _ = (ctx, opt, api, req);
		Err(anyhow::Error::new(crate::exec::Error::Unimplemented(
			"DEFINE API handlers are not available: this build was compiled without the `api` \
			 feature"
				.to_owned(),
		)))
	}
}

/// The body of [`process`]: authorises the request and runs the API's chain.
#[cfg(feature = "api")]
async fn run_chain(
	ctx: &FrozenContext,
	opt: &Options,
	api: &ApiDefinition,
	req: ApiRequest,
) -> Result<ApiResponse> {
	// Tenant-boundary enforcement. The chain runs with permissions disabled,
	// so reaching it for a namespace/database the caller is not authenticated
	// for is a cross-tenant authorization bypass. The selected ns/db can be
	// steered by caller-controlled input — the URL path on the HTTP route, or
	// session headers / `USE` for `api::invoke` — so the authenticated level
	// is the only trustworthy scope (GHSA-848m-r628-vrxw).
	let (ns_name, db_name) = opt.ns_db()?;
	if !opt.auth.can_access_ns_db(ns_name, db_name) {
		trace!(
			request_id = %req.request_id,
			"API request denied: selected namespace/database is outside the authenticated session scope"
		);
		return Ok(ApiResponse::from_error(ApiError::PermissionDenied, req.request_id.clone()));
	}

	// At most one stored action holds a given method: `DEFINE API` refuses a
	// method twice and `ALTER API` moves one rather than copying it.
	let method_action = api.actions.iter().find(|x| x.methods.contains(&req.method));
	let (handler, method_config) = match (method_action, &api.fallback) {
		(Some(x), _) => (x.action.clone(), Some(&x.config)),
		(None, Some(x)) => (x.clone(), None),
		_ => {
			trace!(
				request_id = %req.request_id,
				method = ?req.method,
				"No matching handler or fallback for API request"
			);
			return Ok(ApiResponse::from_error(ApiError::NotFound, req.request_id.clone()));
		}
	};

	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let global_entry = ctx.tx().get_db_config(ns, db, "api", None).await?;
	let global = global_entry.as_ref().map(|v| v.try_as_api()).transpose()?;

	if ctx.check_perms(&opt.clone().with_perms(true), Action::Edit)? {
		let permissions = method_config
			.map(|config| &config.permissions)
			.into_iter()
			.chain(std::iter::once(&api.config.permissions))
			.chain(global.as_ref().map(|config| &config.permissions));
		for permission in permissions {
			let admitted = match permission {
				Permission::None => false,
				Permission::Full => true,
				Permission::Specific(clause) => {
					// Disable permission recursion and block side effects.
					let frame =
						crate::exec::entry::frame(ctx, &opt.new_for_permission_predicate(), None)
							.await;
					crate::exec::entry::scalar(&frame, clause).await?.is_truthy()
				}
			};
			if !admitted {
				trace!(request_id = %req.request_id, "API request denied by its PERMISSIONS");
				return Ok(ApiResponse::from_error(
					ApiError::PermissionDenied,
					req.request_id.clone(),
				));
			}
		}
	}

	let stages: Vec<MiddlewareDefinition> = global
		.into_iter()
		.flat_map(|cfg| cfg.middleware.iter().cloned())
		.chain(api.config.middleware.iter().cloned())
		.chain(method_config.into_iter().flat_map(|config| config.middleware.iter().cloned()))
		.collect();
	debug!(
		request_id = %req.request_id,
		middleware_count = stages.len(),
		"Executing API middleware chain"
	);
	let chain = Arc::new(ApiChain {
		request_id: req.request_id.clone(),
		stages,
		handler,
	});

	// The chain runs under the definer's auth limit: permissions suppressed for
	// a definer-authored API, or the caller's own permissions for an invoker
	// one. Either way it carries whether the caller's own reads are permission
	// checked, so a UNIQUE-index conflict a write in the chain raises names
	// neither the stored record nor its value to a caller who cannot read them:
	// the response body carries the error's text.
	let caller_view_checked = ctx.check_perms(opt, Action::View)?;
	let opt = opt.limited_by(&AuthLimit::try_from(&api.auth_limit)?);
	let opt = match api.security {
		ApiSecurity::Definer => opt.new_with_perms(false),
		ApiSecurity::Invoker => opt,
	};
	let frame = crate::exec::entry::frame(ctx, &opt, None)
		.await
		.with_caller_view_checked(caller_view_checked);
	let answered = crate::exec::trampoline::nest(run_stage(chain, 0, req.into(), frame))
		.await
		.map_err(|signal| {
			signal.into_error_or(|other| {
				anyhow::anyhow!(crate::err::EngineError::Internal(format!(
					"unexpected control flow from an API chain: {other}"
				)))
			})
		})?;
	let mut res: ApiResponse = answered.try_into()?;
	res.ensure_request_id_header();
	Ok(res)
}

/// The chain one request runs: its middleware stages in execution order, and
/// the handler after them.
struct ApiChain {
	request_id: String,
	stages: Vec<MiddlewareDefinition>,
	handler: Expr,
}

impl ApiChain {
	/// `req` read as the request a stage is handed, with the request id the
	/// server assigned restored on it and in its headers, whatever an
	/// earlier stage did to either.
	fn request(&self, req: Value) -> Option<ApiRequest> {
		let (FromPublic(mut req),): (FromPublic<ApiRequest>,) =
			FromArgs::from_args("", vec![req]).ok()?;
		req.request_id.clone_from(&self.request_id);
		if !self.request_id.is_empty() {
			let _ = req.headers.insert(
				X_SURREAL_REQUEST_ID,
				HeaderValue::from_str(&self.request_id)
					.unwrap_or_else(|_| HeaderValue::from_static("unknown")),
			);
		}
		Some(req)
	}
}

/// The rest of an API chain from `stage`: the `next` a middleware is handed.
pub(crate) struct ApiContinuation {
	chain: Arc<ApiChain>,
	stage: usize,
}

impl BuiltinClosureObj for ApiContinuation {
	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

impl ApiContinuation {
	/// Run the chain from this continuation's stage with `args`, the request
	/// first, in `exec`, the frame of the stage that called it.
	pub(crate) async fn invoke(
		&self,
		args: Vec<Value>,
		exec: &ExecutionContext,
	) -> FlowResult<Value> {
		#[cfg(not(feature = "api"))]
		{
			// Only `process` makes a continuation, and without `api` it runs
			// no chain.
			let _ = (args, exec);
			Err(ControlFlow::Err(anyhow::Error::new(crate::exec::Error::Unimplemented(
				"DEFINE API handlers are not available: this build was compiled without the \
				 `api` feature"
					.to_owned(),
			))))
		}
		#[cfg(feature = "api")]
		{
			let req = args.into_iter().next().unwrap_or(Value::None);
			crate::exec::trampoline::nest(run_stage(
				Arc::clone(&self.chain),
				self.stage,
				req,
				exec.clone(),
			))
			.await
		}
	}
}

/// Run `chain` from `stage` with the request `req`, in `exec`.
fn run_stage(
	chain: Arc<ApiChain>,
	stage: usize,
	req: Value,
	exec: ExecutionContext,
) -> crate::exec::BoxFut<'static, FlowResult<Value>> {
	Box::pin(async move {
		match chain.stages.get(stage) {
			Some(middleware) => run_middleware(&chain, stage, middleware, req, &exec).await,
			None => run_handler(&chain, req, &exec).await,
		}
	})
}

/// Stage `stage` of `chain`, the middleware `middleware`: its function called
/// with the request, the continuation after it, and its stored arguments.
async fn run_middleware(
	chain: &Arc<ApiChain>,
	stage: usize,
	middleware: &MiddlewareDefinition,
	req: Value,
	exec: &ExecutionContext,
) -> FlowResult<Value> {
	let Some(req) = chain.request(req) else {
		return Err(ControlFlow::Err(
			ApiError::MiddlewareRequestParseFailure {
				middleware: middleware.name.to_string(),
			}
			.into(),
		));
	};
	let function: crate::expr::Function = match crate::syn::function_with_capabilities(
		middleware.name.as_str(),
		exec.ctx().get_capabilities().as_ref(),
		&exec.ctx().config.parser,
	) {
		Ok(function) => function,
		Err(_) => {
			return Err(ControlFlow::Err(
				ApiError::MiddlewareFunctionNotFound {
					function: middleware.name.to_string(),
				}
				.into(),
			));
		}
	};
	let next = Closure::Builtin(Arc::new(ApiContinuation {
		chain: Arc::clone(chain),
		stage: stage + 1,
	}));
	let mut args = vec![Value::from(req), Value::Closure(Box::new(next))];
	args.extend(middleware.args.iter().cloned());

	// Each middleware runs isolated from the stage that called it.
	let isolated = exec.with_new_ctx(Context::new_isolated(exec.ctx()).freeze());
	let scope = CatchScope::open(exec.try_txn().as_ref(), true).await.map_err(ControlFlow::Err)?;
	let called = call_middleware(&function, args, &isolated)
		.await
		.catch_return()
		.and_then(ApiResponse::try_from);
	scope.end(called.is_err()).await.map_err(ControlFlow::Err)?;

	let is_initial = stage == 0;
	let failed = |e: anyhow::Error| {
		if is_initial {
			error!(
				request_id = %chain.request_id,
				middleware = %middleware.name,
				error = %e,
				"API middleware error; converting to response (ApiError exposed, internal errors masked)"
			);
			ApiResponse::from_error_secure(e, chain.request_id.clone())
		} else {
			ApiResponse::from_error(e, chain.request_id.clone())
		}
	};
	let mut res = match called {
		Ok(mut res) => {
			res.request_id.clone_from(&chain.request_id);
			res
		}
		Err(e) => failed(e),
	};
	res.ensure_request_id_header();
	Ok(Value::from(res))
}

/// Call `function`, a middleware, with `args` in `exec`: a builtin through the
/// function registry, a user-defined function through its call, and any other
/// kind of function as a compiled call.
async fn call_middleware(
	function: &crate::expr::Function,
	args: Vec<Value>,
	exec: &ExecutionContext,
) -> FlowResult<Value> {
	let eval = EvalContext::from_exec_ctx(exec);
	match function {
		crate::expr::Function::Normal(name) => {
			eval.check_allowed_function(name).map_err(ControlFlow::Err)?;
			let registry = exec.function_registry();
			let builtin = registry.get(name).ok_or_else(|| {
				ControlFlow::Err(anyhow::anyhow!(
					"Unknown function '{name}' - not found in function registry"
				))
			})?;
			if builtin.is_pure() && !builtin.is_async() {
				builtin.invoke(args).map_err(ControlFlow::Err)
			} else {
				if builtin.effects().contains(crate::expr::logical::EffectSet::WRITES_EXTERNAL) {
					exec.txn().note_external_effect();
				}
				builtin.invoke_async(&eval, args).await.map_err(ControlFlow::Err)
			}
		}
		crate::expr::Function::Custom(name) => {
			crate::exec::vm::invoke_stored_function(name, 0, args, &eval).await
		}

		other @ (crate::expr::Function::Script(_)
		| crate::expr::Function::Model(_)
		| crate::expr::Function::Module(..)
		| crate::expr::Function::Silo {
			..
		}) => {
			// Any other kind of function — a module, a silo package, a script
			// or a model — is compiled as the call it is, over the arguments
			// bound as parameters of a context of its own, and evaluated by
			// the engine.
			use crate::expr::logical::{LogicalCall, LogicalExpr};
			let mut scope = Context::new_isolated(exec.ctx());
			let mut params = Vec::with_capacity(args.len());
			for (i, arg) in args.into_iter().enumerate() {
				let name = format!("middleware_arg_{i}");
				params.push(LogicalExpr::Param(name.clone().into()));
				scope.add_value(name, Arc::new(arg));
			}
			let exec = exec.with_new_ctx(scope.freeze());
			let call = LogicalExpr::Call(Box::new(LogicalCall {
				receiver: other.clone(),
				args: params,
			}));
			// Served from the compile cache by the call as written; the
			// arguments it folds in are what it is kept against
			// ([`crate::exec::vm::serve`]).
			let compiled = crate::exec::vm::serve::expr(&exec, call, chain_principal(&exec), 0)
				.await
				.map_err(ControlFlow::Err)?;
			compiled.evaluate(EvalContext::from_exec_ctx(&exec)).await
		}
	}
}

/// The chain's last stage: the handler, evaluated with the request bound as
/// `$request`.
async fn run_handler(chain: &ApiChain, req: Value, exec: &ExecutionContext) -> FlowResult<Value> {
	let Some(req) = chain.request(req) else {
		return Err(ControlFlow::Err(ApiError::FinalActionRequestParseFailure.into()));
	};
	let mut isolated = Context::new_isolated(exec.ctx());
	isolated.add_value("request", Arc::new(req.into()));
	let exec = exec.with_new_ctx(isolated.freeze());
	let scope = CatchScope::open(exec.try_txn().as_ref(), !chain.handler.read_only())
		.await
		.map_err(ControlFlow::Err)?;
	let answered = evaluate_handler(&chain.handler, &exec).await.and_then(ApiResponse::try_from);
	scope.end(answered.is_err()).await.map_err(ControlFlow::Err)?;
	let mut res = answered.unwrap_or_else(|e| ApiResponse::from_error(e, chain.request_id.clone()));
	res.request_id.clone_from(&chain.request_id);
	res.ensure_request_id_header();
	Ok(Value::from(res))
}

/// The principal a stage of the chain is compiled for: the caller's when its
/// reads are permission-checked, and none when they are not, so a plan that
/// reads without permissions is shared by every caller.
fn chain_principal(exec: &ExecutionContext) -> Option<Arc<crate::iam::Auth>> {
	if exec.root().skip_fetch_perms {
		None
	} else {
		Some(Arc::clone(&exec.root().auth))
	}
}

/// The value of `handler` in `exec`.
///
/// A handler written as a block runs as the statement it is, planned by the
/// statement planner — on the VM where that covers it. It is bound to the
/// caller's principal when the chain's reads are permission-checked (`SECURITY
/// INVOKER` under a caller subject to permissions), and without one when they
/// are not, so every caller of a `SECURITY DEFINER` handler shares one plan. Any other
/// handler, and a block the planner declines, is an expression reached from
/// outside a plan ([`crate::exec::entry`]).
async fn evaluate_handler(handler: &Expr, exec: &ExecutionContext) -> Result<Value> {
	use crate::expr::logical::LogicalExpr;
	// The handler is evaluated one expression at a time, and spends the way
	// to the write or the request it makes as it is entered
	// ([`crate::exec::vm::depth`]).
	let depth = crate::exec::vm::depth::ExprDepth::of_expr(handler);
	let clamp = |n: u32| u8::try_from(n).unwrap_or(u8::MAX);
	exec.check_dive_by(clamp(depth.deepest))?;
	let exec = &exec.dive_by(clamp(depth.handler_charged()))?.entering_stored_code(0, true);
	let logical = crate::expr::logical::lower_expr(handler.clone());
	if let Ok(LogicalExpr::Subquery(op)) = &logical
		&& crate::exec::logical::unplannable_write_in_expr(logical.as_ref().expect("matched"))
			.is_none()
		&& exec.try_txn().is_some()
	{
		let principal = chain_principal(exec);
		// Served from the compile cache by the handler as written
		// ([`crate::exec::vm::serve`]).
		match crate::exec::vm::serve::statement(exec, op.as_ref().clone(), principal, 0, Vec::new())
			.await
		{
			Ok(plan) => {
				let shape = plan.output_shape();
				let stream = plan.execute(exec).map_err(flow_error)?;
				return crate::exec::operators::collect_shaped_value(stream, shape)
					.await
					.catch_return();
			}
			Err(e)
				if matches!(
					e.downcast_ref::<crate::err::Error>(),
					Some(crate::err::Error::Exec(crate::exec::Error::PlannerUnsupported { .. }))
				) || matches!(
					e.downcast_ref::<crate::exec::Error>(),
					Some(crate::exec::Error::PlannerUnsupported { .. })
				) => {}
			Err(e) => return Err(e),
		}
	}
	crate::exec::entry::scalar(exec, handler).await
}

fn flow_error(signal: ControlFlow) -> anyhow::Error {
	signal.into_error_or(|other| {
		anyhow::anyhow!(crate::err::EngineError::Internal(format!(
			"unexpected control flow from an API handler: {other}"
		)))
	})
}
