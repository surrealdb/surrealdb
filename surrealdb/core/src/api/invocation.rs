use std::sync::Arc;

use anyhow::Result;
use reblessive::TreeStack;
use reblessive::tree::Stk;

use super::response::ApiResponse;
use crate::api::err::ApiError;
use crate::api::request::ApiRequest;
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{ApiDefinition, MiddlewareDefinition};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, FlowResultExt as _};
use crate::fnc::args::{Any, FromArgs, FromPublic};
use crate::iam::AuthLimit;
use crate::syn::function_with_capabilities;
use crate::val::{Closure, Value};

/// Processes an API request through the middleware chain and executes the handler.
///
/// This function orchestrates the entire API request lifecycle:
/// 1. Finds the appropriate handler based on HTTP method
/// 2. Collects middleware from database-level, route-level, and method-level configs
/// 3. Builds a middleware chain in execution order
/// 4. Executes the middleware chain with the final handler
/// 5. Returns the response or `None` if no handler matched
///
/// # Arguments
/// * `ctx` - The frozen context containing database and transaction information
/// * `opt` - Database options including permissions
/// * `api` - The API definition containing path, handlers, and middleware configuration
/// * `req` - The incoming API request with method, headers, body, params, etc.
///
/// # Returns
/// * `Ok(Some(response))` - Successfully processed request with a response
/// * `Ok(None)` - No matching handler found for the request method
/// * `Err(e)` - Error during processing (middleware failure, handler error, etc.)
pub async fn process_api_request(
	ctx: &FrozenContext,
	opt: &Options,
	api: &ApiDefinition,
	req: ApiRequest,
) -> Result<Option<ApiResponse>> {
	let mut stack = TreeStack::new();
	stack.enter(|stk| process_api_request_with_stack(stk, ctx, opt, api, req)).finish().await
}

/// Internal version of `process_api_request` that uses an existing stack.
///
/// This function is used internally when a stack is already available,
/// avoiding the overhead of creating a new stack.
///
/// # Arguments
/// * `stk` - The existing reblessive stack for async execution
/// * `ctx` - The frozen context containing database and transaction information
/// * `opt` - Database options including permissions
/// * `api` - The API definition containing path, handlers, and middleware configuration
/// * `req` - The incoming API request with method, headers, body, params, etc.
///
/// # Returns
/// * `Ok(Some(response))` - Successfully processed request with a response
/// * `Ok(None)` - No matching handler found for the request method
/// * `Err(e)` - Error during processing (middleware failure, handler error, etc.)
pub async fn process_api_request_with_stack(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	api: &ApiDefinition,
	req: ApiRequest,
) -> Result<Option<ApiResponse>> {
	// TODO: Figure out if it is possible if multiple actions can have the same
	// method, and if so should they all be run?
	let method_action = api.actions.iter().find(|x| x.methods.contains(&req.method));

	let (action_expr, method_config) = match (method_action, &api.fallback) {
		(Some(x), _) => (x.action.clone(), Some(&x.config)),
		(None, Some(x)) => (x.clone(), None),
		// nothing to do, just return
		_ => {
			return Ok(None);
		}
	};

	// first run the middleware which is globally configured for the database.
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let global = ctx.tx().get_db_config(ns, db, "api").await?;
	let middleware: Vec<_> = global
		.as_ref()
		.map(|v| v.try_as_api())
		.transpose()?
		.into_iter()
		.flat_map(|cfg| cfg.middleware.iter().cloned())
		.chain(api.config.middleware.iter().cloned())
		.chain(method_config.into_iter().flat_map(|config| config.middleware.iter().cloned()))
		.collect();

	// Create the final action closure (end of the middleware chain)
	let final_action = create_final_action_closure(action_expr);

	// Build the middleware chain backwards, wrapping each middleware around the previous closure
	let next = middleware
		.iter()
		.rev()
		.fold(final_action, |next, def| create_middleware_closure(def.clone(), next));

	// APIs run without permissions & limit auth
	let opt = AuthLimit::try_from(&api.auth_limit)?.limit_opt(opt);
	let opt = opt.new_with_perms(false);
	let res = next.invoke(stk, ctx, &opt, None, vec![req.into()]).await?;
	let res: ApiResponse = res.try_into()?;
	Ok(Some(res))
}

/// Creates a closure that executes the final API action handler.
///
/// This closure is the end of the middleware chain and directly executes
/// the action expression with the request in the context.
fn create_final_action_closure(action_expr: Expr) -> Closure {
	Closure::Builtin(Arc::new(
		move |stk: &mut Stk,
		      ctx: &FrozenContext,
		      opt: &Options,
		      doc: Option<&CursorDoc>,
		      args: Any| {
			// Extract request argument
			let (FromPublic(req),): (FromPublic<ApiRequest>,) =
				match FromArgs::from_args("", args.0) {
					Ok(v) => v,
					Err(_e) => {
						return Box::pin(std::future::ready(Err(
							ApiError::FinalActionRequestParseFailure.into(),
						)));
					}
				};

			// Update context - use the parameters passed to the closure
			let mut ctx_isolated = Context::new_isolated(ctx);
			ctx_isolated.add_value("request", Arc::new(req.into()));
			let ctx_frozen = ctx_isolated.freeze();

			// Clone required values
			let action_expr = action_expr.clone();
			// Execute
			Box::pin(stk.run(async move |stk| {
				action_expr
					.compute(stk, &ctx_frozen, opt, doc)
					.await
					.catch_return()
					// Ensure that the next middleware receives a proper api response object
					.and_then(ApiResponse::try_from)
					.map(Value::from)
			}))
		},
	))
}

/// Creates a closure that executes a middleware function.
///
/// This closure wraps the next middleware/handler in the chain and calls
/// the middleware function with the request and next closure.
fn create_middleware_closure(def: MiddlewareDefinition, next: Closure) -> Closure {
	Closure::Builtin(Arc::new(
		move |stk: &mut Stk,
		      ctx: &FrozenContext,
		      opt: &Options,
		      doc: Option<&CursorDoc>,
		      args: Any| {
			// Clone required values
			let def = def.clone();

			// Extract request argument
			let (FromPublic(req),): (FromPublic<ApiRequest>,) =
				match FromArgs::from_args("", args.0) {
					Ok(v) => v,
					Err(_e) => {
						return Box::pin(std::future::ready(Err(
							ApiError::MiddlewareRequestParseFailure {
								middleware: def.name,
							}
							.into(),
						)));
					}
				};

			// Parse function name - use ctx parameter directly
			let function: crate::expr::Function =
				match function_with_capabilities(&def.name, ctx.get_capabilities().as_ref()) {
					Ok(f) => f.into(),
					Err(_e) => {
						return Box::pin(std::future::ready(Err(
							ApiError::MiddlewareFunctionNotFound {
								function: def.name.clone(),
							}
							.into(),
						)));
					}
				};

			// Prepare arguments to be passed
			let mut fn_args = vec![Value::from(req), Value::Closure(Box::new(next.clone()))];
			fn_args.extend(def.args);

			// Each middleware should execute in an isolated context to prevent cross-contamination
			// between middleware calls, besides passed context objects
			let ctx = Context::new_isolated(ctx).freeze();
			let opt = opt.clone();
			let doc = doc.cloned();
			Box::pin(stk.run(async move |stk| {
				function
					.compute(stk, &ctx, &opt, doc.as_ref(), fn_args)
					.await
					.catch_return()
					// Ensure that the next middleware receives a proper api response object
					.and_then(ApiResponse::try_from)
					.map(Value::from)
			}))
		},
	))
}
