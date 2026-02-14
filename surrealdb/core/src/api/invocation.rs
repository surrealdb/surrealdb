use std::sync::Arc;

use anyhow::Result;
use http::HeaderValue;
use reblessive::TreeStack;
use reblessive::tree::Stk;
use tracing::{debug, error, trace};

use super::response::ApiResponse;
use crate::api::X_SURREAL_REQUEST_ID;
use crate::api::err::ApiError;
use crate::api::request::ApiRequest;
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{ApiDefinition, MiddlewareDefinition, Permission};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, FlowResultExt as _};
use crate::fnc::args::{Any, FromArgs, FromPublic};
use crate::iam::{Action, AuthLimit};
use crate::rpc::types_error_from_anyhow;
use crate::syn::function_with_capabilities;
use crate::val::{Closure, Value};

/// Processes an API request through the middleware chain and executes the handler.
///
/// This function orchestrates the entire API request lifecycle:
/// 1. Finds the appropriate handler based on HTTP method
/// 2. Collects middleware from database-level, route-level, and method-level configs
/// 3. Builds a middleware chain in execution order
/// 4. Executes the middleware chain with the final handler
/// 5. Returns an [`ApiResponse`]; no handler or permission denied yield 404/403 responses.
///
/// # Arguments
/// * `ctx` - The frozen context containing database and transaction information
/// * `opt` - Database options including permissions
/// * `api` - The API definition containing path, handlers, and middleware configuration
/// * `req` - The incoming API request with method, headers, body, params, etc.
///
/// # Returns
/// * `Ok(response)` - Processed request; includes 404/403 when no handler or permission denied
/// * `Err(e)` - Error during processing (e.g. middleware or handler failure)
pub async fn process_api_request(
	ctx: &FrozenContext,
	opt: &Options,
	api: &ApiDefinition,
	req: ApiRequest,
) -> Result<ApiResponse> {
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
/// * `Ok(response)` - Processed request; 404/403 when no handler or permission denied
/// * `Err(e)` - Error during processing (e.g. middleware or handler failure)
pub async fn process_api_request_with_stack(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	api: &ApiDefinition,
	req: ApiRequest,
) -> Result<ApiResponse> {
	// TODO: Figure out if it is possible if multiple actions can have the same
	// method, and if so should they all be run?
	let method_action = api.actions.iter().find(|x| x.methods.contains(&req.method));

	let (action_expr, method_config) = match (method_action, &api.fallback) {
		(Some(x), _) => (x.action.clone(), Some(&x.config)),
		(None, Some(x)) => (x.clone(), None),
		// nothing to do, just return
		_ => {
			trace!(
				request_id = %req.request_id,
				method = ?req.method,
				"No matching handler or fallback for API request"
			);
			let res = ApiResponse::from_error(
				ApiError::NotFound.into_types_error(),
				req.request_id.clone(),
			);
			return Ok(res);
		}
	};

	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let global_entry = ctx.tx().get_db_config(ns, db, "api").await?;
	let global = global_entry.as_ref().map(|v| v.try_as_api()).transpose()?;

	// Check permissions
	if opt.check_perms(Action::Edit)? {
		let permissions: Vec<&Permission> = method_config
			.map(|config| &config.permissions)
			.into_iter()
			.chain(std::iter::once(&api.config.permissions))
			.chain(global.as_ref().map(|config| &config.permissions))
			.collect();

		// Iterate through permissions and process them
		for permission in permissions {
			match permission {
				Permission::None => {
					trace!(
						request_id = %req.request_id,
						"API request denied by PERMISSIONS NONE"
					);
					let res = ApiResponse::from_error(
						ApiError::PermissionDenied.into_types_error(),
						req.request_id.clone(),
					);
					return Ok(res);
				}
				Permission::Full => (),
				Permission::Specific(e) => {
					// Disable permissions
					let opt = &opt.new_with_perms(false);
					// Process the PERMISSION clause
					if !stk
						.run(|stk| e.compute(stk, ctx, opt, None))
						.await
						.catch_return()?
						.is_truthy()
					{
						trace!(
							request_id = %req.request_id,
							"API request denied by PERMISSIONS WHERE clause"
						);
						let res = ApiResponse::from_error(
							ApiError::PermissionDenied.into_types_error(),
							req.request_id.clone(),
						);
						return Ok(res);
					}
				}
			}
		}
	}

	let middleware: Vec<_> = global
		.into_iter()
		.flat_map(|cfg| cfg.middleware.iter().cloned())
		.chain(api.config.middleware.iter().cloned())
		.chain(method_config.into_iter().flat_map(|config| config.middleware.iter().cloned()))
		.collect();

	// Create the final action closure (end of the middleware chain)
	let final_action = create_final_action_closure(req.request_id.clone(), action_expr);

	// Build the middleware chain backwards, wrapping each middleware around the previous closure
	let middleware_len = middleware.len();
	let next = middleware.iter().rev().enumerate().fold(final_action, |next, (idx, def)| {
		// is_initial is true for the first middleware in execution order (furthest from action)
		// When reversed, the last index is the first middleware
		let is_initial = idx == middleware_len.saturating_sub(1);
		create_middleware_closure(req.request_id.clone(), def.clone(), next, is_initial)
	});

	// APIs run without permissions & limit auth
	let opt = AuthLimit::try_from(&api.auth_limit)?.limit_opt(opt);
	let opt = opt.new_with_perms(false);

	debug!(
		request_id = %req.request_id,
		middleware_count = middleware.len(),
		"Executing API middleware chain"
	);
	let mut res: ApiResponse =
		next.invoke(stk, ctx, &opt, None, vec![req.into()]).await?.try_into()?;

	// Ensure X-Surreal-Request-ID is present in final response headers (from res.request_id)
	res.ensure_request_id_header();

	Ok(res)
}

/// Creates a closure that executes the final API action handler.
///
/// This closure is the end of the middleware chain and directly executes
/// the action expression with the request in the context.
///
/// # Arguments
/// * `action_expr` - The expression to execute as the final action
fn create_final_action_closure(request_id: String, action_expr: Expr) -> Closure {
	Closure::Builtin(Arc::new(
		move |stk: &mut Stk,
		      ctx: &FrozenContext,
		      opt: &Options,
		      doc: Option<&CursorDoc>,
		      args: Any| {
			// Extract request argument
			let (FromPublic(mut req),): (FromPublic<ApiRequest>,) =
				match FromArgs::from_args("", args.0) {
					Ok(v) => v,
					Err(_e) => {
						return Box::pin(std::future::ready(Err(
							ApiError::FinalActionRequestParseFailure.into(),
						)));
					}
				};

			// Enforce request ID in request headers & object (prevent user modification)
			req.request_id.clone_from(&request_id);
			if !request_id.is_empty() {
				let _ = req.headers.insert(
					X_SURREAL_REQUEST_ID,
					HeaderValue::from_str(&request_id)
						.unwrap_or_else(|_| HeaderValue::from_static("unknown")),
				);
			}

			// Update context
			let mut ctx_isolated = Context::new_isolated(ctx);
			ctx_isolated.add_value("request", Arc::new(req.into()));
			let ctx_frozen = ctx_isolated.freeze();

			// Clone required values
			let action_expr = action_expr.clone();
			let request_id = request_id.clone();
			// Execute
			Box::pin(stk.run(async move |stk| {
				// Computed result
				let res = action_expr.compute(stk, &ctx_frozen, opt, doc).await.catch_return();

				// Convert to ApiResponse; set request_id from request for all responses
				let mut res = match res {
					Ok(res) => ApiResponse::try_from(res)
						.unwrap_or_else(|e| ApiResponse::from_error(e, request_id.clone())),
					Err(e) => {
						ApiResponse::from_error(types_error_from_anyhow(e), request_id.clone())
					}
				};
				res.request_id.clone_from(&request_id);
				res.ensure_request_id_header();

				Ok(Value::from(res))
			}))
		},
	))
}

/// Creates a closure that executes a middleware function.
///
/// This closure wraps the next middleware/handler in the chain and calls
/// the middleware function with the request and next closure.
///
/// # Arguments
/// * `def` - The middleware definition
/// * `next` - The next closure in the chain
/// * `is_initial` - Whether this is the initial middleware (furthest from action)
fn create_middleware_closure(
	request_id: String,
	def: MiddlewareDefinition,
	next: Closure,
	is_initial: bool,
) -> Closure {
	Closure::Builtin(Arc::new(
		move |stk: &mut Stk,
		      ctx: &FrozenContext,
		      opt: &Options,
		      doc: Option<&CursorDoc>,
		      args: Any| {
			// Clone required values
			let def = def.clone();

			// Extract request argument
			let (FromPublic(mut req),): (FromPublic<ApiRequest>,) =
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

			// Enforce request ID in request headers & object (prevent user modification)
			req.request_id.clone_from(&request_id);
			if !request_id.is_empty() {
				let _ = req.headers.insert(
					X_SURREAL_REQUEST_ID,
					HeaderValue::from_str(&request_id)
						.unwrap_or_else(|_| HeaderValue::from_static("unknown")),
				);
			}

			// Prepare arguments to be passed
			let mut fn_args = vec![Value::from(req), Value::Closure(Box::new(next.clone()))];
			fn_args.extend(def.args);

			// Each middleware should execute in an isolated context to prevent cross-contamination
			let ctx = Context::new_isolated(ctx).freeze();
			let opt = opt.clone();
			let doc = doc.cloned();
			let middleware_name = def.name;
			let request_id = request_id.clone();

			Box::pin(stk.run(async move |stk| {
				// Computed result
				let res =
					function.compute(stk, &ctx, &opt, doc.as_ref(), fn_args).await.catch_return();

				let mut res = match res {
					Ok(res) => match ApiResponse::try_from(res) {
						Ok(mut res) => {
							res.request_id.clone_from(&request_id);
							res
						}
						Err(e) => {
							if is_initial {
								error!(
									request_id = %request_id,
									middleware = %middleware_name,
									error = %e,
									"API middleware error; converting to response (ApiError exposed, internal errors masked)"
								);
								ApiResponse::from_error_secure(e, request_id.clone())
							} else {
								ApiResponse::from_error(e, request_id.clone())
							}
						}
					},
					Err(e) => {
						let types_err = types_error_from_anyhow(e);
						if is_initial {
							error!(
								request_id = %request_id,
								middleware = %middleware_name,
								error = %types_err,
								"API middleware error; converting to response (ApiError exposed, internal errors masked)"
							);
							ApiResponse::from_error_secure(types_err, request_id.clone())
						} else {
							ApiResponse::from_error(types_err, request_id.clone())
						}
					}
				};

				res.ensure_request_id_header();
				Ok(Value::from(res))
			}))
		},
	))
}
