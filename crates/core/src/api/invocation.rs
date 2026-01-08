use std::sync::Arc;
use anyhow::Result;
use reblessive::TreeStack;
use reblessive::tree::Stk;

use super::response::ApiResponse;
use crate::api::request::ApiRequest;
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::ApiDefinition;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::FlowResultExt as _;
use crate::fnc::args::{Any, FromArgs, FromPublic};
use crate::syn::function_with_capabilities;
use crate::val::{Closure, Value};

pub async fn process_api_request(
	ctx: &FrozenContext,
	opt: &Options,
	api: &ApiDefinition,
	req: ApiRequest,
) -> Result<Option<ApiResponse>> {
	let mut stack = TreeStack::new();
	stack.enter(|stk| process_api_request_with_stack(stk, ctx, opt, api, req)).finish().await
}

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
		.chain(
			method_config
				.into_iter()
				.flat_map(|config| config.middleware.iter().cloned()),
		)
		.collect();

	// Create the final action closure (end of the middleware chain)
	let final_action = Closure::Builtin(Arc::new(move |stk: &mut Stk, ctx: &FrozenContext, opt: &Options, _doc: Option<&CursorDoc>, args: Any| {
		// Clone required values
		let action_expr = action_expr.clone();

		// Extract request argument
		let (FromPublic(req),): (FromPublic<ApiRequest>,) = match FromArgs::from_args("", args.0) {
			Ok(v) => v,
			Err(e) => return Box::pin(std::future::ready(Err(e))),
		};
		
		// Update options and context
		let opt = opt.new_with_perms(false);
		let mut ctx_isolated = Context::new_isolated(ctx);
		ctx_isolated.add_value("request", Arc::new(req.into()));
		let ctx_frozen = ctx_isolated.freeze();

		// Execute
		Box::pin(async {
			stk.run(move |stk| {
				let ctx_frozen = ctx_frozen.clone();
				let opt = opt.clone();
				async move {
					action_expr.compute(stk, &ctx_frozen, &opt, None).await
				}
			})
				.await
				.catch_return()
		})
	}));

	// Build the middleware chain backwards, wrapping each middleware around the previous closure
	let next = middleware.iter().rev().fold(final_action, |next, def| {
		let def = def.clone();
		Closure::Builtin(Arc::new(move |stk: &mut Stk, ctx: &FrozenContext, opt: &Options, doc: Option<&CursorDoc>, args: Any| {
			// Clone required values
			let def = def.clone();
			let ctx = ctx.clone();
			let opt = opt.clone();
			let doc = doc.cloned();
			
			// Extract request argument
			let (FromPublic(req),): (FromPublic<ApiRequest>,) = match FromArgs::from_args("", args.0) {
				Ok(v) => v,
				Err(e) => return Box::pin(std::future::ready(Err(e))),
			};

			// Parse function name
			let function: crate::expr::Function = match function_with_capabilities(&def.name, ctx.get_capabilities().as_ref()) {
				Ok(f) => f.into(),
				Err(e) => return Box::pin(std::future::ready(Err(e))),
			};

			// Prepare arguments to be passed
			let mut fn_args = vec![
				Value::from(req),
				Value::Closure(Box::new(next.clone())),
			];
			fn_args.extend(def.args);

			// Execute
			Box::pin(async {
				stk.run(move |stk| async move {
					function.compute(stk, &ctx, &opt, doc.as_ref(), fn_args).await
				})
					.await
					.catch_return()
			})
		}))
	});

	let res = next.invoke(stk, ctx, opt, None, vec![req.into()]).await?;
	let res: ApiResponse = res.try_into()?;
	Ok(Some(res))
}