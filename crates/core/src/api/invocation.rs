use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use http::HeaderMap;
use reblessive::TreeStack;
use reblessive::tree::Stk;

use super::body::ApiBody;
use super::response::ApiResponse;
use crate::api::request::ApiRequest;
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{ApiDefinition, ApiMethod};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::FlowResultExt as _;
use crate::fnc::args::{Any, FromArgs, FromPublic};
use crate::syn::function_with_capabilities;
use crate::types::PublicObject;
use crate::val::{Closure, Value};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiInvocation {
	pub params: PublicObject,
	pub method: ApiMethod,
	pub query: BTreeMap<String, String>,
	pub headers: HeaderMap,
}

impl ApiInvocation {
	pub async fn invoke_with_transaction(
		self,
		ctx: &FrozenContext,
		opt: &Options,
		api: &ApiDefinition,
		body: ApiBody,
	) -> Result<Option<ApiResponse>> {
		let mut stack = TreeStack::new();
		stack.enter(|stk| self.invoke_with_context(stk, ctx, opt, api, body)).finish().await
	}

	// The `invoke` method accepting a parameter like `Option<&mut Stk>`
	// causes issues with axum, hence the separation
	pub async fn invoke_with_context(
		self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		api: &ApiDefinition,
		body: ApiBody,
	) -> Result<Option<ApiResponse>> {
		// TODO: Figure out if it is possible if multiple actions can have the same
		// method, and if so should they all be run?
		let method_action = api.actions.iter().find(|x| x.methods.contains(&self.method));

		if method_action.is_none() && api.fallback.is_none() {
			// nothing to do, just return.
			return Ok(None);
		}

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
				method_action
					.into_iter()
					.flat_map(|ma| ma.config.middleware.iter().cloned()),
			)
			.collect();

		// Create the final action closure (end of the middleware chain)
		let action_expr = method_action.map(|x| x.action.clone()).or_else(|| api.fallback.clone());
		let final_action: Closure = {
			let action_expr = action_expr.clone();
			let logic: std::sync::Arc<dyn for<'a> Fn(&'a mut Stk, &'a FrozenContext, &'a Options, Option<&'a CursorDoc>, Any) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value>> + 'a>> + Send + Sync> = std::sync::Arc::new(move |stk: &mut Stk, ctx: &FrozenContext, opt: &Options, _doc: Option<&CursorDoc>, args: Any| {
				let action_expr = action_expr.clone();
				// Extract and prepare data outside async block
				let (FromPublic(req),): (FromPublic<ApiRequest>,) = match FromArgs::from_args("", args.0) {
					Ok(v) => v,
					Err(e) => return Box::pin(std::future::ready(Err(e))),
				};
				
				let opt = opt.new_with_perms(false);
				let mut ctx_isolated = Context::new_isolated(ctx);
				ctx_isolated.add_value("request", Arc::new(req.into()));
				let ctx_frozen = ctx_isolated.freeze();

			let Some(action) = action_expr else {
				// condition already checked above.
				// either method_action is some or api fallback is some.
				return Box::pin(std::future::ready(Err(anyhow::anyhow!("No action found"))));
			};

			Box::pin(async {
				let result = stk.run(move |stk| {
					let ctx_frozen = ctx_frozen.clone();
					let opt = opt.clone();
					let action = action.clone();
					async move {
						action.compute(stk, &ctx_frozen, &opt, None).await
					}
				}).await;
				result.catch_return()
			})
			});
			Closure::Builtin(logic)
		};

		// Build the middleware chain backwards, wrapping each middleware around the previous closure
		let next = middleware.iter().rev().fold(final_action, |next_closure, def| {
			let def = def.clone();
			let logic: std::sync::Arc<dyn for<'a> Fn(&'a mut Stk, &'a FrozenContext, &'a Options, Option<&'a CursorDoc>, Any) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value>> + 'a>> + Send + Sync> = std::sync::Arc::new(move |stk: &mut Stk, ctx: &FrozenContext, opt: &Options, doc: Option<&CursorDoc>, args: Any| {
				let def = def.clone();
				let next_closure = next_closure.clone();
				let ctx = ctx.clone();
				let opt = opt.clone();
				// Extract and prepare data
				let (FromPublic(req),): (FromPublic<ApiRequest>,) = match FromArgs::from_args("", args.0) {
					Ok(v) => v,
					Err(e) => return Box::pin(std::future::ready(Err(e))),
				};
				println!("def.name {}", def.name);
				let function: crate::expr::Function = match function_with_capabilities(&def.name, ctx.get_capabilities().as_ref()) {
					Ok(f) => f.into(),
					Err(e) => return Box::pin(std::future::ready(Err(e))),
				};
				let mut fn_args = vec![
					Value::from(req),
					Value::Closure(Box::new(next_closure)),
				];
				fn_args.extend(def.args);
				let doc = doc.cloned();
				Box::pin(async {
					let result = stk.run(move |stk| async move {
						function.compute(stk, &ctx, &opt, doc.as_ref(), fn_args).await
					}).await;
					result.catch_return()
				})
			});
			Closure::Builtin(logic)
		});

		let req = ApiRequest {
			body: body.process().await?,
			headers: self.headers,
			params: self.params,
			method: self.method,
			query: self.query,
		};

		let res = next.invoke(stk, ctx, opt, None, vec![req.into()]).await?;
		let res: ApiResponse = res.try_into()?;
		Ok(Some(res))
	}
}
