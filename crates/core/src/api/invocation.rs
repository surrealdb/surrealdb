use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use http::HeaderMap;
use reblessive::TreeStack;
use reblessive::tree::Stk;

use super::body::ApiBody;
use super::context::InvocationContext;
use super::convert;
use super::middleware::invoke;
use super::response::{ApiResponse, ResponseInstruction};
use crate::catalog::{ApiDefinition, ApiMethod};
use crate::ctx::{Context, MutableContext};
use crate::dbs::{Options, Session};
use crate::expr::FlowResultExt as _;
use crate::kvs::{Datastore, Transaction};
use crate::val::{Object, Value};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiInvocation {
	pub params: Object,
	pub method: ApiMethod,
	pub query: BTreeMap<String, String>,
	pub headers: HeaderMap,
}

impl ApiInvocation {
	pub fn vars(self, body: Value) -> Result<Value> {
		let obj = map! {
			"params" => Value::from(self.params),
			"body" => body,
			"method" => self.method.to_string().into(),
			"query" => Value::Object(self.query.into()),
			"headers" => Value::Object(convert::headermap_to_object(self.headers)?),
		};

		Ok(obj.into())
	}

	pub async fn invoke_with_transaction(
		self,
		tx: Arc<Transaction>,
		ds: Arc<Datastore>,
		sess: &Session,
		api: &ApiDefinition,
		body: ApiBody,
	) -> Result<Option<(ApiResponse, ResponseInstruction)>> {
		let opt = ds.setup_options(sess);

		let mut ctx = ds.setup_ctx()?;
		ctx.set_transaction(tx);
		ctx.attach_session(sess)?;
		let ctx = &ctx.freeze();

		let mut stack = TreeStack::new();
		stack.enter(|stk| self.invoke_with_context(stk, ctx, &opt, api, body)).finish().await
	}

	// The `invoke` method accepting a parameter like `Option<&mut Stk>`
	// causes issues with axum, hence the separation
	pub async fn invoke_with_context(
		self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		api: &ApiDefinition,
		body: ApiBody,
	) -> Result<Option<(ApiResponse, ResponseInstruction)>> {
		// TODO: Figure out if it is possible if multiple actions can have the same
		// method, and if so should they all be run?
		let method_action = api.actions.iter().find(|x| x.methods.contains(&self.method));

		if method_action.is_none() && api.fallback.is_none() {
			// nothing to do, just return.
			return Ok(None);
		}

		let mut inv_ctx = InvocationContext::default();

		// first run the middleware which is globally configured for the database.
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let global = ctx.tx().get_db_optional_config(ns, db, "api").await?;
		if let Some(config) = global.as_ref().map(|v| v.try_as_api()).transpose()? {
			for m in config.middleware.iter() {
				invoke::invoke(&mut inv_ctx, &m.name, m.args.clone())?;
			}
		}

		// run the middleware for the api definition.
		for m in api.config.middleware.iter() {
			invoke::invoke(&mut inv_ctx, &m.name, m.args.clone())?;
		}

		// run the middleware for the http method.
		if let Some(method_action) = method_action {
			for m in method_action.config.middleware.iter() {
				invoke::invoke(&mut inv_ctx, &m.name, m.args.clone())?;
			}
		}

		// Prepare the response headers and conversion
		let res_instruction = if body.is_native() {
			ResponseInstruction::Native
		} else if inv_ctx.response_body_raw {
			ResponseInstruction::Raw
		} else {
			ResponseInstruction::for_format(&self)?
		};

		let body = body.process(&inv_ctx, &self).await?;

		// Edit the options
		let opt = opt.new_with_perms(false);

		// Edit the context
		let mut ctx = MutableContext::new_isolated(ctx);

		// Set the request variable
		let vars = self.vars(body)?;
		ctx.add_value("request", vars.into());

		// Possibly set the timeout
		if let Some(timeout) = inv_ctx.timeout {
			ctx.add_timeout(*timeout)?
		}

		// Freeze the context
		let ctx = ctx.freeze();

		// Compute the action

		let Some(action) = method_action.map(|x| &x.action).or(api.fallback.as_ref()) else {
			// condition already checked above.
			// either method_action is some or api fallback is some.
			unreachable!()
		};

		let res = stk.run(|stk| action.compute(stk, &ctx, &opt, None)).await.catch_return()?;

		let mut res = ApiResponse::from_action_result(res)?;
		if let Some(headers) = inv_ctx.response_headers {
			let mut headers = headers;
			headers.extend(res.headers);
			res.headers = headers;
		}

		Ok(Some((res, res_instruction)))
	}
}
