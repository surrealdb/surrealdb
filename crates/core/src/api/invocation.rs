use std::{collections::BTreeMap, sync::Arc};

use http::HeaderMap;
use reblessive::{tree::Stk, TreeStack};

use super::{
	body::ApiBody,
	context::InvocationContext,
	method::Method,
	middleware::CollectMiddleware,
	response::{ApiResponse, ResponseInstruction},
};
use crate::{
	api::middleware::RequestMiddleware,
	ctx::{Context, MutableContext},
	dbs::{Options, Session},
	err::Error,
	kvs::{Datastore, Transaction},
	sql::{
		statements::{define::config::api::ApiConfig, define::ApiDefinition},
		Object, Value,
	},
};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ApiInvocation {
	pub params: Object,
	pub method: Method,
	pub query: BTreeMap<String, String>,
	#[cfg_attr(feature = "arbitrary", arbitrary(value = HeaderMap::new()))]
	pub headers: HeaderMap,
}

impl ApiInvocation {
	pub fn vars(self, body: Value) -> Result<Value, Error> {
		let obj = map! {
			"params" => Value::from(self.params),
			"body" => body,
			"method" => self.method.to_string().into(),
			"query" => Value::Object(self.query.into()),
			"headers" => Value::Object(self.headers.try_into()?),
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
	) -> Result<Option<(ApiResponse, ResponseInstruction)>, Error> {
		let opt = ds.setup_options(sess);

		let mut ctx = ds.setup_ctx()?;
		ctx.set_transaction(tx);
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
	) -> Result<Option<(ApiResponse, ResponseInstruction)>, Error> {
		let (action, action_config) =
			match api.actions.iter().find(|x| x.methods.contains(&self.method)) {
				Some(v) => (&v.action, &v.config),
				None => match &api.fallback {
					Some(v) => (v, &None),
					None => return Ok(None),
				},
			};

		let mut configs: Vec<&ApiConfig> = Vec::new();
		let global = ctx.tx().get_db_optional_config(opt.ns()?, opt.db()?, "api").await?;
		configs.extend(global.as_ref().map(|v| v.inner.try_into_api()).transpose()?);
		configs.extend(api.config.as_ref());
		configs.extend(action_config);

		let middleware: Vec<&RequestMiddleware> =
			configs.into_iter().filter_map(|v| v.middleware.as_ref()).collect();
		let builtin = middleware.collect()?;

		let mut inv_ctx = InvocationContext::default();
		inv_ctx.apply_middleware(builtin)?;

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

		let res = action.compute(stk, &ctx, &opt, None).await?;

		let mut res = ApiResponse::try_from(res)?;
		if let Some(headers) = inv_ctx.response_headers {
			let mut headers = headers;
			headers.extend(res.headers);
			res.headers = headers;
		}

		Ok(Some((res, res_instruction)))
	}
}
