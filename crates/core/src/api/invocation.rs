use std::sync::Arc;

use reblessive::{tree::Stk, TreeStack};

use super::{context::RequestContext, method::Method, middleware::CollectMiddleware};
use crate::{
	api::middleware::RequestMiddleware,
	ctx::{Context, ContextIsolation, MutableContext},
	dbs::{Options, Session},
	err::Error,
	iam::{Level, Role},
	kvs::{Datastore, Transaction},
	sql::{
		statements::{define::config::api::ApiConfig, DefineApiStatement},
		Bytes, Object, Value,
	},
	ApiBody,
};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ApiInvocation<'a> {
	pub params: Object,
	pub method: Method,
	pub query: Object,
	pub headers: Object,
	pub session: Option<Session>,
	pub values: Vec<(&'a str, Value)>,
}

impl<'a> ApiInvocation<'a> {
	pub fn vars(self, body: Value) -> Value {
		let mut obj = map! {
			"params" => Value::from(self.params),
			"body" => body,
			"method" => self.method.to_string().into(),
			"query" => Value::from(self.query),
			"headers" => Value::from(self.headers),
		};

		if let Some(session) = self.session {
			obj.extend(session.values().into_iter());
		}

		obj.extend(self.values.into_iter());

		obj.into()
	}

	pub async fn invoke_with_transaction(
		self,
		ns: String,
		db: String,
		tx: Arc<Transaction>,
		ds: Arc<Datastore>,
		api: &DefineApiStatement,
		body: ApiBody,
	) -> Result<Option<Value>, Error> {
		let sess = Session::for_level(Level::Database(ns.clone(), db.clone()), Role::Owner)
			.with_ns(&ns)
			.with_db(&db);
		let opt = ds.setup_options(&sess);

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
		api: &DefineApiStatement,
		body: ApiBody,
	) -> Result<Option<Value>, Error> {
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

		let mut req_ctx = RequestContext::default();
		req_ctx.apply_middleware(builtin)?;

		println!("req_ctx: {:#?}", req_ctx);

		let body = body.stream(req_ctx.max_body_size).await?;

		let mut ctx = MutableContext::new_isolated(ctx, ContextIsolation::Full);
		let vars = self.vars(Value::Bytes(Bytes(body)));
		ctx.add_value("request", vars.into());
		let ctx = ctx.freeze();

		let res = action.compute(stk, &ctx, opt, None).await?;
		Ok(Some(res))
	}
}
