use crate::api::body::ApiBody;
use crate::api::method::Method;
use crate::api::path::Path;
use crate::ctx::{ContextIsolation, MutableContext};
use crate::dbs::{Options, Session};
use crate::err::Error;
use crate::iam::{Action, Level, ResourceKind, Role};
use crate::kvs::{Datastore, Transaction};
use crate::sql::fmt::{pretty_indent, Fmt};
use crate::sql::{Base, Object, Value};
use crate::ApiInvocation;
use crate::{ctx::Context, sql::statements::info::InfoStructure};
use derive::Store;
use reblessive::tree::Stk;
use reblessive::TreeStack;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::sync::Arc;

use super::config::api::{ApiConfig, MergedApiConfig};
use super::CursorDoc;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineApiStatement {
	pub id: Option<u32>,
	pub if_not_exists: bool,
	pub overwrite: bool,
	pub path: Path,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<Value>,
	pub config: Option<ApiConfig>,
}

impl DefineApiStatement {
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Api, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = (opt.ns()?, opt.db()?);
		// Check if the definition exists
		if txn.get_db_api(ns, db, &self.path.to_string()).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else if !self.overwrite {
				return Err(Error::ApAlreadyExists {
					value: self.path.to_string(),
				});
			}
		}
		// Process the statement
		let name = self.path.to_string();
		let key = crate::key::database::ap::new(ns, db, &name);
		txn.get_or_add_ns(ns, opt.strict).await?;
		txn.get_or_add_db(ns, db, opt.strict).await?;
		let ap = DefineApiStatement {
			// Don't persist the `IF NOT EXISTS` clause to schema
			if_not_exists: false,
			overwrite: false,
			..self.clone()
		};
		txn.set(key, ap, None).await?;
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}

	pub async fn invoke_with_transaction(
		&self,
		ns: String,
		db: String,
		tx: Arc<Transaction>,
		ds: Arc<Datastore>,
		invocation: ApiInvocation<'_>,
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
		stack.enter(|stk| self.invoke_with_context(stk, ctx, &opt, invocation, body)).finish().await
	}

	// The `invoke` method accepting a parameter like `Option<&mut Stk>`
	// causes issues with axum, hence the separation
	pub async fn invoke_with_context(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		invocation: ApiInvocation<'_>,
		body: ApiBody,
	) -> Result<Option<Value>, Error> {
		let (action, action_config) = match self.actions.iter().find(|x| x.methods.contains(&invocation.method)) {
			Some(v) => (&v.action, &v.config),
			None => match &self.fallback {
				Some(v) => (v, &None),
				None => return Ok(None),
			},
		};

		let config_stm = match ctx.tx().get_db_config(opt.ns()?, opt.db()?, "api").await {
			Ok(v) => Some(v.clone()),
			Err(Error::CgNotFound { .. }) => None,
			Err(e) => return Err(e),
		};

		let global = match &config_stm {
			Some(v) => Some(v.inner.try_into_api()?),
			None => None,
		};

		let config = MergedApiConfig {
			global,
			stmt: self.config.as_ref(),
			method: action_config.as_ref(),
		};

		let body = body.stream(config.max_body_size().cloned()).await?;

		println!("\nconfig: {:#?}\n", ApiConfig::from(config));

		let mut ctx = MutableContext::new_isolated(ctx, ContextIsolation::Full);
		ctx.add_value("request", Arc::new(invocation.vars(Value::from(body))));
		let ctx = ctx.freeze();

		let res = action.compute(stk, &ctx, opt, None).await?;
		Ok(Some(res))
	}
}

impl Display for DefineApiStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE API")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {}", self.path.to_url())?;
		let indent = pretty_indent();
		if let Some(config) = &self.config {
			write!(f, "{}", config)?;
		}

		if let Some(fallback) = &self.fallback {
			write!(f, "FOR any {}", fallback)?;
		}

		for action in &self.actions {
			write!(f, "{}", action)?;
		}

		drop(indent);
		Ok(())
	}
}

impl InfoStructure for DefineApiStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"path".to_string() => Value::from(self.path.to_string()),
		})
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ApiAction {
	pub methods: Vec<Method>,
	pub action: Value,
	pub config: Option<ApiConfig>,
}

impl Display for ApiAction {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FOR {}", Fmt::comma_separated(self.methods.iter()))?;
		let indent = pretty_indent();
		if let Some(config) = &self.config {
			write!(f, "{}", config)?;
		}
		write!(f, "THEN {}", self.action)?;
		drop(indent);
		Ok(())
	}
}

pub trait FindApi<'a> {
	fn find_api(&'a self, segments: Vec<&'a str>, method: Method) -> Option<(&'a DefineApiStatement, Object)>;
}

impl<'a> FindApi<'a> for &'a [DefineApiStatement] {
	fn find_api(&'a self, segments: Vec<&'a str>, method: Method) -> Option<(&'a DefineApiStatement, Object)> {
		let mut specifity = 0 as u8;
		let mut res = None;
		for api in self.iter() {
			if let Some(params) = api.path.fit(segments.as_slice()) {
				if api.fallback.is_some() || api.actions.iter().any(|x| x.methods.contains(&method)) {
					let s = api.path.specifity();
					if s > specifity {
						specifity = s;
						res = Some((api, params));
					}
				}
			}
		}

		res
	}
}

// /*bla - 1
// /:bla - 2
// /bla  - 3

// /*bla - 1
// /:bla - 2
// /bla  - 3