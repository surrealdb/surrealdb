use crate::api::method::Method;
use crate::api::path::Path;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::sql::fmt::{pretty_indent, Fmt};
use crate::sql::{Base, Object, Value};
use crate::{ctx::Context, sql::statements::info::InfoStructure};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

use super::config::api::ApiConfig;
use super::CursorDoc;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineApiStatement {
	pub if_not_exists: bool,
	pub overwrite: bool,
	pub path: Value,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<Value>,
	pub config: Option<ApiConfig>,
}

impl DefineApiStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
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
		let path: Path =
			self.path.compute(stk, ctx, opt, doc).await?.coerce_to_string()?.parse()?;
		let name = path.to_string();
		let key = crate::key::database::ap::new(ns, db, &name);
		txn.get_or_add_ns(ns, opt.strict).await?;
		txn.get_or_add_db(ns, db, opt.strict).await?;
		let ap = ApiDefinition {
			// Don't persist the `IF NOT EXISTS` clause to schema
			path,
			actions: self.actions.clone(),
			fallback: self.fallback.clone(),
			config: self.config.clone(),
			auth_limit: AuthLimit::new_from_auth(opt.auth.as_ref()),
			..Default::default()
		};
		txn.set(key, revision::to_vec(&ap)?, None).await?;
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
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
		write!(f, " {}", self.path)?;
		let indent = pretty_indent();

		if self.config.is_some() || self.fallback.is_some() {
			write!(f, "FOR any")?;
			let indent = pretty_indent();

			if let Some(config) = &self.config {
				write!(f, "{}", config)?;
			}

			if let Some(fallback) = &self.fallback {
				write!(f, "THEN {}", fallback)?;
			}

			drop(indent);
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
			"path".to_string() => self.path,
			"config".to_string(), if let Some(config) = self.config => config.structure(),
			"fallback".to_string(), if let Some(fallback) = self.fallback => fallback.structure(),
			"actions".to_string() => Value::from(self.actions.into_iter().map(InfoStructure::structure).collect::<Vec<Value>>()),
		})
	}
}

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[non_exhaustive]
pub struct ApiDefinition {
	pub id: Option<u32>,
	pub path: Path,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<Value>,
	pub config: Option<ApiConfig>,
	#[revision(start = 2, default_fn = "existing_auth_limit")]
	pub auth_limit: AuthLimit,
}

impl ApiDefinition {
	fn existing_auth_limit(_revision: u16) -> Result<AuthLimit, revision::Error> {
		Ok(AuthLimit::new_no_limit())
	}
}

impl From<ApiDefinition> for DefineApiStatement {
	fn from(value: ApiDefinition) -> Self {
		DefineApiStatement {
			if_not_exists: false,
			overwrite: false,
			path: value.path.to_string().into(),
			actions: value.actions,
			fallback: value.fallback,
			config: value.config,
		}
	}
}

impl InfoStructure for ApiDefinition {
	fn structure(self) -> Value {
		let da: DefineApiStatement = self.into();
		da.structure()
	}
}

impl Display for ApiDefinition {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let da: DefineApiStatement = self.clone().into();
		da.fmt(f)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
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

impl InfoStructure for ApiAction {
	fn structure(self) -> Value {
		Value::from(map!(
			"methods" => Value::from(self.methods.into_iter().map(InfoStructure::structure).collect::<Vec<Value>>()),
			"action" => Value::from(self.action.to_string()),
			"config", if let Some(config) = self.config => config.structure(),
		))
	}
}

pub trait FindApi<'a> {
	fn find_api(
		&'a self,
		segments: Vec<&'a str>,
		method: Method,
	) -> Option<(&'a ApiDefinition, Object)>;
}

impl<'a> FindApi<'a> for &'a [ApiDefinition] {
	fn find_api(
		&'a self,
		segments: Vec<&'a str>,
		method: Method,
	) -> Option<(&'a ApiDefinition, Object)> {
		let mut specifity = 0_u8;
		let mut res = None;
		for api in self.iter() {
			if let Some(params) = api.path.fit(segments.as_slice()) {
				if api.fallback.is_some() || api.actions.iter().any(|x| x.methods.contains(&method))
				{
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
