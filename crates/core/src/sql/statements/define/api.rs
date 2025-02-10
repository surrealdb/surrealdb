use crate::api::method::Method;
use crate::api::path::Path;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::{pretty_indent, Fmt};
use crate::sql::{Base, Object, Value};
use crate::{ctx::Context, sql::statements::info::InfoStructure};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

use super::config::api::ApiConfig;
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
	fn find_api(
		&'a self,
		segments: Vec<&'a str>,
		method: Method,
	) -> Option<(&'a DefineApiStatement, Object)>;
}

impl<'a> FindApi<'a> for &'a [DefineApiStatement] {
	fn find_api(
		&'a self,
		segments: Vec<&'a str>,
		method: Method,
	) -> Option<(&'a DefineApiStatement, Object)> {
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

// /*bla - 1
// /:bla - 2
// /bla  - 3

// /*bla - 1
// /:bla - 2
// /bla  - 3
