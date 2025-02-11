pub mod api;
pub mod graphql;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ConfigKind, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Base, Value};

use api::ApiConfig;
use graphql::GraphQLConfig;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineConfigStatement {
	pub inner: ConfigInner,
	pub if_not_exists: bool,
	pub overwrite: bool,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum ConfigInner {
	GraphQL(GraphQLConfig),
	Api(ApiConfig),
}

impl DefineConfigStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Config(ConfigKind::GraphQL), &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Get the config kind
		let cg = match &self.inner {
			ConfigInner::GraphQL(_) => "graphql",
			ConfigInner::Api(_) => "api",
		};
		// Check if the definition exists
		let (ns, db) = opt.ns_db()?;
		if txn.get_db_config(ns, db, cg).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else if !self.overwrite {
				return Err(Error::CgAlreadyExists {
					name: cg.to_string(),
				});
			}
		}
		// Process the statement
		let key = crate::key::database::cg::new(ns, db, cg);
		txn.get_or_add_ns(ns, opt.strict).await?;
		txn.get_or_add_db(ns, db, opt.strict).await?;
		txn.replace(key, revision::to_vec(self)?).await?;
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl ConfigInner {
	pub fn name(&self) -> String {
		ConfigKind::from(self).to_string()
	}

	pub fn try_into_graphql(self) -> Result<GraphQLConfig, Error> {
		match self {
			ConfigInner::GraphQL(g) => Ok(g),
			c => Err(fail!("found {c} when a graphql config was expected")),
		}
	}

	pub fn try_into_api(&self) -> Result<&ApiConfig, Error> {
		match self {
			ConfigInner::Api(a) => Ok(a),
			c => Err(fail!("found {c} when a api config was expected")),
		}
	}
}

impl From<ConfigInner> for ConfigKind {
	fn from(value: ConfigInner) -> Self {
		(&value).into()
	}
}

impl From<&ConfigInner> for ConfigKind {
	fn from(value: &ConfigInner) -> Self {
		match value {
			ConfigInner::GraphQL(_) => ConfigKind::GraphQL,
			ConfigInner::Api(_) => ConfigKind::Api,
		}
	}
}

impl InfoStructure for DefineConfigStatement {
	fn structure(self) -> Value {
		match self.inner {
			ConfigInner::GraphQL(v) => Value::from(map!(
				"graphql" => v.structure()
			)),
			ConfigInner::Api(v) => Value::from(map!(
				"api" => v.structure()
			)),
		}
	}
}

impl Display for DefineConfigStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE CONFIG")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}

		write!(f, "{}", self.inner)?;

		Ok(())
	}
}

impl Display for ConfigInner {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match &self {
			ConfigInner::GraphQL(v) => Display::fmt(v, f),
			ConfigInner::Api(v) => Display::fmt(v, f),
		}
	}
}
