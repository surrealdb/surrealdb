pub mod api;
pub mod graphql;

use std::fmt::{self, Display};

use anyhow::{Result, bail};
use api::{ApiConfig, ApiConfigStore};
use graphql::GraphQLConfig;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Base, Value};
use crate::iam::{Action, ConfigKind, ResourceKind};
use crate::kvs::impl_kv_value_revisioned;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct DefineConfigStatement {
	pub kind: DefineKind,
	pub inner: ConfigInner,
}

/// The config struct as a computation target.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum ConfigInner {
	GraphQL(GraphQLConfig),
	Api(ApiConfig),
}

/// The config struct as it is stored on disk.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum ConfigStore {
	GraphQL(GraphQLConfig),
	Api(ApiConfigStore),
}
impl_kv_value_revisioned!(ConfigStore);

impl DefineConfigStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
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
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		if txn.get_db_config(ns, db, cg).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::CgAlreadyExists {
							name: cg.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}
		}

		let store = match &self.inner {
			ConfigInner::GraphQL(g) => ConfigStore::GraphQL(g.clone()),
			ConfigInner::Api(a) => ConfigStore::Api(a.compute(stk, ctx, opt, doc).await?),
		};

		// Process the statement
		let key = crate::key::database::cg::new(ns, db, cg);
		txn.replace(&key, &store).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl ConfigStore {
	pub fn name(&self) -> String {
		match self {
			ConfigStore::GraphQL(_) => ConfigKind::GraphQL.to_string(),
			ConfigStore::Api(_) => ConfigKind::Api.to_string(),
		}
	}

	pub fn try_into_graphql(self) -> Result<GraphQLConfig> {
		match self {
			ConfigStore::GraphQL(g) => Ok(g),
			c => fail!("found {c} when a graphql config was expected"),
		}
	}

	pub fn try_as_api(&self) -> Result<&ApiConfigStore> {
		match self {
			ConfigStore::Api(a) => Ok(a),
			c => fail!("found {c} when a api config was expected"),
		}
	}
}

/*
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
}*/

impl InfoStructure for ConfigStore {
	fn structure(self) -> Value {
		match self {
			ConfigStore::GraphQL(v) => Value::from(map!(
				"graphql" => v.structure()
			)),
			ConfigStore::Api(v) => Value::from(map!(
				"api" => v.structure()
			)),
		}
	}
}

impl Display for DefineConfigStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE CONFIG")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
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

impl Display for ConfigStore {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match &self {
			ConfigStore::GraphQL(v) => Display::fmt(v, f),
			ConfigStore::Api(v) => Display::fmt(v, f),
		}
	}
}
