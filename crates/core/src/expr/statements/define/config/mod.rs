pub mod api;

use std::fmt::{self, Display};

use anyhow::{Result, bail};
use api::ApiConfig;
use reblessive::tree::Stk;

use crate::catalog::{ConfigDefinition, GraphQLConfig};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::statements::define::DefineKind;
use crate::expr::{Base, Value};
use crate::iam::{Action, ConfigKind, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineConfigStatement {
	pub kind: DefineKind,
	pub inner: ConfigInner,
}

/// The config struct as a computation target.

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ConfigInner {
	GraphQL(GraphQLConfig),
	Api(ApiConfig),
}

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
			ConfigInner::GraphQL(g) => ConfigDefinition::GraphQL(g.clone()),
			ConfigInner::Api(a) => ConfigDefinition::Api(a.compute(stk, ctx, opt, doc).await?),
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
