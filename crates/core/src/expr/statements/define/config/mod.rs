pub mod api;
pub mod defaults;

use std::fmt::{self, Display};

use anyhow::{Result, bail};
use api::ApiConfig;
use defaults::DefaultConfig;
use reblessive::tree::Stk;

use crate::catalog::base::Base;
use crate::catalog::providers::{DatabaseProvider, RootProvider};
use crate::catalog::{ConfigDefinition, GraphQLConfig};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::Value;
use crate::expr::statements::define::DefineKind;
use crate::iam::{Action, ConfigKind, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineConfigStatement {
	pub kind: DefineKind,
	pub inner: ConfigInner,
}

/// The config struct as a computation target.

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum ConfigInner {
	GraphQL(GraphQLConfig),
	Api(ApiConfig),
	Default(DefaultConfig),
}

impl ConfigInner {
	pub(crate) fn kind(&self) -> ConfigKind {
		match self {
			ConfigInner::Default(_) => ConfigKind::Default,
			ConfigInner::GraphQL(_) => ConfigKind::GraphQL,
			ConfigInner::Api(_) => ConfigKind::Api,
		}
	}

	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<ConfigDefinition> {
		Ok(match self {
			ConfigInner::GraphQL(g) => ConfigDefinition::GraphQL(g.clone()),
			ConfigInner::Api(a) => ConfigDefinition::Api(a.compute(stk, ctx, opt, doc).await?),
			ConfigInner::Default(d) => {
				ConfigDefinition::Default(d.compute(stk, ctx, opt, doc).await?)
			}
		})
	}
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
		let kind = self.inner.kind();
		let base = kind.base();
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Config(kind), &base.clone().into())?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Get the config kind
		let cg = match &self.inner {
			ConfigInner::GraphQL(_) => "graphql",
			ConfigInner::Api(_) => "api",
			ConfigInner::Default(_) => "default",
		};

		match base {
			Base::Root => {
				if txn.expect_root_config(cg).await.is_ok() {
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

				// Compute the config
				let key = crate::key::root::cg::new(cg);
				let store = self.inner.compute(stk, ctx, opt, doc).await?;
				// Put the config
				txn.replace(&key, &store).await?;
				// Clear the cache
				txn.clear_cache();
			}
			Base::Ns => {
				fail!("defining config on a namespace is not supported");
			}
			Base::Db => {
				// Check if the definition exists
				let (ns, db) = ctx.get_ns_db_ids(opt).await?;
				if txn.expect_db_config(ns, db, cg).await.is_ok() {
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

				// Compute the config
				let key = crate::key::database::cg::new(ns, db, cg);
				let store = self.inner.compute(stk, ctx, opt, doc).await?;
				// Put the config
				txn.replace(&key, &store).await?;
				// Clear the cache
				txn.clear_cache();
			}
		}
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
		write!(f, " {}", self.inner)?;

		Ok(())
	}
}

impl Display for ConfigInner {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match &self {
			ConfigInner::GraphQL(v) => Display::fmt(v, f),
			ConfigInner::Default(v) => Display::fmt(v, f),
			ConfigInner::Api(v) => {
				write!(f, "API")?;
				Display::fmt(v, f)
			}
		}
	}
}
