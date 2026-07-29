use anyhow::Result;

use crate::catalog::Error;
use crate::catalog::base::Base;
use crate::catalog::providers::{DatabaseProvider, RootProvider};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::Value;
use crate::iam::{Action, ConfigKind, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveConfigStatement {
	pub kind: ConfigKind,
	pub if_exists: bool,
}

impl RemoveConfigStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &FrozenContext, opt: &Options) -> Result<Value> {
		let base = crate::expr::statements::define::config::config_kind_base(&self.kind);
		// Allowed to run?
		ctx.is_allowed(
			opt,
			Action::Edit,
			ResourceKind::Config(self.kind.clone()),
			base.clone().into(),
		)?;
		let cg = match &self.kind {
			ConfigKind::GraphQL => "graphql",
			ConfigKind::Api => "api",
			ConfigKind::Default => "default",
		};
		let txn = ctx.tx();
		match base {
			Base::Root => {
				if txn.get_root_config(cg).await?.is_none() {
					if self.if_exists {
						return Ok(Value::None);
					} else {
						return Err(Error::CgNotFound {
							name: cg.to_string(),
						}
						.into());
					}
				}
				let key = crate::key::root::root_config::RootConfig {
					ty: std::borrow::Cow::Borrowed(cg),
				};
				txn.del_key(&key).await?;
			}
			Base::Db => {
				let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
				if txn.get_db_config(ns, db, cg, None).await?.is_none() {
					if self.if_exists {
						return Ok(Value::None);
					} else {
						return Err(Error::CgNotFound {
							name: cg.to_string(),
						}
						.into());
					}
				}
				let key = crate::key::database::cg::Config {
					prefix: crate::key::database::all::DatabaseRoot {
						ns,
						db,
					},
					ty: std::borrow::Cow::Borrowed(cg),
				};
				txn.del_key(&key).await?;
			}
			Base::Ns => {
				fail!("config on namespace scope is not supported");
			}
		}
		txn.clear_cache();
		Ok(Value::None)
	}
}
