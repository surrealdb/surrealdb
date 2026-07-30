pub(crate) mod api;
pub(crate) mod defaults;

use std::borrow::Cow;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog::base::Base;
use crate::catalog::providers::{DatabaseProvider, RootProvider};
use crate::catalog::{ConfigDefinition, Error};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::config::{ConfigInner, DefineConfigStatement};
use crate::iam::{Action, ConfigKind, ResourceKind};
use crate::key::database::all::DatabaseRoot;
use crate::val::Value;

pub(crate) async fn config_inner_compute(
	this: &ConfigInner,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<ConfigDefinition> {
	Ok(match this {
		ConfigInner::GraphQL(g) => ConfigDefinition::GraphQL(g.clone()),
		ConfigInner::Api(a) => {
			ConfigDefinition::Api(crate::legacy::api_config_compute(a, stk, ctx, opt, doc).await?)
		}
		ConfigInner::Default(d) => ConfigDefinition::Default(
			crate::legacy::default_config_compute(d, stk, ctx, opt, doc).await?,
		),
	})
}

/// Process this type returning a computed simple Value
pub(crate) async fn define_config_statement_compute(
	this: &DefineConfigStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	let kind = this.inner.kind();
	let base = config_kind_base(&kind);
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Config(kind), base.clone().into())?;
	// Fetch the transaction
	let txn = ctx.tx();
	// Get the config kind
	let cg = match &this.inner {
		ConfigInner::GraphQL(_) => "graphql",
		ConfigInner::Api(_) => "api",
		ConfigInner::Default(_) => "default",
	};

	match base {
		Base::Root => {
			if txn.expect_root_config(cg).await.is_ok() {
				match this.kind {
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
			let key = crate::key::root::root_config::RootConfig {
				ty: Cow::Borrowed(cg),
			};
			let store =
				crate::legacy::config_inner_compute(&this.inner, stk, ctx, opt, doc).await?;
			// Put the config
			txn.replace_key(&key, &store.to_stored()).await?;
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
				match this.kind {
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
			let key = crate::key::database::cg::Config {
				prefix: DatabaseRoot {
					ns,
					db,
				},
				ty: Cow::Borrowed(cg),
			};
			let store =
				crate::legacy::config_inner_compute(&this.inner, stk, ctx, opt, doc).await?;
			// Put the config
			txn.replace_key(&key, &store.to_stored()).await?;
			// Clear the cache
			txn.clear_cache();
		}
	}
	// Ok all good
	Ok(Value::None)
}

/// Map a config kind to the base at which it is administered.
pub(crate) fn config_kind_base(kind: &ConfigKind) -> Base {
	match kind {
		ConfigKind::Default => Base::Root,
		ConfigKind::GraphQL | ConfigKind::Api => Base::Db,
	}
}
