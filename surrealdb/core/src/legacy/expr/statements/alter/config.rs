use std::borrow::Cow;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::alter::config::AlterConfigStatement;
use crate::expr::statements::define::config::ConfigInner;
use crate::iam::{Action, ConfigKind, ResourceKind};
use crate::key::schema::DbConfigKey;
use crate::val::Value;

#[instrument(level = "trace", name = "AlterConfigStatement::compute", skip_all)]
pub(crate) async fn alter_config_statement_compute(
	this: &AlterConfigStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	let config_kind = match &this.inner {
		ConfigInner::GraphQL(_) => ConfigKind::GraphQL,
		ConfigInner::Api(_) => ConfigKind::Api,
		ConfigInner::Default(_) => ConfigKind::Default,
	};
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Config(config_kind), Base::Db)?;
	let (_, _) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let txn = ctx.tx();

	let config_name = match &this.inner {
		ConfigInner::GraphQL(_) => "graphql",
		ConfigInner::Api(_) => "api",
		ConfigInner::Default(_) => "default",
	};

	let existing = txn.get_db_config(ns, db, config_name, None).await?;

	if existing.is_none() && this.if_exists {
		return Ok(Value::None);
	}

	let new_def = crate::legacy::config_inner_compute(&this.inner, stk, ctx, opt, doc).await?;
	let key = DbConfigKey {
		ns,
		db,
		ty: Cow::Borrowed(config_name),
	};
	txn.set_key(&key, &new_def.to_stored()).await?;
	txn.clear_cache();
	Ok(Value::None)
}
