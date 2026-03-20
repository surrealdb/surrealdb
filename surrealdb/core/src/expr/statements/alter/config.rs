use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::define::config::ConfigInner;
use crate::iam::{Action, ConfigKind, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct AlterConfigStatement {
	pub if_exists: bool,
	pub inner: ConfigInner,
	pub comment: AlterKind<String>,
}

impl Default for AlterConfigStatement {
	fn default() -> Self {
		Self {
			if_exists: false,
			inner: ConfigInner::GraphQL(Default::default()),
			comment: AlterKind::None,
		}
	}
}

impl AlterConfigStatement {
	#[instrument(level = "trace", name = "AlterConfigStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		let config_kind = match &self.inner {
			ConfigInner::GraphQL(_) => ConfigKind::GraphQL,
			ConfigInner::Api(_) => ConfigKind::Api,
			ConfigInner::Default(_) => ConfigKind::Default,
		};
		opt.is_allowed(Action::Edit, ResourceKind::Config(config_kind), &Base::Db)?;
		let (_, _) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let txn = ctx.tx();

		let config_name = match &self.inner {
			ConfigInner::GraphQL(_) => "graphql",
			ConfigInner::Api(_) => "api",
			ConfigInner::Default(_) => "default",
		};

		let existing = txn.get_db_config(ns, db, config_name).await?;

		if existing.is_none() && self.if_exists {
			return Ok(Value::None);
		}

		let new_def = self.inner.compute(stk, ctx, opt, doc).await?;
		let key = crate::key::database::cg::new(ns, db, config_name);
		txn.set(&key, &new_def, None).await?;
		txn.clear_cache();
		Ok(Value::None)
	}
}

impl ToSql for AlterConfigStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterConfigStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
