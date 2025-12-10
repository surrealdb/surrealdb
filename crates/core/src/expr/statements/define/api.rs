use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use super::config::api::ApiConfig;
use super::{CursorDoc, DefineKind};
use crate::api::path::Path;
use crate::catalog::providers::ApiProvider;
use crate::catalog::{ApiActionDefinition, ApiDefinition, ApiMethod};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Expr, FlowResultExt as _, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineApiStatement {
	pub kind: DefineKind,
	pub path: Expr,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<Expr>,
	pub config: ApiConfig,
	pub comment: Expr,
}

impl DefineApiStatement {
	#[instrument(level = "trace", name = "DefineApiStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Api, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		// Check if the definition exists
		if txn.get_db_api(ns, db, &self.path.to_sql()).await?.is_some() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::ApAlreadyExists {
							value: self.path.to_sql(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => {
					return Ok(Value::None);
				}
			}
		}

		let path = stk.run(|stk| self.path.compute(stk, ctx, opt, doc)).await.catch_return()?;
		// Process the statement
		let path: Path = path.coerce_to::<String>()?.parse()?;

		let config = self.config.compute(stk, ctx, opt, doc).await?;

		let mut actions = Vec::new();
		for action in self.actions.iter() {
			actions.push(ApiActionDefinition {
				methods: action.methods.clone(),
				action: action.action.clone(),
				config: action.config.compute(stk, ctx, opt, doc).await?,
			});
		}

		let comment = stk
			.run(|stk| self.comment.compute(stk, ctx, opt, doc))
			.await
			.catch_return()?
			.cast_to()?;

		let ap = ApiDefinition {
			path,
			actions,
			fallback: self.fallback.clone(),
			config,
			comment,
		};
		txn.put_db_api(ns, db, &ap).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ApiAction {
	pub methods: Vec<ApiMethod>,
	pub action: Expr,
	pub config: ApiConfig,
}

impl ToSql for ApiAction {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let stmt: crate::sql::statements::define::ApiAction = self.clone().into();
		stmt.fmt_sql(f, sql_fmt);
	}
}
