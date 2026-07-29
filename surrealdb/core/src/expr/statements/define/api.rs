use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use super::config::api::ApiConfig;
use super::{CursorDoc, DefineKind};
use crate::api::path::Path;
use crate::catalog::providers::ApiProvider;
use crate::catalog::{ApiAction as CatalogApiAction, ApiDefinition, ApiMethod, Error};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, FlowResultExt as _, Value};
use crate::iam::{Action, AuthLimit, ResourceKind};

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
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Api, Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		// Resolve the path identifier
		let path_name = expr_to_ident(stk, ctx, opt, doc, &self.path, "api path").await?;
		// Check if the definition exists
		if txn.get_db_api(ns, db, &path_name, None).await?.is_some() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::ApAlreadyExists {
							value: path_name,
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => {
					return Ok(Value::None);
				}
			}
		}

		let path: Path = path_name.parse()?;

		// Reject duplicate methods across all FOR clauses on this DEFINE API.
		// `find_definition`/`process_api_request` route a request to the first
		// action whose `methods` contain the request's method, so a second
		// FOR clause with an overlapping method would be silently unreachable.
		// ALTER API consolidates overlapping methods via its split-and-replace
		// semantics; DEFINE has no such consolidation, so we must reject up-front.
		let mut seen: Vec<ApiMethod> = Vec::new();
		for action in self.actions.iter() {
			for m in &action.methods {
				if seen.contains(m) {
					bail!(Error::ApMethodDuplicate {
						value: path_name,
						method: m.to_string(),
					});
				}
				seen.push(*m);
			}
		}

		let config = self.config.compute(stk, ctx, opt, doc).await?;

		let mut actions = Vec::new();
		for action in self.actions.iter() {
			actions.push(CatalogApiAction {
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
			auth_limit: AuthLimit::new_from_auth(opt.auth.as_ref()).into(),
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
