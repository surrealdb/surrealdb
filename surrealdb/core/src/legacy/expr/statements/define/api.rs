use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::api::path::Path;
use crate::catalog::providers::ApiProvider;
use crate::catalog::{ApiAction as CatalogApiAction, ApiDefinition, Error};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt as _;
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::api::DefineApiStatement;
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::legacy::expr_to_ident;
use crate::sql::ApiMethod;
use crate::val::Value;

#[instrument(level = "trace", name = "DefineApiStatement::compute", skip_all)]
pub(crate) async fn define_api_statement_compute(
	this: &DefineApiStatement,
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
	let path_name = expr_to_ident(stk, ctx, opt, doc, &this.path, "api path").await?;
	// Check if the definition exists
	if txn.get_db_api(ns, db, &path_name, None).await?.is_some() {
		match this.kind {
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
	for action in this.actions.iter() {
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

	let config = crate::legacy::api_config_compute(&this.config, stk, ctx, opt, doc).await?;

	let mut actions = Vec::new();
	for action in this.actions.iter() {
		actions.push(CatalogApiAction {
			methods: action.methods.iter().map(|m| (*m).into()).collect(),
			action: action.action.clone(),
			config: crate::legacy::api_config_compute(&action.config, stk, ctx, opt, doc).await?,
		});
	}

	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;

	let ap = ApiDefinition {
		path,
		actions,
		fallback: this.fallback.clone(),
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
