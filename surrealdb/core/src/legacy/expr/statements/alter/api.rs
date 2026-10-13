use std::borrow::Cow;
use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::ApiProvider;
use crate::catalog::{ApiAction as CatalogApiAction, Error};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::api::{AlterApiClause, AlterApiStatement};
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::key::schema::ApiKey;
use crate::legacy::expr_to_ident;
use crate::sql::ApiMethod;
use crate::val::Value;

#[instrument(level = "trace", name = "AlterApiStatement::compute", skip_all)]
pub(crate) async fn alter_api_statement_compute(
	this: &AlterApiStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Api, Base::Db)?;
	let (_, _) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let txn = ctx.tx();

	let path_name = expr_to_ident(stk, ctx, opt, doc, &this.path, "api path").await?;
	let mut ap = match txn.get_db_api(ns, db, &path_name, None).await? {
		Some(v) => v.deref().clone(),
		None => {
			if this.if_exists {
				return Ok(Value::None);
			}
			return Err(Error::ApNotFound {
				value: path_name,
			}
			.into());
		}
	};

	for clause in &this.clauses {
		match clause {
			AlterApiClause::ForAny {
				config,
				fallback,
			} => {
				if let Some(c) = config {
					ap.config = crate::legacy::api_config_compute(c, stk, ctx, opt, doc).await?;
				}
				match fallback {
					AlterKind::Set(v) => ap.fallback = Some(v.clone()),
					AlterKind::Drop => ap.fallback = None,
					AlterKind::None => {}
				}
			}
			AlterApiClause::SetAction(action) => {
				remove_methods_from_actions(&mut ap.actions, &action.methods);
				ap.actions.push(CatalogApiAction {
					methods: action.methods.iter().map(|m| (*m).into()).collect(),
					action: action.action.clone(),
					config: crate::legacy::api_config_compute(&action.config, stk, ctx, opt, doc)
						.await?,
				});
			}
			AlterApiClause::DropAction {
				methods,
			} => {
				remove_methods_from_actions(&mut ap.actions, methods);
			}
		}
	}

	match this.comment {
		AlterKind::Set(ref v) => ap.comment = Some(v.clone()),
		AlterKind::Drop => ap.comment = None,
		AlterKind::None => {}
	}

	// Recompute auth_limit from the current principal to prevent privilege escalation
	ap.auth_limit = AuthLimit::new_from_auth(opt.auth.as_ref()).into();

	let key = ApiKey {
		ns,
		db,
		ap: Cow::Borrowed(&path_name),
	};
	txn.set_key(&key, &ap.to_stored()).await?;
	txn.clear_cache();
	Ok(Value::None)
}

/// Remove the given methods from existing action entries, splitting entries
/// that partially overlap and removing entries that are fully consumed.
pub(crate) fn remove_methods_from_actions(
	actions: &mut Vec<CatalogApiAction>,
	drop_methods: &[ApiMethod],
) {
	let mut i = 0;
	while i < actions.len() {
		actions[i].methods.retain(|m| !drop_methods.contains(&crate::sql::ApiMethod::from(*m)));
		if actions[i].methods.is_empty() {
			actions.swap_remove(i);
		} else {
			i += 1;
		}
	}
}
