use std::borrow::Cow;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{Error as CatalogError, MlModelDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::model::DefineModelStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::MlModelKey;
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "DefineModelStatement::compute", skip_all)]
pub(crate) async fn define_model_statement_compute(
	this: &DefineModelStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Model, Base::Db)?;
	// Fetch the transaction
	let txn = ctx.tx();
	// Check if the definition exists
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	if let Some(model) = txn.get_db_model(ns, db, &this.name, &this.version, None).await? {
		match this.kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(CatalogError::MlAlreadyExists {
						name: model.name.to_string(),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
	}

	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;

	// Process the statement
	let key = MlModelKey {
		ns,
		db,
		ml: Cow::Borrowed(&this.name),
		vn: Cow::Borrowed(&this.version),
	};
	txn.set_key(
		&key,
		&MlModelDefinition {
			hash: this.hash.clone(),
			name: this.name.clone(),
			version: this.version.clone(),
			comment,
			permissions: this.permissions.clone(),
		}
		.to_stored(),
	)
	.await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
