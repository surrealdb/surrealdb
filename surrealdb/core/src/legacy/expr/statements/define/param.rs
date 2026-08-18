use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::catalog::{Error as CatalogError, ParamDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt as _;
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::param::DefineParamStatement;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "DefineParamStatement::compute", skip_all)]
pub(crate) async fn define_param_statement_compute(
	this: &DefineParamStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Parameter, Base::Db)?;

	let value = stk
		.run(|stk| crate::legacy::expr_compute(&this.value, stk, ctx, opt, doc))
		.await
		.catch_return()?;

	// Fetch the transaction
	let txn = ctx.tx();

	// Check if the definition exists
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	if txn.get_db_param(ns, db, &this.name, None).await.is_ok() {
		match this.kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(CatalogError::PaAlreadyExists {
						name: this.name.to_string(),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
	}

	let db = {
		let (ns, db) = opt.ns_db()?;
		txn.get_or_add_db(Some(ctx), ns, db).await?
	};

	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;
	// Process the statement
	txn.put_db_param(
		db.namespace_id,
		db.database_id,
		&ParamDefinition {
			value,
			name: this.name.clone(),
			comment,
			permissions: this.permissions.clone(),
		},
	)
	.await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
