use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::catalog::{Error as CatalogError, FunctionDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::function::DefineFunctionStatement;
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "DefineFunctionStatement::compute", skip_all)]
pub(crate) async fn define_function_statement_compute(
	this: &DefineFunctionStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Function, Base::Db)?;
	// Validate any GRAPHQL_ALIAS at definition time so typos surface here
	// rather than silently falling back at schema-generation time.
	crate::legacy::expr::statements::define::validate_graphql_alias(
		&this.graphql_alias,
		"function",
	)?;
	// Fetch the transaction
	let txn = ctx.tx();
	// Check if the definition exists
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	if txn.get_db_function(ns, db, &this.name, None).await.is_ok() {
		match this.kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(CatalogError::FcAlreadyExists {
						name: format!("fn::{}", this.name),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => {
				return Ok(Value::None);
			}
		}
	}

	// Process the statement
	let (ns_name, db_name) = opt.ns_db()?;
	txn.get_or_add_db(Some(ctx), ns_name, db_name).await?;

	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;

	txn.put_db_function(
		ns,
		db,
		&FunctionDefinition {
			name: this.name.clone(),
			args: this.args.clone(),
			block: this.block.clone(),
			permissions: this.permissions.clone(),
			returns: this.returns.clone(),
			comment,
			auth_limit: AuthLimit::new_from_auth(&opt.auth).into(),
			graphql_alias: this.graphql_alias.clone(),
			graphql_deprecated: this.graphql_deprecated.clone(),
		},
	)
	.await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
