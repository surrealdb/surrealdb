use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog::providers::{CatalogProvider, NamespaceProvider, UserProvider};
use crate::catalog::{self, Error, UserDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::user::DefineUserStatement;
use crate::iam::{Action, ResourceKind};
use crate::legacy::expr_to_ident;
use crate::val::{Duration, Value};

pub(crate) async fn define_user_statement_to_definition(
	this: &DefineUserStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<catalog::UserDefinition> {
	let token_duration = stk
		.run(|stk| crate::legacy::expr_compute(&this.duration.token, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to::<Option<Duration>>()?
		.map(|x| x.0);
	let session_duration = stk
		.run(|stk| crate::legacy::expr_compute(&this.duration.session, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to::<Option<Duration>>()?
		.map(|x| x.0);

	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;

	Ok(UserDefinition {
		name: expr_to_ident(stk, ctx, opt, doc, &this.name, "user name").await?.into(),
		hash: this.hash.clone(),
		code: this.code.clone(),
		roles: this.roles.clone(),
		token_duration,
		session_duration,
		comment,
		base: this.base.into(),
		scram: this.scram.clone(),
	})
}

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "DefineUserStatement::compute", skip_all)]
pub(crate) async fn define_user_statement_compute(
	this: &DefineUserStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Actor, this.base)?;
	// Compute definition
	let definition =
		crate::legacy::define_user_statement_to_definition(this, stk, ctx, opt, doc).await?;
	// Check the statement type
	match this.base {
		Base::Root => {
			// Fetch the transaction
			let txn = ctx.tx();
			// Check if the definition exists
			if let Some(user) = txn.get_root_user(definition.name.as_str(), None).await? {
				match this.kind {
					DefineKind::Default => {
						if !opt.import {
							bail!(Error::UserRootAlreadyExists {
								name: user.name.to_string(),
							});
						}
					}
					DefineKind::Overwrite => {}
					DefineKind::IfNotExists => return Ok(Value::None),
				}
			}
			// Process the statement
			txn.put_root_user(&definition).await?;
			// Clear the cache
			txn.clear_cache();
			// Ok all good
			Ok(Value::None)
		}
		Base::Ns => {
			// Fetch the transaction
			let txn = ctx.tx();
			let ns = ctx.get_ns_id(opt).await?;
			// Check if the definition exists
			if let Some(user) = txn.get_ns_user(ns, definition.name.as_str(), None).await? {
				match this.kind {
					DefineKind::Default => {
						if !opt.import {
							bail!(Error::UserNsAlreadyExists {
								name: user.name.to_string(),
								ns: opt.ns()?.into(),
							});
						}
					}
					DefineKind::Overwrite => {}
					DefineKind::IfNotExists => return Ok(Value::None),
				}
			}

			let ns = {
				let ns = opt.ns()?;
				txn.get_or_add_ns(Some(ctx), ns).await?
			};

			// Process the statement
			txn.put_ns_user(ns.namespace_id, &definition).await?;
			// Clear the cache
			txn.clear_cache();
			// Ok all good
			Ok(Value::None)
		}
		Base::Db => {
			// Fetch the transaction
			let txn = ctx.tx();
			// Check if the definition exists
			let (ns, db) = ctx.get_ns_db_ids(opt).await?;
			if let Some(user) = txn.get_db_user(ns, db, definition.name.as_str(), None).await? {
				match this.kind {
					DefineKind::Default => {
						if !opt.import {
							bail!(Error::UserDbAlreadyExists {
								name: user.name.to_string(),
								ns: opt.ns()?.to_string(),
								db: opt.db()?.to_string(),
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

			// Process the statement
			txn.put_db_user(db.namespace_id, db.database_id, &definition).await?;
			// Clear the cache
			txn.clear_cache();
			// Ok all good
			Ok(Value::None)
		}
	}
}
