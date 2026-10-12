use anyhow::{Result, bail};

use crate::catalog::providers::{CatalogProvider, NamespaceProvider, UserProvider};
use crate::catalog::{self, Error, UserDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::user::DefineUserStatement;
use crate::iam::{Action, ResourceKind};
use crate::val::{BoundedDuration, Duration, Value};

pub(crate) async fn define_user_statement_to_definition(
	this: &DefineUserStatement,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<catalog::UserDefinition> {
	let exec = &crate::ddl::frame(ctx, opt, doc).await;
	let token_duration = crate::ddl::scalar::scalar(exec, &this.duration.token)
		.await?
		.cast_to::<Option<Duration>>()?
		.map(|x| BoundedDuration(x.0));
	let session_duration = crate::ddl::scalar::scalar(exec, &this.duration.session)
		.await?
		.cast_to::<Option<Duration>>()?
		.map(|x| BoundedDuration(x.0));

	let comment = crate::ddl::scalar::scalar(exec, &this.comment).await?.cast_to()?;

	Ok(UserDefinition {
		name: crate::ddl::scalar::ident(exec, &this.name, "user name").await?.into(),
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
#[cfg_attr(
	feature = "observability",
	tracing::instrument(level = "trace", name = "DefineUserStatement::compute", skip_all)
)]
pub(crate) async fn define_user_statement_compute(
	this: &DefineUserStatement,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Actor, this.base)?;
	// Compute definition
	let definition = crate::ddl::define_user_statement_to_definition(this, ctx, opt, doc).await?;
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
