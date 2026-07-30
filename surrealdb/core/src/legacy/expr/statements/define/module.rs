use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::catalog::{Error as CatalogError, ModuleDefinition, ModuleName};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt as _;
use crate::exec::Error as ExecError;
use crate::expr::Base;
#[cfg(feature = "surrealism")]
use crate::expr::module::ModuleExecutable;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::module::DefineModuleStatement;
use crate::iam::{Action, ResourceKind};
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCacheLookup;
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "DefineModuleStatement::compute", skip_all)]
pub(crate) async fn define_module_statement_compute(
	this: &DefineModuleStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Module, Base::Db)?;
	// Fetch the transaction
	let txn = ctx.tx();
	// Check if the definition exists
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	let storage_name = ModuleName::try_from(this)?.get_storage_name();
	// A PERMISSIONS clause must not perform writes (GHSA-66r2-5gwj-gxm2).
	if this.permissions.has_direct_write() {
		bail!(ExecError::PermissionClauseNotReadonly {
			kind: "module",
			name: storage_name.clone(),
		});
	}
	if txn.get_db_module(ns, db, &storage_name, None).await.is_ok() {
		match this.kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(CatalogError::MdAlreadyExists {
						name: storage_name,
					});
				}
			}
			DefineKind::Overwrite => {
				// Remove the module from the cache
				#[cfg(feature = "surrealism")]
				if let Some(cache) = ctx.get_surrealism_cache() {
					let lookup = match &this.executable {
						ModuleExecutable::Surrealism(surrealism) => SurrealismCacheLookup::File(
							&ns,
							&db,
							&surrealism.0.bucket,
							&surrealism.0.key,
						),
						ModuleExecutable::Silo(silo) => SurrealismCacheLookup::Silo(
							&silo.organisation,
							&silo.package,
							silo.major,
							silo.minor,
							silo.patch,
						),
					};

					cache.remove(&lookup);
				}
			}
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

	txn.put_db_module(
		ns,
		db,
		&ModuleDefinition {
			name: this.name.clone(),
			executable: this.executable.clone().into(),
			comment,
			permissions: this.permissions.clone(),
		},
	)
	.await?;
	// Clear the cache
	txn.clear_cache();
	// Warm the surrealism runtime cache for the newly defined module
	#[cfg(feature = "surrealism")]
	if let ModuleExecutable::Surrealism(surrealism) = &this.executable {
		let lookup = SurrealismCacheLookup::File(&ns, &db, &surrealism.0.bucket, &surrealism.0.key);
		if let Err(e) = ctx.get_surrealism_runtime(lookup).await {
			tracing::warn!(
				module = ?this.name,
				error = %e,
				"Failed to eagerly load surrealism module into cache"
			);
		}
	}
	// Ok all good
	Ok(Value::None)
}
