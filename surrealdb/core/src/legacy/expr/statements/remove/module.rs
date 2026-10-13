use anyhow::Result;

use crate::catalog::Error;
#[cfg_attr(not(feature = "surrealism"), allow(unused_imports))]
use crate::catalog::ModuleExecutable;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::Base;
use crate::expr::statements::remove::module::RemoveModuleStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::ModuleKey;
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCacheLookup;
use crate::val::Value;

/// Process this type returning a computed simple Value
pub(crate) async fn remove_module_statement_compute(
	this: &RemoveModuleStatement,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Module, Base::Db)?;
	// Get the transaction
	let txn = ctx.tx();
	// Get the definition
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let storage_name = this.name.get_storage_name();
	#[cfg_attr(not(feature = "surrealism"), allow(unused_variables))]
	let md = match txn.get_db_module(ns, db, &storage_name, None).await {
		Ok(x) => x,
		Err(e) => {
			if this.if_exists && matches!(e.downcast_ref(), Some(Error::MdNotFound { .. })) {
				return Ok(Value::None);
			} else {
				return Err(e);
			}
		}
	};
	// Delete the definition
	let key = ModuleKey {
		ns,
		db,
		md: std::borrow::Cow::Borrowed(&storage_name),
	};
	txn.del_key(&key).await?;
	// Clear the cache
	txn.clear_cache();
	// Remove the module from the cache
	#[cfg(feature = "surrealism")]
	if let Some(cache) = ctx.get_surrealism_cache() {
		let lookup = match &md.executable {
			ModuleExecutable::Surrealism(surrealism) => {
				SurrealismCacheLookup::File(&ns, &db, &surrealism.bucket, &surrealism.key)
			}
			ModuleExecutable::Silo(silo) => SurrealismCacheLookup::Silo(
				&ns,
				&db,
				&silo.organisation,
				&silo.package,
				silo.major,
				silo.minor,
				silo.patch,
			),
		};

		cache.remove(&lookup);
	}
	// Ok all good
	Ok(Value::None)
}
