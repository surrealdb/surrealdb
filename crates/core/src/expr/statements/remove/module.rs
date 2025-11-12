use std::fmt::{self, Display};

use anyhow::Result;

use crate::catalog::providers::DatabaseProvider;
#[cfg_attr(not(feature = "surrealism"), allow(unused_imports))]
use crate::catalog::{ModuleExecutable, ModuleName};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Value};
use crate::iam::{Action, ResourceKind};
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCacheLookup;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveModuleStatement {
	pub name: ModuleName,
	pub if_exists: bool,
}

impl RemoveModuleStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Module, &Base::Db)?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the definition
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let storage_name = self.name.get_storage_name();
		#[cfg_attr(not(feature = "surrealism"), allow(unused_variables))]
		let md = match txn.get_db_module(ns, db, &storage_name).await {
			Ok(x) => x,
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::MdNotFound { .. })) {
					return Ok(Value::None);
				} else {
					return Err(e);
				}
			}
		};
		// Delete the definition
		let key = crate::key::database::md::new(ns, db, &storage_name);
		txn.del(&key).await?;
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
}

impl Display for RemoveModuleStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Bypass ident display since we don't want backticks arround the ident.
		write!(f, "REMOVE MODULE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
