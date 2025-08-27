use std::fmt::{self, Display, Formatter};

use anyhow::Result;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RemoveDatabaseStatement {
	pub name: Ident,
	pub if_exists: bool,
	pub expunge: bool,
}

impl RemoveDatabaseStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;
		// Get the transaction
		let txn = ctx.tx();

		let ns = opt.ns()?;
		let db = match txn.get_db_by_name(ns, &self.name).await? {
			Some(x) => x,
			None => {
				if self.if_exists {
					return Ok(Value::None);
				} else {
					return Err(Error::DbNotFound {
						name: self.name.to_string(),
					}
					.into());
				}
			}
		};

		// Remove the index stores
		#[cfg(not(target_family = "wasm"))]
		ctx.get_index_stores()
			.database_removed(ctx.get_index_builder(), &txn, db.namespace_id, db.database_id)
			.await?;
		#[cfg(target_family = "wasm")]
		ctx.get_index_stores().database_removed(&txn, db.namespace_id, db.database_id).await?;
		// Remove the sequences
		if let Some(seq) = ctx.get_sequences() {
			seq.database_removed(&txn, db.namespace_id, db.database_id).await?;
		}

		// Delete the definition
		let key = crate::key::namespace::db::new(db.namespace_id, &db.name);
		let database_root = crate::key::database::all::new(db.namespace_id, db.database_id);
		if self.expunge {
			txn.clr(&key).await?;
			txn.clrp(&database_root).await?;
		} else {
			txn.del(&key).await?;
			txn.delp(&database_root).await?
		};

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear();
		}
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveDatabaseStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE DATABASE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
