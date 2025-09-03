use std::fmt::{self, Display, Formatter};

use anyhow::Result;
use uuid::Uuid;

use crate::catalog::TableDefinition;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RemoveIndexStatement {
	pub name: Ident,
	pub what: Ident,
	pub if_exists: bool,
}

impl RemoveIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Get the transaction
		let txn = ctx.tx();
		// Clear the index store cache
		#[cfg(not(target_family = "wasm"))]
		let err = ctx
			.get_index_stores()
			.index_removed(ctx.get_index_builder(), &txn, ns, db, &self.what, &self.name)
			.await;
		#[cfg(target_family = "wasm")]
		let err = ctx.get_index_stores().index_removed(&txn, ns, db, &self.what, &self.name).await;

		if let Err(e) = err {
			if self.if_exists && matches!(e.downcast_ref(), Some(Error::IxNotFound { .. })) {
				return Ok(Value::None);
			}
			return Err(e);
		}

		// Delete the definition
		let key = crate::key::table::ix::new(ns, db, &self.what, &self.name);
		txn.del(&key).await?;
		// Remove the index data
		let key = crate::key::index::all::new(ns, db, &self.what, &self.name);
		txn.delp(&key).await?;
		// Refresh the table cache for indexes
		let Some(tb) = txn.get_tb(ns, db, &self.what).await? else {
			return Err(Error::TbNotFound {
				name: self.what.to_string(),
			}
			.into());
		};

		txn.put_tb(
			ns_name,
			db_name,
			&TableDefinition {
				cache_indexes_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
		)
		.await?;
		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &self.what);
		}
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveIndexStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE INDEX")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		Ok(())
	}
}
