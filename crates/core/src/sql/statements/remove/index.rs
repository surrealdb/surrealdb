use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::define::DefineTableStatement;
use crate::sql::{Base, Ident, Value};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use uuid::Uuid;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveIndexStatement {
	pub name: Ident,
	pub what: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
			// Get the NS and DB
			let (ns, db) = opt.ns_db()?;
			// Get the transaction
			let txn = ctx.tx();
			// Clear the index store cache
			#[cfg(not(target_family = "wasm"))]
			ctx.get_index_stores()
				.index_removed(ctx.get_index_builder(), &txn, ns, db, &self.what, &self.name)
				.await?;
			#[cfg(target_family = "wasm")]
			ctx.get_index_stores().index_removed(&txn, ns, db, &self.what, &self.name).await?;
			// Delete the definition
			let key = crate::key::table::ix::new(ns, db, &self.what, &self.name);
			txn.del(key).await?;
			// Remove the index data
			let key = crate::key::index::all::new(ns, db, &self.what, &self.name);
			txn.delp(key).await?;
			// Refresh the table cache for indexes
			let key = crate::key::database::tb::new(ns, db, &self.what);
			let tb = txn.get_tb(ns, db, &self.what).await?;
			txn.set(
				key,
				revision::to_vec(&DefineTableStatement {
					cache_indexes_ts: Uuid::now_v7(),
					..tb.as_ref().clone()
				})?,
				None,
			)
			.await?;
			// Clear the cache
			if let Some(cache) = ctx.get_cache() {
				cache.clear_tb(ns, db, &self.what);
			}
			// Clear the cache
			txn.clear();
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::IxNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
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
