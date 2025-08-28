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
pub struct RemoveEventStatement {
	pub name: Ident,
	pub table_name: Ident,
	pub if_exists: bool,
}

impl RemoveEventStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;

		// Get the transaction
		let txn = ctx.tx();
		// Get the definition
		let ev = match txn.get_tb_event(ns, db, &self.table_name, &self.name).await {
			Ok(x) => x,
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::EvNotFound { .. })) {
					return Ok(Value::None);
				} else {
					return Err(e);
				}
			}
		};
		// Delete the definition
		let key = crate::key::table::ev::new(ns, db, &ev.target_table, &ev.name);
		txn.del(&key).await?;

		let Some(tb) = txn.get_tb(ns, db, &self.table_name).await? else {
			return Err(Error::TbNotFound {
				name: self.table_name.to_string(),
			}
			.into());
		};

		// Refresh the table cache for events
		txn.put_tb(
			ns_name,
			db_name,
			&TableDefinition {
				cache_events_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
		)
		.await?;

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &self.table_name);
		}
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveEventStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE EVENT")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.table_name)?;
		Ok(())
	}
}
