use crate::catalog::TableDefinition;
use crate::ctx::Context;
use crate::dbs::{self, Notification, Options};
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};

use anyhow::Result;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use uuid::Uuid;

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveTableStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
	#[revision(start = 3)]
	pub expunge: bool,
}

impl RemoveTableStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Get the NS and DB
		let (ns, db) = ctx.get_ns_db_ids_ro(opt).await?;
		// Get the transaction
		let txn = ctx.tx();
		// Remove the index stores
		#[cfg(not(target_family = "wasm"))]
		ctx.get_index_stores()
			.table_removed(ctx.get_index_builder(), &txn, ns, db, &self.name)
			.await?;
		#[cfg(target_family = "wasm")]
		ctx.get_index_stores().table_removed(&txn, ns, db, &self.name).await?;
		// Get the defined table
		let Some(tb) = txn.get_tb(ns, db, &self.name).await? else {
			if self.if_exists {
				return Ok(Value::None);
			}

			return Err(Error::TbNotFound {
				name: self.name.to_string(),
			}
			.into());
		};

		// Get the foreign tables
		let fts = txn.all_tb_views(ns, db, &self.name).await?;
		// Get the live queries
		let lvs = txn.all_tb_lives(ns, db, &self.name).await?;
		// Delete the definition
		let key = crate::key::database::tb::new(ns, db, &self.name);
		match self.expunge {
			true => txn.clr(&key).await?,
			false => txn.del(&key).await?,
		};
		// Remove the resource data
		let key = crate::key::table::all::new(ns, db, &self.name);
		match self.expunge {
			true => txn.clrp(&key).await?,
			false => txn.delp(&key).await?,
		};
		// Process each attached foreign table
		for ft in fts.iter() {
			// Refresh the table cache
			let key = crate::key::database::tb::new(ns, db, &ft.name);
			let tb = txn.expect_tb(ns, db, &ft.name).await?;
			txn.set(
				&key,
				&TableDefinition {
					cache_tables_ts: Uuid::now_v7(),
					..tb.as_ref().clone()
				},
				None,
			)
			.await?;
		}
		// Check if this is a foreign table
		if let Some(view) = &tb.view {
			// Process each foreign table
			for ft in view.what.0.iter() {
				// Save the view config
				let key = crate::key::table::ft::new(ns, db, ft, &self.name);
				txn.del(&key).await?;
				// Refresh the table cache for foreign tables
				let key = crate::key::database::tb::new(ns, db, ft);
				let tb = txn.expect_tb(ns, db, ft).await?;
				txn.set(
					&key,
					&TableDefinition {
						cache_tables_ts: Uuid::now_v7(),
						..tb.as_ref().clone()
					},
					None,
				)
				.await?;
			}
		}
		if let Some(chn) = opt.sender.as_ref() {
			for lv in lvs.iter() {
				chn.send(Notification {
					id: lv.id,
					action: dbs::Action::Killed,
					record: Value::None,
					result: Value::None,
				})
				.await?;
			}
		}
		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &self.name);
		}
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveTableStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE TABLE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
