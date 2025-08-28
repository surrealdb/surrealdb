use std::fmt::{self, Display, Formatter};

use anyhow::Result;
use uuid::Uuid;

use crate::catalog::TableDefinition;
use crate::ctx::Context;
use crate::dbs::{self, Notification, Options};
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RemoveTableStatement {
	pub name: Ident,
	pub if_exists: bool,
	pub expunge: bool,
}

impl RemoveTableStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
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
		if self.expunge {
			txn.clr_tb(ns_name, db_name, &self.name).await?
		} else {
			txn.del_tb(ns_name, db_name, &self.name).await?
		};

		// Remove the resource data
		let key = crate::key::table::all::new(ns, db, &self.name);
		if self.expunge {
			txn.clrp(&key).await?
		} else {
			txn.delp(&key).await?
		};
		// Process each attached foreign table
		for ft in fts.iter() {
			// Refresh the table cache
			let foreign_tb = txn.expect_tb(ns, db, &ft.name).await?;
			txn.put_tb(
				ns_name,
				db_name,
				&TableDefinition {
					cache_tables_ts: Uuid::now_v7(),
					..foreign_tb.as_ref().clone()
				},
			)
			.await?;
		}
		// Check if this is a foreign table
		if let Some(view) = &tb.view {
			// Process each foreign table
			for ft in view.what.iter() {
				// Save the view config
				let key = crate::key::table::ft::new(ns, db, ft, &self.name);
				txn.del(&key).await?;
				// Refresh the table cache for foreign tables
				let foreign_tb = txn.expect_tb(ns, db, ft).await?;
				txn.put_tb(
					ns_name,
					db_name,
					&TableDefinition {
						cache_tables_ts: Uuid::now_v7(),
						..foreign_tb.as_ref().clone()
					},
				)
				.await?;
			}
		}
		if let Some(chn) = opt.sender.as_ref() {
			for lv in lvs.iter() {
				chn.send(Notification {
					id: lv.id.into(),
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
			cache.clear();
		}
		// Clear the cache
		txn.clear_cache();
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
