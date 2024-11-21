use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::define::DefineTableStatement;
use crate::sql::{Base, Ident, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use uuid::Uuid;

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
			// Get the transaction
			let txn = ctx.tx();
			// Remove the index stores
			ctx.get_index_stores().table_removed(&txn, opt.ns()?, opt.db()?, &self.name).await?;
			// Get the defined table
			let tb = txn.get_tb(opt.ns()?, opt.db()?, &self.name).await?;
			// Get the foreign tables
			let fts = txn.all_tb_views(opt.ns()?, opt.db()?, &self.name).await?;
			// Delete the definition
			let key = crate::key::database::tb::new(opt.ns()?, opt.db()?, &self.name);
			match self.expunge {
				true => txn.clr(key).await?,
				false => txn.del(key).await?,
			};
			// Remove the resource data
			let key = crate::key::table::all::new(opt.ns()?, opt.db()?, &self.name);
			match self.expunge {
				true => txn.clrp(key).await?,
				false => txn.delp(key).await?,
			};
			// Process each attached foreign table
			for ft in fts.iter() {
				// Refresh the table cache
				let key = crate::key::database::tb::new(opt.ns()?, opt.db()?, &ft.name);
				let tb = txn.get_tb(opt.ns()?, opt.db()?, &ft.name).await?;
				txn.set(
					key,
					DefineTableStatement {
						view: None,
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
					let key = crate::key::table::ft::new(opt.ns()?, opt.db()?, ft, &self.name);
					txn.del(key).await?;
					// Refresh the table cache for foreign tables
					let key = crate::key::database::tb::new(opt.ns()?, opt.db()?, ft);
					if let Ok(tb) = txn.get_tb(opt.ns()?, opt.db()?, ft).await {
						txn.set(
							key,
							DefineTableStatement {
								cache_tables_ts: Uuid::now_v7(),
								..tb.as_ref().clone()
							},
							None,
						)
						.await?;
					}
				}
			}
			// Clear the cache
			txn.clear();
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::TbNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
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
