use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 2)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveTableStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveTableStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
			// Claim transaction
			let mut run = txn.lock().await;
			// Remove the index stores
			ctx.get_index_stores().table_removed(opt, &mut run, &self.name).await?;
			// Clear the cache
			run.clear_cache();
			// Get the defined table
			let tb = run.get_tb(opt.ns(), opt.db(), &self.name).await?;
			// Delete the definition
			let key = crate::key::database::tb::new(opt.ns(), opt.db(), &self.name);
			run.del(key).await?;
			// Remove the resource data
			let key = crate::key::table::all::new(opt.ns(), opt.db(), &self.name);
			run.delp(key, u32::MAX).await?;
			// Check if this is a foreign table
			if let Some(view) = &tb.view {
				// Process each foreign table
				for v in view.what.0.iter() {
					// Save the view config
					let key = crate::key::table::ft::new(opt.ns(), opt.db(), v, &self.name);
					run.del(key).await?;
				}
			}
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
