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
#[revisioned(revision = 1)]
pub struct RemoveTableStatement {
	pub name: Ident,
}

impl RemoveTableStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
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
}

impl Display for RemoveTableStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE TABLE {}", self.name)
	}
}
