use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::kvs::Live;
use crate::sql::Value;
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct KillStatement {
	// Uuid of Live Query
	// or Param resolving to Uuid of Live Query
	pub id: Value,
}

impl KillStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Is realtime enabled?
		opt.realtime()?;
		// Valid options?
		opt.valid_for_db()?;
		// Resolve live query id
		let lid = match self.id.compute(stk, ctx, opt, None).await?.convert_to_uuid() {
			Err(_) => {
				return Err(Error::KillStatement {
					value: self.id.to_string(),
				})
			}
			Ok(id) => id,
		};
		// Get the Node ID
		let nid = opt.id()?;
		// Get the LIVE ID
		let lid = lid.0;
		// Get the transaction
		let txn = ctx.tx();
		// Lock the transaction
		let mut txn = txn.lock().await;
		// Fetch the live query key
		let key = crate::key::node::lq::new(nid, lid);
		// Fetch the live query key if it exists
		match txn.get(key, None).await? {
			Some(val) => {
				// Decode the data for this live query
				let val: Live = val.into();
				// Delete the node live query
				let key = crate::key::node::lq::new(nid, lid);
				txn.del(key).await?;
				// Delete the table live query
				let key = crate::key::table::lq::new(&val.ns, &val.db, &val.tb, lid);
				txn.del(key).await?;
			}
			None => {
				return Err(Error::KillStatement {
					value: self.id.to_string(),
				});
			}
		}
		// Return the query id
		Ok(Value::None)
	}
}

impl fmt::Display for KillStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "KILL {}", self.id)
	}
}
