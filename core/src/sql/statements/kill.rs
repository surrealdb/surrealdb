use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::kvs::Live;
use crate::sql::Uuid;
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
		ctx: &Context<'_>,
		opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Is realtime enabled?
		opt.realtime()?;
		// Valid options?
		opt.valid_for_db()?;
		// Resolve live query id
		let lid = match &self.id {
			Value::Uuid(id) => *id,
			Value::Param(param) => match param.compute(stk, ctx, opt, None).await? {
				Value::Uuid(id) => id,
				Value::Strand(id) => match uuid::Uuid::try_parse(&id) {
					Ok(id) => Uuid(id),
					_ => {
						return Err(Error::KillStatement {
							value:
								"KILL received a parameter that could not be converted to a UUID"
									.to_string(),
						});
					}
				},
				_ => {
					return Err(Error::KillStatement {
						value: "KILL received a parameter that was not expected".to_string(),
					});
				}
			},
			Value::Strand(maybe_id) => match uuid::Uuid::try_parse(maybe_id) {
				Ok(id) => Uuid(id),
				_ => {
					return Err(Error::KillStatement {
						value: "KILL received a Strand that could not be converted to a UUID"
							.to_string(),
					});
				}
			},
			_ => {
				return Err(Error::KillStatement {
					value: "Unhandled type for KILL statement".to_string(),
				});
			}
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
		match txn.get(key).await? {
			Some(val) => {
				// Get the NS and DB
				let ns = opt.ns()?;
				let db = opt.db()?;
				// Decode the data for this live query
				let val: Live = val.into();
				// Check the NS and DB are correct
				if ns == val.ns && db == val.db {
					// Delete the node live query
					let key = crate::key::node::lq::new(nid, lid);
					txn.del(key).await?;
					// Delete the table live query
					let key = crate::key::table::lq::new(&val.ns, &val.db, &val.tb, lid);
					txn.del(key).await?;
				} else {
					return Err(Error::KillStatement {
						value: self.id.to_string(),
					});
				}
			}
			None => {
				return Err(Error::KillStatement {
					value: "KILL statement uuid did not exist".to_string(),
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
