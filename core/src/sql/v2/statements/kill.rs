use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::fflags::FFLAGS;
use crate::kvs::lq_structs::{KillEntry, LqEntry, TrackedResult};
use crate::sql::Uuid;
use crate::sql::Value;
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
pub struct KillStatement {
	// Uuid of Live Query
	// or Param resolving to Uuid of Live Query
	pub id: Value,
}

impl KillStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Is realtime enabled?
		opt.realtime()?;
		// Valid options?
		opt.valid_for_db()?;
		// Resolve live query id
		let live_query_id = match &self.id {
			Value::Uuid(id) => *id,
			Value::Param(param) => match param.compute(ctx, opt, txn, None).await? {
				Value::Uuid(id) => id,
				Value::Strand(id) => match uuid::Uuid::try_parse(&id) {
					Ok(id) => Uuid(id),
					_ => {
						return Err(Error::KillStatement {
							value: self.id.to_string(),
						});
					}
				},
				_ => {
					return Err(Error::KillStatement {
						value: self.id.to_string(),
					});
				}
			},
			_ => {
				return Err(Error::KillStatement {
					value: self.id.to_string(),
				});
			}
		};
		// Claim transaction
		let mut run = txn.lock().await;
		if FFLAGS.change_feed_live_queries.enabled() {
			run.pre_commit_register_async_event(TrackedResult::KillQuery(KillEntry {
				live_id: live_query_id,
				ns: opt.ns().to_string(),
				db: opt.db().to_string(),
			}))?;
		} else {
			// Fetch the live query key
			let key = crate::key::node::lq::new(opt.id()?, live_query_id.0, opt.ns(), opt.db());
			// Fetch the live query key if it exists
			match run.get(key).await? {
				Some(val) => match std::str::from_utf8(&val) {
					Ok(tb) => {
						// Delete the node live query
						let key = crate::key::node::lq::new(
							opt.id()?,
							live_query_id.0,
							opt.ns(),
							opt.db(),
						);
						run.del(key).await?;
						// Delete the table live query
						let key =
							crate::key::table::lq::new(opt.ns(), opt.db(), tb, live_query_id.0);
						run.del(key).await?;
					}
					_ => {
						return Err(Error::KillStatement {
							value: self.id.to_string(),
						});
					}
				},
				None => {
					return Err(Error::KillStatement {
						value: self.id.to_string(),
					});
				}
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

#[cfg(test)]
mod test {
	use crate::sql::statements::KillStatement;
	use crate::sql::{Ident, Param, Statement, Value};
	use crate::syn::v2::parser::mac::test_parse;

	#[test_log::test(tokio::test)]
	async fn kill_handles_uuid() {
		let res =
			test_parse!(parse_stmt, r#"KILL "8f92f057-c739-4bf2-9d0c-a74d01299efc""#).unwrap();
		res.compute()
	}
}
