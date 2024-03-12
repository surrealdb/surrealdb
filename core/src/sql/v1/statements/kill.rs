use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::fflags::FFLAGS;
use crate::kvs::lq_structs::{KillEntry, TrackedResult};
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
						})
					}
				},
				_ => {
					return Err(Error::KillStatement {
						value: self.id.to_string(),
					})
				}
			},
			_ => {
				return Err(Error::KillStatement {
					value: self.id.to_string(),
				})
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
						})
					}
				},
				None => {
					return Err(Error::KillStatement {
						value: self.id.to_string(),
					})
				}
			}
			// Return the query id
		}
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
	use std::str::FromStr;

	use crate::ctx::Context;
	use crate::dbs::Options;
	use crate::fflags::FFLAGS;
	use crate::kvs::lq_structs::{KillEntry, TrackedResult};
	use crate::kvs::{Datastore, LockType, TransactionType};
	use crate::sql::v1::statements::KillStatement;
	use crate::sql::Uuid;

	#[test_log::test(tokio::test)]
	async fn kill_handles_uuid_event_registration() {
		if !FFLAGS.change_feed_live_queries.enabled() {
			return;
		}
		let res = KillStatement {
			id: Uuid::from_str("889757b3-2040-4da3-9ad6-47fe65bd2fb6").unwrap().into(),
		};
		let ctx = Context::default();
		let opt = Options::new()
			.with_id(uuid::Uuid::from_str("55a85e9c-7cd1-49cb-a8f7-41124d8fdaf8").unwrap())
			.with_live(true)
			.with_db(Some("database".into()))
			.with_ns(Some("namespace".into()));
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx =
			ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap().enclose();
		res.compute(&ctx, &opt, &tx, None).await.unwrap();

		let mut tx = tx.lock().await;
		tx.commit().await.unwrap();

		// Validate sent
		assert_eq!(
			tx.consume_pending_live_queries(),
			vec![TrackedResult::KillQuery(KillEntry {
				live_id: Uuid::from_str("889757b3-2040-4da3-9ad6-47fe65bd2fb6").unwrap(),
				ns: "namespace".to_string(),
				db: "database".to_string(),
			})]
		);
	}
}
