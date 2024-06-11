use std::fmt;

use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::fflags::FFLAGS;
use crate::kvs::lq_structs::{KillEntry, TrackedResult};
use crate::sql::Uuid;
use crate::sql::Value;

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
		let live_query_id = match &self.id {
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
		// Claim transaction
		let mut run = ctx.tx_lock().await;
		if FFLAGS.change_feed_live_queries.enabled() {
			run.pre_commit_register_async_event(TrackedResult::KillQuery(KillEntry {
				live_id: live_query_id,
				ns: opt.ns()?.to_string(),
				db: opt.db()?.to_string(),
			}))?;
		} else {
			// Fetch the live query key
			let key = crate::key::node::lq::new(opt.id()?, live_query_id.0, opt.ns()?, opt.db()?);
			// Fetch the live query key if it exists
			match run.get(key).await? {
				Some(val) => match std::str::from_utf8(&val) {
					Ok(tb) => {
						// Delete the node live query
						let key = crate::key::node::lq::new(
							opt.id()?,
							live_query_id.0,
							opt.ns()?,
							opt.db()?,
						);
						run.del(key).await?;
						// Delete the table live query
						let key =
							crate::key::table::lq::new(opt.ns()?, opt.db()?, tb, live_query_id.0);
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
						value: "KILL statement uuid did not exist".to_string(),
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
	use std::str::FromStr;

	use crate::ctx::Context;
	use crate::dbs::Options;
	use crate::fflags::FFLAGS;
	use crate::kvs::lq_structs::{KillEntry, TrackedResult};
	use crate::kvs::{Datastore, LockType, TransactionType};
	use crate::sql::statements::KillStatement;
	use crate::sql::uuid::Uuid;

	#[test_log::test(tokio::test)]
	async fn kill_handles_uuid_event_registration() {
		if !FFLAGS.change_feed_live_queries.enabled() {
			return;
		}
		let res = KillStatement {
			id: Uuid::from_str("8f92f057-c739-4bf2-9d0c-a74d01299efc").unwrap().into(),
		};
		let ctx = Context::default();
		let opt = Options::new()
			.with_id(uuid::Uuid::from_str("8c41d9f7-a627-40f7-86f5-59d56cd765c6").unwrap())
			.with_live(true)
			.with_db(Some("database".into()))
			.with_ns(Some("namespace".into()));
		let ds = Datastore::new("memory").await.unwrap();
		let tx =
			ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap().enclose();
		let ctx = ctx.set_transaction(tx.clone());

		let mut stack = reblessive::tree::TreeStack::new();

		stack.enter(|stk| res.compute(stk, &ctx, &opt, None)).finish().await.unwrap();

		let mut tx = tx.lock().await;
		tx.commit().await.unwrap();

		// Validate sent
		assert_eq!(
			tx.consume_pending_live_queries(),
			vec![TrackedResult::KillQuery(KillEntry {
				live_id: Uuid::from_str("8f92f057-c739-4bf2-9d0c-a74d01299efc").unwrap(),
				ns: "namespace".to_string(),
				db: "database".to_string(),
			})]
		);
	}
}
