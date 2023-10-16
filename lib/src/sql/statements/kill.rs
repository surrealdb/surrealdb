use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::param::param;
use crate::sql::uuid::uuid;
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::into;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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
		// Fetch the live query key
		let key = crate::key::node::lq::new(opt.id()?, live_query_id.0, opt.ns(), opt.db());
		// Fetch the live query key if it exists
		match run.get(key).await? {
			Some(val) => match std::str::from_utf8(&val) {
				Ok(tb) => {
					// Delete the node live query
					let key =
						crate::key::node::lq::new(opt.id()?, live_query_id.0, opt.ns(), opt.db());
					run.del(key).await?;
					// Delete the table live query
					let key = crate::key::table::lq::new(opt.ns(), opt.db(), tb, live_query_id.0);
					run.del(key).await?;
					// Delete notifications
					let start =
						crate::key::table::nt::prefix(opt.ns(), opt.db(), tb, live_query_id);
					let end = crate::key::table::nt::suffix(opt.ns(), opt.db(), tb, live_query_id);
					run.delr(start..end, 1000).await?
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
		Ok(Value::None)
	}
}

impl fmt::Display for KillStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "KILL {}", self.id)
	}
}

pub fn kill(i: &str) -> IResult<&str, KillStatement> {
	let (i, _) = tag_no_case("KILL")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = alt((into(uuid), into(param)))(i)?;
	Ok((
		i,
		KillStatement {
			id: v,
		},
	))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::{Ident, Param, Uuid};

	#[test]
	fn kill_uuid() {
		let uuid_str = "c005b8da-63a4-48bc-a371-07e95b39d58e";
		let uuid_str_wrapped = format!("'{}'", uuid_str);
		let sql = format!("kill {}", uuid_str_wrapped);
		let res = kill(&sql);
		assert!(res.is_ok(), "{:?}", res);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			KillStatement {
				id: Value::Uuid(Uuid::from(uuid::Uuid::parse_str(uuid_str).unwrap()))
			}
		);
		assert_eq!("KILL 'c005b8da-63a4-48bc-a371-07e95b39d58e'", format!("{}", out));
	}

	#[test]
	fn kill_param() {
		let sql = "kill $id";
		let res = kill(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			KillStatement {
				id: Value::Param(Param(Ident("id".to_string()))),
			}
		);
		assert_eq!("KILL $id", format!("{}", out));
	}

	#[tokio::test]
	#[cfg(feature = "kv-mem")]
	async fn kill_removes_notifications() {
		use crate::dbs::{Action, Notification, Session};
		use crate::iam::{Level, Role};
		use crate::kvs::Datastore;
		use crate::kvs::LockType::Optimistic;
		use crate::kvs::TransactionType::Write;
		use crate::sql;
		use crate::sql::Strand;
		use std::collections::BTreeMap;

		let ds = Datastore::new("memory").await.unwrap();
		let remote_node = sql::Uuid::try_from("fe54b86e-d88e-462a-9835-9cb553a75619").unwrap();
		let remote_not_id = sql::Uuid::try_from("3daf1c90-e251-4691-9542-df25b7aa787f").unwrap();
		let ns = "namespace_abc";
		let db = "database_abc";
		let tb = "table_abc";
		let sess = Session::for_level(Level::Root, Role::Owner).with_ns(ns).with_db(db);

		// Create a live query
		let mut vars = BTreeMap::new();
		vars.insert("table".to_string(), Value::Strand(Strand(tb.to_string())));
		let lq = ds.execute("LIVE SELECT * FROM table_abc", &sess, Some(vars)).await.unwrap();
		assert_eq!(lq.len(), 1);
		let lq = match lq.get(0) {
			None => {
				panic!("Expected the response to contain a Uuid");
			}
			Some(r) => match &r.result {
				Ok(Value::Uuid(uuid)) => uuid,
				Ok(_) => panic!("Expected the response to be a Uuid"),
				Err(e) => panic!("Unexpected error: %{}", e),
			},
		};

		// Add notification artificially
		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
		let ts = txn.clock().await;
		let key =
			crate::key::table::nt::new(ns, db, tb, lq.clone(), ts.clone(), remote_not_id.clone());
		let nt = Notification {
			live_id: lq.clone(),
			node_id: remote_node.clone(),
			notification_id: remote_not_id.clone(),
			action: Action::Create,
			result: Value::Strand(Strand::from("this would be an object")),
			timestamp: ts,
		};
		txn.putc_tbnt(key, nt, None).await.unwrap();
		txn.commit().await.unwrap();

		// Kill
		let mut vars = BTreeMap::new();
		vars.insert("id".to_string(), Value::Uuid(lq.clone()));
		let res = ds.execute("KILL $id", &sess, Some(vars)).await.unwrap();
		assert_eq!(res.len(), 1);
		// Verify the response is not an error
		match res.get(0) {
			None => {
				panic!("Expected exactly 1 response")
			}
			Some(r) => match &r.result {
				Ok(_) => {}
				Err(e) => {
					panic!("Expected kill to be successful, got error: {}", e)
				}
			},
		}

		// Verify garbage is removed
		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
		let nots = txn.scan_tbnt(ns, db, tb, lq.clone(), 1000).await.unwrap();
		txn.commit().await.unwrap();
		assert_eq!(nots.len(), 0);
	}
}
