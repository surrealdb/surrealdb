use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Auth;
use crate::kvs::Live;
use crate::sql::statements::define::DefineTableStatement;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Cond, Fetchs, Fields, Uuid, Value};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct LiveStatement {
	pub id: Uuid,
	pub node: Uuid,
	pub expr: Fields,
	pub what: Value,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can check it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	pub(crate) auth: Option<Auth>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can check it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	pub(crate) session: Option<Value>,
}

impl LiveStatement {
	#[doc(hidden)]
	pub fn new(expr: Fields) -> Self {
		LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::new_v4(),
			expr,
			..Default::default()
		}
	}

	/// Creates a live statement from parts that can be set during a query.
	pub(crate) fn from_source_parts(
		expr: Fields,
		what: Value,
		cond: Option<Cond>,
		fetch: Option<Fetchs>,
	) -> Self {
		LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::new_v4(),
			expr,
			what,
			cond,
			fetch,
			..Default::default()
		}
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Is realtime enabled?
		opt.realtime()?;
		// Valid options?
		opt.valid_for_db()?;
		// Get the Node ID
		let nid = opt.id()?;
		// Check that auth has been set
		let mut stm = LiveStatement {
			// Use the current session authentication
			// for when we store the LIVE Statement
			auth: Some(opt.auth.as_ref().clone()),
			// Use the current session authentication
			// for when we store the LIVE Statement
			session: ctx.value("session").cloned(),
			// Clone the rest of the original fields
			// from the LIVE statement to the new one
			..self.clone()
		};
		// Get the id
		let id = stm.id.0;
		// Process the live query table
		match stm.what.compute(stk, ctx, opt, doc).await? {
			Value::Table(tb) => {
				// Store the current Node ID
				stm.node = nid.into();
				// Get the NS and DB
				let ns = opt.ns()?;
				let db = opt.db()?;
				// Store the live info
				let lq = Live {
					ns: ns.to_string(),
					db: db.to_string(),
					tb: tb.to_string(),
				};
				// Get the transaction
				let txn = ctx.tx();
				// Ensure that the table definition exists
				txn.ensure_ns_db_tb(ns, db, &tb, opt.strict).await?;
				// Insert the node live query
				let key = crate::key::node::lq::new(nid, id);
				txn.put(key, lq, None).await?;
				// Insert the table live query
				let key = crate::key::table::lq::new(ns, db, &tb, id);
				txn.put(key, stm, None).await?;
				// Refresh the cache id
				let key = crate::key::database::tb::new(ns, db, &tb);
				let tb = txn.get_tb(ns, db, &tb).await?;
				txn.set(
					key,
					DefineTableStatement {
						cache_lives_ts: uuid::Uuid::now_v7(),
						..tb.as_ref().clone()
					},
					None,
				)
				.await?;
			}
			v => {
				return Err(Error::LiveStatement {
					value: v.to_string(),
				});
			}
		};
		// Return the query id
		Ok(id.into())
	}
}

impl fmt::Display for LiveStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LIVE SELECT {} FROM {}", self.expr, self.what)?;
		if let Some(ref v) = self.cond {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.fetch {
			write!(f, " {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for LiveStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"expr".to_string() => self.expr.structure(),
			"what".to_string() => self.what.structure(),
			"cond".to_string(), if let Some(v) = self.cond => v.structure(),
			"fetch".to_string(), if let Some(v) = self.fetch => v.structure(),
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::dbs::{Action, Capabilities, Notification, Session};
	use crate::kvs::Datastore;
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::TransactionType::Write;
	use crate::sql::Thing;
	use crate::sql::Value;
	use crate::syn::Parse;

	pub async fn new_ds() -> Result<Datastore, crate::err::Error> {
		Ok(Datastore::new("memory")
			.await?
			.with_capabilities(Capabilities::all())
			.with_notifications())
	}

	#[tokio::test]
	async fn test_table_definition_is_created_for_live_query() {
		let dbs = new_ds().await.unwrap().with_notifications();
		let (ns, db, tb) = ("test", "test", "person");
		let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);

		// Create a new transaction and verify that there are no tables defined.
		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let table_occurrences = &*(tx.all_tb(ns, db, None).await.unwrap());
		assert!(table_occurrences.is_empty());
		tx.cancel().await.unwrap();

		// Initiate a live query statement
		let lq_stmt = format!("LIVE SELECT * FROM {}", tb);
		let live_query_response = &mut dbs.execute(&lq_stmt, &ses, None).await.unwrap();

		let live_id = live_query_response.remove(0).result.unwrap();
		let live_id = match live_id {
			Value::Uuid(id) => id,
			_ => panic!("expected uuid"),
		};

		// Verify that the table definition has been created.
		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let table_occurrences = &*(tx.all_tb(ns, db, None).await.unwrap());
		assert_eq!(table_occurrences.len(), 1);
		assert_eq!(table_occurrences[0].name.0, tb);
		tx.cancel().await.unwrap();

		// Initiate a Create record
		let create_statement = format!("CREATE {tb}:test_true SET condition = true");
		let create_response = &mut dbs.execute(&create_statement, &ses, None).await.unwrap();
		assert_eq!(create_response.len(), 1);
		let expected_record = Value::parse(&format!(
			"[{{
				id: {tb}:test_true,
				condition: true,
			}}]"
		));

		let tmp = create_response.remove(0).result.unwrap();
		assert_eq!(tmp, expected_record);

		// Create a new transaction to verify that the same table was used.
		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let table_occurrences = &*(tx.all_tb(ns, db, None).await.unwrap());
		assert_eq!(table_occurrences.len(), 1);
		assert_eq!(table_occurrences[0].name.0, tb);
		tx.cancel().await.unwrap();

		// Validate notification
		let notifications = dbs.notifications().expect("expected notifications");
		let notification = notifications.recv().await.unwrap();
		assert_eq!(
			notification,
			Notification::new(
				live_id,
				Action::Create,
				Value::Thing(Thing::from((tb, "test_true"))),
				Value::parse(&format!(
					"{{
						id: {tb}:test_true,
						condition: true,
					}}"
				),),
			)
		);
	}

	#[tokio::test]
	async fn test_table_exists_for_live_query() {
		let dbs = new_ds().await.unwrap().with_notifications();
		let (ns, db, tb) = ("test", "test", "person");
		let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);

		// Create a new transaction and verify that there are no tables defined.
		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let table_occurrences = &*(tx.all_tb(ns, db, None).await.unwrap());
		assert!(table_occurrences.is_empty());
		tx.cancel().await.unwrap();

		// Initiate a Create record
		let create_statement = format!("CREATE {}:test_true SET condition = true", tb);
		dbs.execute(&create_statement, &ses, None).await.unwrap();

		// Create a new transaction and confirm that a new table is created.
		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let table_occurrences = &*(tx.all_tb(ns, db, None).await.unwrap());
		assert_eq!(table_occurrences.len(), 1);
		assert_eq!(table_occurrences[0].name.0, tb);
		tx.cancel().await.unwrap();

		// Initiate a live query statement
		let lq_stmt = format!("LIVE SELECT * FROM {}", tb);
		dbs.execute(&lq_stmt, &ses, None).await.unwrap();

		// Verify that the old table definition was used.
		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let table_occurrences = &*(tx.all_tb(ns, db, None).await.unwrap());
		assert_eq!(table_occurrences.len(), 1);
		assert_eq!(table_occurrences[0].name.0, tb);
		tx.cancel().await.unwrap();
	}
}
