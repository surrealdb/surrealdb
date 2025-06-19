use crate::sql::{Cond, Expr, Fetchs, Fields};

use std::fmt;
use uuid::Uuid;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct LiveStatement {
	pub expr: Fields,
	pub what: Expr,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,
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

impl From<LiveStatement> for crate::expr::statements::LiveStatement {
	fn from(v: LiveStatement) -> Self {
		crate::expr::statements::LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::new_v4(),
			expr: v.expr.into(),
			what: v.what.into(),
			cond: v.cond.map(Into::into),
			fetch: v.fetch.map(Into::into),
			auth: None,
			session: None,
		}
	}
}
impl From<crate::expr::statements::LiveStatement> for LiveStatement {
	fn from(v: crate::expr::statements::LiveStatement) -> Self {
		LiveStatement {
			expr: v.expr.into(),
			what: v.what.into(),
			cond: v.cond.map(Into::into),
			fetch: v.fetch.map(Into::into),
		}
	}
}

// #[cfg(test)]
// mod tests {
// 	use crate::dbs::{Action, Capabilities, Notification, Session};
// 	use crate::kvs::Datastore;
// 	use crate::kvs::LockType::Optimistic;
// 	use crate::kvs::TransactionType::Write;
// 	use crate::sql::Thing;
// 	use crate::syn::Parse;
// 	use anyhow::Result;
//
// 	pub async fn new_ds() -> Result<Datastore> {
// 		Ok(Datastore::new("memory")
// 			.await?
// 			.with_capabilities(Capabilities::all())
// 			.with_notifications())
// 	}
//
// 	#[tokio::test]
// 	async fn test_table_definition_is_created_for_live_query() {
// 		let dbs = new_ds().await.unwrap().with_notifications();
// 		let (ns, db, tb) = ("test", "test", "person");
// 		let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
//
// 		// Create a new transaction and verify that there are no tables defined.
// 		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
// 		let table_occurrences = &*(tx.all_tb(ns, db, None).await.unwrap());
// 		assert!(table_occurrences.is_empty());
// 		tx.cancel().await.unwrap();
//
// 		// Initiate a live query statement
// 		let lq_stmt = format!("LIVE SELECT * FROM {}", tb);
// 		let live_query_response = &mut dbs.execute(&lq_stmt, &ses, None).await.unwrap();
//
// 		let live_id = live_query_response.remove(0).result.unwrap();
// 		let live_id = match live_id {
// 			Value::Uuid(id) => id,
// 			_ => panic!("expected uuid"),
// 		};
//
// 		// Verify that the table definition has been created.
// 		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
// 		let table_occurrences = &*(tx.all_tb(ns, db, None).await.unwrap());
// 		assert_eq!(table_occurrences.len(), 1);
// 		assert_eq!(table_occurrences[0].name.0, tb);
// 		tx.cancel().await.unwrap();
//
// 		// Initiate a Create record
// 		let create_statement = format!("CREATE {tb}:test_true SET condition = true");
// 		let create_response = &mut dbs.execute(&create_statement, &ses, None).await.unwrap();
// 		assert_eq!(create_response.len(), 1);
// 		let expected_record: Value = SqlValue::parse(&format!(
// 			"[{{
// 				id: {tb}:test_true,
// 				condition: true,
// 			}}]"
// 		))
// 		.into();
//
// 		let tmp = create_response.remove(0).result.unwrap();
// 		assert_eq!(tmp, expected_record);
//
// 		// Create a new transaction to verify that the same table was used.
// 		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
// 		let table_occurrences = &*(tx.all_tb(ns, db, None).await.unwrap());
// 		assert_eq!(table_occurrences.len(), 1);
// 		assert_eq!(table_occurrences[0].name.0, tb);
// 		tx.cancel().await.unwrap();
//
// 		// Validate notification
// 		let notifications = dbs.notifications().expect("expected notifications");
// 		let notification = notifications.recv().await.unwrap();
// 		assert_eq!(
// 			notification,
// 			Notification::new(
// 				live_id,
// 				Action::Create,
// 				SqlValue::Thing(Thing::from((tb, "test_true"))).into(),
// 				SqlValue::parse(&format!(
// 					"{{
// 						id: {tb}:test_true,
// 						condition: true,
// 					}}"
// 				))
// 				.into(),
// 			)
// 		);
// 	}
//
// 	#[tokio::test]
// 	async fn test_table_exists_for_live_query() {
// 		let dbs = new_ds().await.unwrap().with_notifications();
// 		let (ns, db, tb) = ("test", "test", "person");
// 		let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
//
// 		// Create a new transaction and verify that there are no tables defined.
// 		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
// 		let table_occurrences = &*(tx.all_tb(ns, db, None).await.unwrap());
// 		assert!(table_occurrences.is_empty());
// 		tx.cancel().await.unwrap();
//
// 		// Initiate a Create record
// 		let create_statement = format!("CREATE {}:test_true SET condition = true", tb);
// 		dbs.execute(&create_statement, &ses, None).await.unwrap();
//
// 		// Create a new transaction and confirm that a new table is created.
// 		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
// 		let table_occurrences = &*(tx.all_tb(ns, db, None).await.unwrap());
// 		assert_eq!(table_occurrences.len(), 1);
// 		assert_eq!(table_occurrences[0].name.0, tb);
// 		tx.cancel().await.unwrap();
//
// 		// Initiate a live query statement
// 		let lq_stmt = format!("LIVE SELECT * FROM {}", tb);
// 		dbs.execute(&lq_stmt, &ses, None).await.unwrap();
//
// 		// Verify that the old table definition was used.
// 		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
// 		let table_occurrences = &*(tx.all_tb(ns, db, None).await.unwrap());
// 		assert_eq!(table_occurrences.len(), 1);
// 		assert_eq!(table_occurrences[0].name.0, tb);
// 		tx.cancel().await.unwrap();
// 	}
// }
