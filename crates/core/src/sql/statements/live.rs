use crate::iam::Auth;

use crate::sql::{Cond, Fetchs, Fields, SqlValue, Uuid};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct LiveStatement {
	pub id: Uuid,
	pub node: Uuid,
	pub expr: Fields,
	pub what: SqlValue,
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
	pub(crate) session: Option<SqlValue>,
}

impl LiveStatement {
	pub fn new(expr: Fields) -> Self {
		LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::new_v4(),
			expr,
			..Default::default()
		}
	}

	pub fn new_from_what_expr(expr: Fields, what: SqlValue) -> Self {
		LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::new_v4(),
			what,
			expr,
			..Default::default()
		}
	}

	/// Creates a live statement from parts that can be set during a query.
	pub(crate) fn from_source_parts(
		expr: Fields,
		what: SqlValue,
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
}

impl From<LiveStatement> for crate::expr::statements::LiveStatement {
	fn from(v: LiveStatement) -> Self {
		crate::expr::statements::LiveStatement {
			id: v.id.into(),
			node: v.node.into(),
			expr: v.expr.into(),
			what: v.what.into(),
			cond: v.cond.map(Into::into),
			fetch: v.fetch.map(Into::into),
			auth: v.auth,
			session: v.session.map(Into::into),
		}
	}
}
impl From<crate::expr::statements::LiveStatement> for LiveStatement {
	fn from(v: crate::expr::statements::LiveStatement) -> Self {
		LiveStatement {
			id: v.id.into(),
			node: v.node.into(),
			expr: v.expr.into(),
			what: v.what.into(),
			cond: v.cond.map(Into::into),
			fetch: v.fetch.map(Into::into),
			auth: v.auth,
			session: v.session.map(Into::into),
		}
	}
}

crate::sql::impl_display_from_sql!(LiveStatement);

impl crate::sql::DisplaySql for LiveStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

#[cfg(test)]
mod tests {
	use crate::dbs::{Action, Capabilities, Notification, Session};
	use crate::kvs::Datastore;
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::TransactionType::Write;
	use crate::sql::SqlValue;
	use crate::sql::Thing;
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
			SqlValue::Uuid(id) => id,
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
		let expected_record = SqlValue::parse(&format!(
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
				SqlValue::Thing(Thing::from((tb, "test_true"))),
				SqlValue::parse(&format!(
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
