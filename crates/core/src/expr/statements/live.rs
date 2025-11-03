use std::fmt;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use uuid::Uuid;

use crate::catalog::providers::CatalogProvider;
use crate::catalog::{NodeLiveQuery, SubscriptionDefinition};
use crate::ctx::Context;
use crate::dbs::{Options, ParameterCapturePass, Variables};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::visit::Visit;
use crate::expr::{Cond, Expr, Fetchs, Fields, FlowResultExt as _};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct LiveStatement {
	pub id: Uuid,
	pub node: Uuid,
	pub fields: Fields,
	pub diff: bool,
	pub what: Expr,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,
}

impl LiveStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Is realtime enabled?
		opt.realtime()?;
		// Valid options?
		opt.valid_for_db()?;
		// Get the Node ID
		let nid = opt.id()?;

		let mut vars = Variables::new();
		let mut pass = ParameterCapturePass {
			context: ctx,
			captures: &mut vars,
		};
		let _ = self.fields.visit(&mut pass);
		let _ = self.what.visit(&mut pass);
		if let Some(cond) = &self.cond {
			let _ = cond.0.visit(&mut pass);
		}
		if let Some(fetch) = &self.fetch {
			for i in fetch.0.iter() {
				let _ = i.0.visit(&mut pass);
			}
		}

		// Check that auth has been set
		let mut subscription_definition = SubscriptionDefinition {
			id: self.id,
			node: self.node,
			fields: self.fields.clone(),
			diff: self.diff,
			what: self.what.clone(),
			cond: self.cond.clone().map(|c| c.0),
			fetch: self.fetch.clone(),

			// Use the current session authentication
			// for when we store the LIVE Statement
			auth: Some(opt.auth.as_ref().clone()),
			// Use the current session authentication
			// for when we store the LIVE Statement
			session: ctx.value("session").cloned(),
			// Add the variables to the subscription definition
			vars: vars.0,
		};
		// Get the id
		let live_query_id = subscription_definition.id;
		// Process the live query table
		match stk
			.run(|stk| subscription_definition.what.compute(stk, ctx, opt, doc))
			.await
			.catch_return()?
		{
			Value::Table(tb) => {
				// Store the current Node ID
				subscription_definition.node = nid;
				// Get the NS and DB
				let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
				// Get the transaction
				let txn = ctx.tx();
				// Ensure that the table definition exists
				{
					let (ns, db) = opt.ns_db()?;
					txn.ensure_ns_db_tb(Some(ctx), ns, db, &tb, opt.strict).await?;
				}
				// Insert the node live query
				let key = crate::key::node::lq::new(nid, live_query_id);
				txn.replace(
					&key,
					&NodeLiveQuery {
						ns,
						db,
						tb: tb.to_string(),
					},
				)
				.await?;
				// Insert the table live query
				let key = crate::key::table::lq::new(ns, db, &tb, live_query_id);
				txn.replace(&key, &subscription_definition).await?;
				// Refresh the table cache for lives
				if let Some(cache) = ctx.get_cache() {
					cache.new_live_queries_version(ns, db, &tb);
				}
				// Clear the cache
				txn.clear_cache();
			}
			v => {
				bail!(Error::LiveStatement {
					value: v.to_string(),
				});
			}
		};
		// Return the query id
		Ok(crate::val::Uuid(live_query_id).into())
	}
}

impl fmt::Display for LiveStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LIVE SELECT {} FROM {}", self.fields, self.what)?;
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
#[allow(clippy::unwrap_used)]
mod tests {
	use anyhow::Result;

	use crate::catalog::providers::{CatalogProvider, TableProvider};
	use crate::dbs::{Capabilities, Session};
	use crate::kvs::Datastore;
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::TransactionType::Write;
	use crate::syn;
	use crate::types::{
		PublicAction, PublicNotification, PublicRecordId, PublicRecordIdKey, PublicValue,
	};

	pub async fn new_ds() -> Result<Datastore> {
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

		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let db = tx.ensure_ns_db(None, ns, db, false).await.unwrap();
		tx.commit().await.unwrap();

		// Create a new transaction and verify that there are no tables defined.
		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let table_occurrences = &*(tx.all_tb(db.namespace_id, db.database_id, None).await.unwrap());
		assert!(table_occurrences.is_empty());
		tx.cancel().await.unwrap();

		// Initiate a live query statement
		let lq_stmt = format!("LIVE SELECT * FROM {}", tb);
		let live_query_response = &mut dbs.execute(&lq_stmt, &ses, None).await.unwrap();

		let live_id = live_query_response.remove(0).result.unwrap();
		let live_id = match live_id {
			PublicValue::Uuid(id) => id,
			_ => panic!("expected uuid"),
		};

		// Verify that the table definition has been created.
		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let table_occurrences = &*(tx.all_tb(db.namespace_id, db.database_id, None).await.unwrap());
		assert_eq!(table_occurrences.len(), 1);
		assert_eq!(table_occurrences[0].name, tb);
		tx.cancel().await.unwrap();

		// Initiate a Create record
		let create_statement = format!("CREATE {tb}:test_true SET condition = true");
		let create_response = &mut dbs.execute(&create_statement, &ses, None).await.unwrap();
		assert_eq!(create_response.len(), 1);
		let expected_record: PublicValue = syn::value(&format!(
			"[{{
				id: {tb}:test_true,
				condition: true,
			}}]"
		))
		.unwrap();

		let tmp = create_response.remove(0).result.unwrap();
		assert_eq!(tmp, expected_record);

		// Create a new transaction to verify that the same table was used.
		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let table_occurrences = &*(tx.all_tb(db.namespace_id, db.database_id, None).await.unwrap());
		assert_eq!(table_occurrences.len(), 1);
		assert_eq!(table_occurrences[0].name, tb);
		tx.cancel().await.unwrap();

		// Validate notification
		let notifications = dbs.notifications().expect("expected notifications");
		let notification = notifications.recv().await.unwrap();
		assert_eq!(
			notification,
			PublicNotification::new(
				live_id,
				PublicAction::Create,
				PublicValue::RecordId(PublicRecordId {
					table: tb.into(),
					key: PublicRecordIdKey::String("test_true".to_owned())
				}),
				syn::value(&format!(
					"{{
						id: {tb}:test_true,
						condition: true,
					}}"
				))
				.unwrap(),
			)
		);
	}

	#[tokio::test]
	async fn test_table_exists_for_live_query() {
		let dbs = new_ds().await.unwrap().with_notifications();
		let (ns, db, tb) = ("test", "test", "person");
		let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);

		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let db = tx.ensure_ns_db(None, ns, db, false).await.unwrap();
		tx.commit().await.unwrap();

		// Create a new transaction and verify that there are no tables defined.
		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let table_occurrences = &*(tx.all_tb(db.namespace_id, db.database_id, None).await.unwrap());
		assert!(table_occurrences.is_empty());
		tx.cancel().await.unwrap();

		// Initiate a Create record
		let create_statement = format!("CREATE {}:test_true SET condition = true", tb);
		dbs.execute(&create_statement, &ses, None).await.unwrap();

		// Create a new transaction and confirm that a new table is created.
		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let table_occurrences = &*(tx.all_tb(db.namespace_id, db.database_id, None).await.unwrap());
		assert_eq!(table_occurrences.len(), 1);
		assert_eq!(table_occurrences[0].name, tb);
		tx.cancel().await.unwrap();

		// Initiate a live query statement
		let lq_stmt = format!("LIVE SELECT * FROM {}", tb);
		dbs.execute(&lq_stmt, &ses, None).await.unwrap();

		// Verify that the old table definition was used.
		let tx = dbs.transaction(Write, Optimistic).await.unwrap();
		let table_occurrences = &*(tx.all_tb(db.namespace_id, db.database_id, None).await.unwrap());
		assert_eq!(table_occurrences.len(), 1);
		assert_eq!(table_occurrences[0].name, tb);
		tx.cancel().await.unwrap();
	}
}
