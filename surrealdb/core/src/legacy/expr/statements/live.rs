use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::catalog::{
	CompiledSubscription, NodeLiveQuery, SubscriptionDefinition, SubscriptionFields,
	SubscriptionQuery,
};
use crate::ctx::FrozenContext;
use crate::dbs::{Options, ParameterCapturePass, Variables};
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt as _;
use crate::exec::Error as ExecError;
use crate::expr::statements::live::{LiveFields, LiveStatement, is_document_dependent};
use crate::expr::visit::Visit;
use crate::key::schema::{NodeLiveQueryKey, SubscriptionKey};
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "LiveStatement::compute", skip_all)]
pub(crate) async fn live_statement_compute(
	this: &LiveStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Is realtime enabled?
	ctx.realtime()?;
	// Valid options?
	opt.valid_for_db()?;
	// Get the Node ID
	let nid = ctx.node_id();

	let mut vars = Variables::new();
	let mut pass = ParameterCapturePass {
		context: ctx,
		captures: &mut vars,
	};
	if let LiveFields::Select(x) = &this.fields {
		let _ = x.visit(&mut pass);
	};
	let _ = this.what.visit(&mut pass);
	if let Some(cond) = &this.cond {
		let _ = cond.0.visit(&mut pass);
	}
	if let Some(fetch) = &this.fetch {
		for i in fetch.iter() {
			let _ = i.0.visit(&mut pass);
		}
	}

	let fields = match &this.fields {
		LiveFields::Diff => SubscriptionFields::Diff,
		LiveFields::Select(x) => SubscriptionFields::Select(x.clone()),
	};

	// The statement was just parsed, so its query is compiled by
	// construction; the uncompilable form only arises reading storage back.
	let query = SubscriptionQuery::Compiled(CompiledSubscription {
		fields,
		what: this.what.clone(),
		cond: this.cond.clone().map(|c| c.0),
		fetch: this.fetch.as_ref().map(|fs| fs.iter().map(|f| f.0.clone()).collect()),
	});

	// Check that auth has been set
	let mut subscription_definition = SubscriptionDefinition {
		id: this.id,
		node: this.node,
		query,

		// Use the current session authentication
		// for when we store the LIVE Statement
		auth: Some(opt.auth.as_ref().clone()),
		// Use the current session authentication
		// for when we store the LIVE Statement
		session: ctx.value("session").cloned(),
		// Add the variables to the subscription definition. Keys are
		// copied out of `Strand` into owned `String` here because the
		// subscription is persisted in the catalog and stores
		// `BTreeMap<String, Value>`.
		vars: vars.0.into_iter().map(|(k, v)| (k.into_string(), v)).collect(),
	};
	// Get the id
	let live_query_id = subscription_definition.id;
	// Process the live query table
	match stk
		.run(|stk| crate::legacy::expr_compute(&this.what, stk, ctx, opt, doc))
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
			// Ensure that the table definition exists.
			{
				let (ns, db) = opt.ns_db()?;
				txn.expect_tb_by_name(ns, db, &tb).await?;
			}
			// If the WHERE clause is document-independent (no field references or
			// document-event params), evaluate it now while we still have a full
			// execution context. This catches obviously broken expressions such as
			// `WHERE string::len(NONE)` at registration time instead of silently
			// suppressing every future notification.
			if let Some(cond) = &this.cond
				&& !is_document_dependent(&cond.0)
				&& let Err(e) = stk
					.run(|stk| crate::legacy::expr_compute(&cond.0, stk, ctx, opt, None))
					.await
					.catch_return()
			{
				bail!("LIVE query WHERE clause is invalid and will never match: {e}");
			}
			// Insert the node live query
			let key = NodeLiveQueryKey {
				nd: nid,
				lq: live_query_id,
			};
			txn.replace_key(
				&key,
				&NodeLiveQuery {
					ns,
					db,
					tb: tb.clone(),
				},
			)
			.await?;
			// Insert the table live query
			let key = SubscriptionKey {
				ns,
				db,
				tb: std::borrow::Cow::Borrowed(&tb),
				lq: live_query_id,
			};
			txn.replace_key(&key, &subscription_definition.to_stored()).await?;
			// Bump the table's committed live-query cache timestamp, in the
			// same transaction as the row write above, so writers observe the
			// new subscription. A concurrent writer with a pre-commit snapshot
			// reads the OLD timestamp and cannot poison the cache.
			txn.bump_table_lives_cache(ns, db, &tb).await?;
			// Clear the cache
			txn.clear_cache();
		}
		v => {
			bail!(ExecError::LiveStatement {
				value: v.to_sql(),
			});
		}
	};
	// Return the query id
	Ok(crate::val::Uuid(live_query_id).into())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use anyhow::Result;

	use crate::catalog::providers::{CatalogProvider, TableProvider};
	use crate::channel::Receiver;
	use crate::dbs::{Capabilities, Session};
	use crate::kvs::Datastore;
	use crate::kvs::TransactionType::Write;
	use crate::syn;
	use crate::types::{
		PublicAction, PublicNotification, PublicRecordId, PublicRecordIdKey, PublicValue,
	};

	pub async fn new_ds() -> Result<(Receiver<PublicNotification>, Datastore)> {
		let (send, recv) = crate::channel::bounded(1000);
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_notify(send)
			.build_with_path("memory")
			.await?;
		Ok((recv, ds))
	}

	#[tokio::test]
	async fn test_table_definition_is_created_for_live_query() {
		let (recv, dbs) = new_ds().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);

		let tx = dbs.transaction(Write).await.unwrap();
		let db = tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();

		// Create a new transaction and verify that there are no tables defined.
		let tx = dbs.transaction(Write).await.unwrap();
		let table_occurrences = &*(tx.all_tb(db.namespace_id, db.database_id, None).await.unwrap());
		assert!(table_occurrences.is_empty());
		tx.cancel().await.unwrap();

		// Define the table
		let define_statement = format!("DEFINE TABLE {tb};");
		dbs.execute(&define_statement, &ses, None).await.unwrap();

		// Initiate a live query statement
		let lq_stmt = format!("LIVE SELECT * FROM {}", tb);
		let live_query_response = &mut dbs.execute(&lq_stmt, &ses, None).await.unwrap();

		let live_id = live_query_response.remove(0).result.unwrap();
		let live_id = match live_id {
			PublicValue::Uuid(id) => id,
			_ => panic!("expected uuid"),
		};

		// Verify that the table definition has been created.
		let tx = dbs.transaction(Write).await.unwrap();
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
		let tx = dbs.transaction(Write).await.unwrap();
		let table_occurrences = &*(tx.all_tb(db.namespace_id, db.database_id, None).await.unwrap());
		assert_eq!(table_occurrences.len(), 1);
		assert_eq!(table_occurrences[0].name, tb);
		tx.cancel().await.unwrap();

		// Validate notification
		let notification = recv.recv().await.unwrap();
		assert_eq!(
			notification,
			PublicNotification::new(
				live_id,
				None,
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
		let (_, dbs) = new_ds().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);

		let tx = dbs.transaction(Write).await.unwrap();
		let db = tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();

		// Create a new transaction and verify that there are no tables defined.
		let tx = dbs.transaction(Write).await.unwrap();
		let table_occurrences = &*(tx.all_tb(db.namespace_id, db.database_id, None).await.unwrap());
		assert!(table_occurrences.is_empty());
		tx.cancel().await.unwrap();

		// Initiate a Create record
		let create_statement = format!("CREATE {}:test_true SET condition = true", tb);
		dbs.execute(&create_statement, &ses, None).await.unwrap();

		// Create a new transaction and confirm that a new table is created.
		let tx = dbs.transaction(Write).await.unwrap();
		let table_occurrences = &*(tx.all_tb(db.namespace_id, db.database_id, None).await.unwrap());
		assert_eq!(table_occurrences.len(), 1);
		assert_eq!(table_occurrences[0].name, tb);
		tx.cancel().await.unwrap();

		// Initiate a live query statement
		let lq_stmt = format!("LIVE SELECT * FROM {}", tb);
		dbs.execute(&lq_stmt, &ses, None).await.unwrap();

		// Verify that the old table definition was used.
		let tx = dbs.transaction(Write).await.unwrap();
		let table_occurrences = &*(tx.all_tb(db.namespace_id, db.database_id, None).await.unwrap());
		assert_eq!(table_occurrences.len(), 1);
		assert_eq!(table_occurrences[0].name, tb);
		tx.cancel().await.unwrap();
	}
}
