//! `LIVE SELECT`: registering a subscription.
//!
//! The statement's target and its document-independent `WHERE` are expressions
//! and are evaluated through the engine ([`crate::exec::entry`]).
//! The subscription written to the catalog, and the node-scoped key that lets
//! `KILL` find it, are persisted formats.

use anyhow::{Result, bail};
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::catalog::{
	CompiledSubscription, NodeLiveQuery, SubscriptionDefinition, SubscriptionFields,
	SubscriptionQuery,
};
use crate::ctx::FrozenContext;
use crate::dbs::{Options, ParameterCapturePass, Variables};
use crate::exec::Error as ExecError;
use crate::exec::vm::live as vm_live;
use crate::expr::statements::live::{LiveFields, LiveStatement, is_document_dependent};
use crate::expr::visit::Visit;
use crate::key::schema::{NodeLiveQueryKey, SubscriptionKey};
use crate::val::Value;

/// The checks a `KILL` or a `LIVE` makes before it evaluates its operands: realtime is enabled,
/// and the session selects a namespace and a database.
pub(crate) fn realtime_checks(ctx: &FrozenContext, opt: &Options) -> Result<()> {
	// Is realtime enabled?
	ctx.realtime()?;
	// Valid options?
	opt.valid_for_db()?;
	Ok(())
}

/// The subscription a `LIVE SELECT` registers, as the session that issued it
/// defines it: its captured parameters, fields, query, authentication,
/// session and variables, on this node. Both phases of a statement build it
/// from the same `this`, so they agree on the definition.
fn subscription_of(
	this: &LiveStatement,
	ctx: &FrozenContext,
	opt: &Options,
) -> SubscriptionDefinition {
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

	completed(
		SubscriptionDefinition {
			id: this.id,
			node: ctx.node_id(),
			query,
			auth: None,
			session: None,
			vars: Default::default(),
			// Set by the registration, inside its transaction.
			watermark: 0,
		},
		ctx,
		opt,
	)
}

/// The subscription `definition` with what the session that runs the statement contributes: the
/// parameters its clauses capture, its authentication and session, and this node.
///
/// A `LIVE` states its identity and query; the session that issues it supplies the rest, so the
/// statement and a program that carries the statement's definition as a template register the
/// same subscription.
pub(crate) fn completed(
	mut definition: SubscriptionDefinition,
	ctx: &FrozenContext,
	opt: &Options,
) -> SubscriptionDefinition {
	let mut vars = Variables::new();
	let mut pass = ParameterCapturePass {
		context: ctx,
		captures: &mut vars,
	};
	if let SubscriptionQuery::Compiled(query) = &definition.query {
		if let SubscriptionFields::Select(x) = &query.fields {
			let _ = x.visit(&mut pass);
		};
		let _ = query.what.visit(&mut pass);
		if let Some(cond) = &query.cond {
			let _ = cond.visit(&mut pass);
		}
		if let Some(fetch) = &query.fetch {
			for i in fetch.iter() {
				let _ = i.visit(&mut pass);
			}
		}
	}
	definition.node = ctx.node_id();
	// Store the authentication of the current session with the subscription.
	definition.auth = Some(opt.auth.as_ref().clone());
	// Store the session value with the subscription.
	definition.session = ctx.value("session").cloned();
	// Add the captured variables to the subscription definition. Names are
	// converted to owned `String`s because the catalog stores
	// `BTreeMap<String, Value>`.
	definition.vars = vars.0.into_iter().map(|(k, v)| (k.into_string(), v)).collect();
	definition
}

/// Compile the unit of a `LIVE SELECT` as the statement arrives, before the
/// transaction that registers it opens.
///
/// The context holds a read transaction. `None` when the statement would not
/// register for any reason (realtime disabled, invalid options, a target that
/// is not an existing table) or the node serves it no unit: registration
/// raises those errors itself. Otherwise the unit's outcome, for
/// [`live_statement_compute`] to hold or raise.
pub(crate) async fn live_statement_arrive(
	this: &LiveStatement,
	ctx: &FrozenContext,
	opt: &Options,
) -> Option<vm_live::Arrived> {
	ctx.realtime().ok()?;
	opt.valid_for_db().ok()?;
	let exec = crate::exec::entry::frame(ctx, opt, None).await;
	let Ok(Value::Table(tb)) = crate::exec::entry::scalar(&exec, &this.what).await else {
		return None;
	};
	let (ns, db) = ctx.expect_ns_db_ids(opt).await.ok()?;
	let names = opt.ns_db().ok()?;
	let txn = ctx.tx();
	txn.expect_tb_by_name(names.0, names.1, &tb).await.ok()?;
	let subscription = subscription_of(this, ctx, opt);
	vm_live::arrive(ctx, &txn, (ns, db), names, &tb, &subscription).await
}

/// Register the subscription a `LIVE SELECT` names and return its id.
///
/// The context must already hold the statement's transaction. The target must
/// evaluate to a table that exists and is not a lightweight relation; the
/// subscription and its node key are written in that transaction, so the
/// registration commits or rolls back with the statement. `arrived` is the
/// unit compiled as the statement arrived, if any; nothing is compiled in this
/// transaction.
#[cfg_attr(
	feature = "observability",
	tracing::instrument(level = "trace", name = "LiveStatement::compute", skip_all)
)]
pub(crate) async fn live_statement_compute(
	this: &LiveStatement,
	ctx: &FrozenContext,
	opt: &Options,
	arrived: Option<vm_live::Arrived>,
) -> Result<Value> {
	let exec = crate::exec::entry::frame(ctx, opt, None).await;
	realtime_checks(ctx, opt)?;
	let subscription_definition = subscription_of(this, ctx, opt);
	// Process the live query table
	let target = crate::exec::entry::scalar(&exec, &this.what).await?;
	live_register(target, subscription_definition, ctx, opt, arrived).await
}

/// Register `subscription_definition` over `target`, the value the statement's target evaluated
/// to, and return its id, after [`realtime_checks`] and the evaluation of the target.
///
/// The rules of [`live_statement_compute`] hold: the target must be a table that exists and is not
/// a lightweight relation, and the subscription and its node key are written in the statement's
/// transaction.
pub(crate) async fn live_register(
	target: Value,
	mut subscription_definition: SubscriptionDefinition,
	ctx: &FrozenContext,
	opt: &Options,
	arrived: Option<vm_live::Arrived>,
) -> Result<Value> {
	// Get the Node ID
	let nid = ctx.node_id();
	// Get the id
	let live_query_id = subscription_definition.id;
	let Value::Table(tb) = target else {
		bail!(ExecError::LiveStatement {
			value: target.to_sql(),
		});
	};
	// Store the current Node ID
	subscription_definition.node = nid;
	// Get the NS and DB
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	// Get the transaction
	let txn = ctx.tx();
	// Ensure that the table definition exists.
	{
		let (ns, db) = opt.ns_db()?;
		let tbdef = txn.expect_tb_by_name(ns, db, &tb).await?;
		// Lightweight relations have no stored records, which the
		// notification path assumes, so a LIVE query on one is rejected.
		anyhow::ensure!(
			crate::kvs::lightweight::lightweight_relation(&tbdef.table_type).is_none(),
			crate::exec::Error::Thrown(
				"a LIVE query cannot subscribe to a LIGHTWEIGHT relation".to_owned()
			)
		);
	}
	// If the WHERE clause is document-independent (no field references or
	// document-event params), evaluate it now while we still have a full
	// execution context. This catches obviously broken expressions such as
	// `WHERE string::len(NONE)` at registration time instead of silently
	// suppressing every future notification.
	if let Some(cond) =
		subscription_definition.query.compiled().and_then(|query| query.cond.as_ref())
		&& !is_document_dependent(cond)
	{
		let exec = crate::exec::entry::frame(ctx, opt, None).await;
		if let Err(e) = crate::exec::entry::scalar(&exec, cond).await {
			bail!("LIVE query WHERE clause is invalid and will never match: {e}");
		}
	}
	// The router delivers the subscription from captured events, which
	// a lap on any node may reach after this transaction commits
	// though they were captured before it, so the subscription is
	// stored with the timestamp minted when it was registered: every
	// event committed or captured at or below it preceded the
	// registration, so a write executed before `LIVE SELECT` in
	// another open transaction is excluded even when that transaction
	// commits after this one; see [`TableReplay`](crate::lq::subscriber::TableReplay).
	subscription_definition.watermark = txn.timestamp().await?.as_versionstamp();
	// The changes this transaction has already captured on the table
	// precede the subscription too, and commit above the watermark
	// with the ones after it: the entry notes the subscription among
	// them, so the router notifies it of the later ones alone.
	txn.live_event_buffer_note_subscription(ns, db, &tb, live_query_id);
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
	// A write whose snapshot predates this registration's commit does
	// not notify it; entered here, it is found by such a write once
	// that commits, and told then ([`crate::dbs::live_recheck`]). Only
	// the inline engine notifies on the write path; the router engine
	// replays committed changes, and no write asks.
	if ctx.config.datastore.live_query_engine == crate::kvs::LiveQueryEngine::Inline {
		let triggers = txn.commit_triggers();
		let token = triggers.live_registrations.begin(ns, db, &tb, live_query_id, txn.serial());
		txn.on_commit(Box::new(crate::dbs::live_recheck::SettleRegistration {
			triggers,
			token: Some(token),
		}))
		.await;
	}
	// Clear the cache
	txn.clear_cache();
	// Hold the unit compiled as the statement arrived; nothing is
	// compiled in this transaction.
	vm_live::registered(ctx, (ns, db), &tb, &subscription_definition, arrived).await?;
	// Return the query id
	Ok(crate::val::Uuid(live_query_id).into())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::sync::Arc;

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

	pub async fn new_ds() -> Result<(Receiver<PublicNotification>, Arc<Datastore>)> {
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
		let PublicValue::Uuid(live_id) = live_id else {
			panic!("expected uuid");
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
