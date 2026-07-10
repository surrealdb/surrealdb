//! Subscriber-side live-query compute.
//!
//! This is the half of the inverted pipeline that runs *off* the mutator's
//! write path. The write path's only job (under the Router engine) is to
//! capture before/after values into the dedicated `lqe` keyspace cheaply
//! ([`crate::lq::event`], [`crate::doc::Document::process_live_events`]). The
//! matching/permission/projection/FETCH work — the part whose cost previously
//! scaled with subscriber count on every write — is performed here instead,
//! reading the captured events back and replaying them through the existing,
//! security-audited [`crate::doc::Document::process_table_lives`] pipeline.
//!
//! The per-node router ([`crate::lq::router`]) drives this as it tails the `lqe`
//! keyspace. Equivalence with the inline engine is pinned by an end-to-end test
//! that asserts the notifications the router delivers are identical in content
//! to those the inline engine delivers for the same writes.

use std::sync::Arc;

use anyhow::Result;
use reblessive::TreeStack;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::dbs::{MessageBroker, Session};
use crate::doc::{Document, DocumentContext, NsDbCtx};
use crate::kvs::{Datastore, Transaction};
use crate::lq::event::LiveEvent;
use crate::val::TableName;

/// Compute and deliver live-query notifications for a batch of captured
/// [`LiveEvent`]s on a single table, entirely off the write path.
///
/// The caller supplies the read `txn` — which must be at-or-after the events'
/// commit so any permission / computed-field / FETCH reads observe the
/// committed state — and the `broker` that receives the produced
/// notifications. For each event this resolves the table's catalog context
/// (including its live subscriptions) once, then replays every event through
/// [`Document::replay_live_event`], reusing the inline matching pipeline so all
/// of its security guarantees hold unchanged.
///
/// `ns_name`/`db_name`/`table` identify the table whose events these are; the
/// router resolves them from the `lqe` key and value before calling in.
pub(crate) async fn replay_table_live_events(
	ds: &Datastore,
	txn: Arc<Transaction>,
	ns_name: &str,
	db_name: &str,
	table: &TableName,
	events: &[LiveEvent],
	broker: Arc<dyn MessageBroker>,
) -> Result<()> {
	if events.is_empty() {
		return Ok(());
	}
	// Build a background context carrying the read transaction and the broker
	// that the replayed notifications are sent through.
	let mut ctx = ds.setup_ctx()?;
	ctx.set_transaction(Arc::clone(&txn));
	ctx.set_broker(Some(broker));
	let ctx = ctx.freeze();
	// Resolve the catalog definitions for this table from the read transaction.
	let ns = ctx.tx().expect_ns_by_name(ns_name).await?;
	let db = ctx.tx().expect_db_by_name(ns_name, db_name).await?;
	let tb = ctx.tx().expect_tb_by_name(ns_name, db_name, table).await?;
	let parent = NsDbCtx {
		ns,
		db,
	};
	// The mutating context carries the table's live subscriptions, which
	// `process_table_lives` iterates over per record change.
	let doc_ctx = DocumentContext::initialise(&ctx, &parent, tb, table, None, true).await?;
	// Base options scoped to this namespace/database. The auth is anonymous and
	// irrelevant: the pipeline applies each subscription's own stored auth
	// snapshot before evaluating any permission.
	let opt = ds.setup_options(&Session::default().with_ns(ns_name).with_db(db_name));
	let mut stack = TreeStack::new();
	for event in events {
		let doc_ctx = doc_ctx.clone();
		stack
			.enter(|stk| Document::replay_live_event(stk, &ctx, &opt, doc_ctx, event))
			.finish()
			.await?;
	}
	Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::time::Duration;

	use crate::channel::Receiver;
	use crate::cnf::ConfigMap;
	use crate::dbs::{Capabilities, Session};
	use crate::kvs::Datastore;
	use crate::types::{PublicNotification, PublicValue};

	/// Build a datastore with the given live-query engine and a notification
	/// channel.
	async fn new_ds(engine: &str) -> (Receiver<PublicNotification>, Datastore) {
		let (send, recv) = crate::channel::bounded(1000);
		let config = ConfigMap::empty().with_key_value("live_query_engine", engine);
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_auth(false)
			.with_notify(send)
			.with_config(config)
			.build_with_path("memory")
			.await
			.unwrap();
		(recv, ds)
	}

	fn record_session(ns: &str, db: &str, user_key: &str) -> Session {
		use crate::types::{PublicRecordId, PublicRecordIdKey};
		Session::for_record(
			ns,
			db,
			"user",
			PublicValue::RecordId(PublicRecordId {
				table: "user".to_string().into(),
				key: PublicRecordIdKey::String(user_key.to_string()),
			}),
		)
		.with_rt(true)
	}

	/// Drain notifications until the channel is quiet for 200ms. Inline delivery
	/// flushes on a spawned task, so notifications are not all immediately
	/// available.
	async fn drain(recv: &Receiver<PublicNotification>) -> Vec<PublicNotification> {
		let mut out = Vec::new();
		while let Ok(Ok(n)) = tokio::time::timeout(Duration::from_millis(200), recv.recv()).await {
			out.push(n);
		}
		out
	}

	/// Reduce notifications to a sorted multiset of their *content* — action,
	/// record, and result. The per-subscription live-query id and session id are
	/// deliberately excluded: they are routing/identity metadata that legitimately
	/// differ between two independent datastores. What must be identical between
	/// the inline and router engines is the delivered content, which is where
	/// every matching/permission/projection/diff/fetch guarantee lives.
	fn content(notifs: Vec<PublicNotification>) -> Vec<String> {
		let mut keys: Vec<String> = notifs
			.into_iter()
			.map(|n| format!("{:?}|{:?}|{:?}", n.action, n.record, n.result))
			.collect();
		keys.sort();
		keys
	}

	/// Drive an identical schema + subscriptions + write sequence on a datastore
	/// of the given engine and return the notifications it delivers.
	///
	/// For the Router engine, delivery is performed by the router: a baseline
	/// pass is run after subscriptions and before the writes (establishing the
	/// cursor), then a delivery pass after the writes tails the captured events
	/// and emits the notifications. The inline engine delivers on the write path,
	/// and the router passes are cheap no-ops for it.
	async fn deliver(engine: &str) -> Vec<PublicNotification> {
		let (recv, ds) = new_ds(engine).await;
		let (ns, db) = ("test", "test");
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute(
			"DEFINE TABLE doc PERMISSIONS FOR select FULL; \
			 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
			 DEFINE FIELD owner ON doc TYPE record<user>; \
			 DEFINE FIELD name ON doc TYPE string; \
			 DEFINE FIELD secret ON doc TYPE string PERMISSIONS FOR select WHERE false; \
			 DEFINE FIELD derived ON doc TYPE string \
			     COMPUTED string::concat('derived_', name) \
			     PERMISSIONS FOR select NONE; \
			 DEFINE FIELD mine ON doc TYPE string \
			     COMPUTED string::concat('hi_', name) \
			     PERMISSIONS FOR select WHERE owner = $auth",
			&owner,
			None,
		)
		.await
		.unwrap();

		// A mix of subscribers and projection modes: a privileged owner, a
		// record-access user (alice), an owner DIFF subscription, and a WHERE
		// filter that only matches alice's records.
		let owner_live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let alice = record_session(ns, db, "alice");
		ds.execute("LIVE SELECT * FROM doc", &owner_live, None).await.unwrap();
		ds.execute("LIVE SELECT * FROM doc", &alice, None).await.unwrap();
		ds.execute("LIVE SELECT DIFF FROM doc", &owner_live, None).await.unwrap();
		ds.execute("LIVE SELECT * FROM doc WHERE owner = user:alice", &owner_live, None)
			.await
			.unwrap();

		// Clear any notifications produced during setup.
		while recv.try_recv().is_ok() {}

		// Router baseline: establish the cursor after subscriptions, before the
		// writes (a no-op under the inline engine).
		ds.live_query_router_process().await.unwrap();

		// Drive a representative sequence of mutations.
		ds.execute(
			"CREATE doc:1 SET owner = user:alice, name = 'one', secret = 's1'",
			&owner,
			None,
		)
		.await
		.unwrap();
		ds.execute("CREATE doc:2 SET owner = user:bob, name = 'two', secret = 's2'", &owner, None)
			.await
			.unwrap();
		ds.execute("UPDATE doc:1 SET name = 'one-updated'", &owner, None).await.unwrap();
		ds.execute("DELETE doc:2", &owner, None).await.unwrap();

		// Router delivery pass: tail the captured events and emit notifications
		// (a no-op under the inline engine, which already delivered on the write
		// path). Run twice to prove the cursor prevents re-delivery.
		ds.live_query_router_process().await.unwrap();
		ds.live_query_router_process().await.unwrap();

		drain(&recv).await
	}

	/// The correctness gate for the inverted engine: with the Router engine the
	/// router is the sole delivery path (the inline path is a no-op), and the
	/// content it delivers off the write path must be identical to what the
	/// inline engine delivers on the write path — across CREATE/UPDATE/DELETE,
	/// computed-field and conditional field permissions, plain SELECT, DIFF, and
	/// WHERE filtering, for both owner and record-access subscribers. Running the
	/// router twice also pins exactly-once delivery (a duplicate would break the
	/// multiset equality).
	#[tokio::test]
	async fn router_delivery_matches_inline_delivery() {
		let inline = deliver("inline").await;
		let router = deliver("router").await;
		assert!(!inline.is_empty(), "the inline engine must deliver notifications");
		assert!(!router.is_empty(), "the router engine must deliver notifications");
		assert_eq!(
			content(inline),
			content(router),
			"router-delivered content must equal inline-delivered content",
		);
	}

	/// Regression for the startup-window drop (review P1): the router baseline is
	/// established eagerly at datastore construction, so a subscription and a
	/// write that both happen *before the first router pass* must still be
	/// delivered — the first pass must not treat them as pre-baseline history.
	/// (With the previous lazy baseline, the first pass set the cursor to "now"
	/// and silently dropped the captured event.)
	#[tokio::test]
	async fn router_delivers_events_captured_before_first_pass() {
		use surrealdb_kvs::TransactionType::Write;

		use crate::catalog::providers::CatalogProvider;
		use crate::kvs::LockType::Optimistic;

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute("DEFINE TABLE doc", &owner, None).await.unwrap();
		// Subscribe and write before any router pass has run.
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT * FROM doc", &live, None).await.unwrap();
		while recv.try_recv().is_ok() {}
		ds.execute("CREATE doc:1 SET n = 1", &owner, None).await.unwrap();
		// The very first router pass must deliver the event captured in the
		// startup window (eager baseline was set at construction, before both).
		ds.live_query_router_process().await.unwrap();
		let notifs = drain(&recv).await;
		assert_eq!(notifs.len(), 1, "first router pass must deliver the startup-window event");
	}
}
