//! Subscriber-side live-query compute.
//!
//! This is the half of the inverted pipeline that runs *off* the mutator's
//! write path. The write path's only job (under the Router engine) is to
//! capture before/after values into the dedicated `lqe` keyspace cheaply
//! ([`crate::lq::event`]). The matching/permission/projection/FETCH work — the
//! part whose cost previously scaled with subscriber count on every write — is
//! performed here instead, reading the captured events back and replaying them
//! through the same per-subscription units the inline engine notifies through
//! ([`crate::exec::vm::notify_at`]).
//!
//! The per-node router ([`crate::lq::router`]) drives this as it tails the `lqe`
//! keyspace. Equivalence with the inline engine is pinned by an end-to-end test
//! that asserts the notifications the router delivers are identical in content
//! to those the inline engine delivers for the same writes.

use std::sync::Arc;

use anyhow::{Context as _, Result};
use surrealdb_strand::TableName;

use crate::dbs::{MessageBroker, Session};
use crate::doc::Action;
use crate::exec::operators::write::ctx::{TableExistence, WriteCtx};
use crate::exec::operators::write::stages::lives::{Images, Notify, deliver};
use crate::expr::ControlFlow;
use crate::kvs::{Datastore, Transaction};
use crate::lq::event::{LiveAction, LiveEvent};

/// The error a write-context failure carries. Neither resolving a table's
/// context nor delivering a notification can signal a loop or a `RETURN`, so
/// anything but an error is a broken invariant.
fn into_error(flow: ControlFlow) -> anyhow::Error {
	match flow {
		ControlFlow::Err(e) => e,
		ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_) => {
			anyhow::Error::new(crate::exec::Error::InvalidControlFlow)
		}
	}
}

/// A table's live subscriptions, resolved once per router pass, that captured
/// [`LiveEvent`]s are replayed against entirely off the write path.
///
/// Replay runs every event against each subscription through
/// [`crate::exec::vm::notify_at`], so all of the inline pipeline's security
/// guarantees hold unchanged: field-permission and computed-field filtering,
/// DIFF leakage protection, session-expiry suppression and per-subscriber error
/// isolation.
pub(crate) struct TableReplay {
	write_ctx: WriteCtx,
	broker: Arc<dyn MessageBroker>,
}

impl TableReplay {
	/// Resolve `table`'s catalog context, including its live subscriptions,
	/// from the read `txn`. That transaction must be at-or-after the commit of
	/// every event replayed through the result, so any permission /
	/// computed-field / FETCH reads observe the committed state.
	///
	/// Returns `None` when the table no longer exists. Removing a table kills
	/// its subscriptions, so events captured for it before the removal have no
	/// one left to notify and are dropped rather than failing the replay.
	pub(crate) async fn resolve(
		ds: &Datastore,
		txn: Arc<Transaction>,
		ns_name: &str,
		db_name: &str,
		table: &TableName,
		broker: Arc<dyn MessageBroker>,
	) -> Result<Option<Self>> {
		// Build a background context carrying the read transaction and the broker
		// that the replayed notifications are sent through.
		let mut ctx = ds.setup_ctx()?;
		ctx.set_transaction(txn);
		ctx.set_broker(Some(Arc::clone(&broker)));
		let ctx = ctx.freeze();
		// Base options scoped to this namespace/database. The auth is anonymous and
		// irrelevant: the pipeline applies each subscription's own stored auth
		// snapshot before evaluating any permission.
		let opt = ds.setup_options(&Session::default().with_ns(ns_name).with_db(db_name));
		let exec = crate::exec::entry::frame(&ctx, &opt, None).await;
		// The lookup is cached on the frame, so resolving the context below
		// reads the definition again for free.
		let db_ctx = exec.database().context("a live-query replay requires a database context")?;
		if db_ctx.get_table_def(table, None).await?.is_none() {
			return Ok(None);
		}
		let write_ctx =
			WriteCtx::resolve(&exec, table, TableExistence::Required).await.map_err(into_error)?;
		Ok(Some(Self {
			write_ctx,
			broker,
		}))
	}

	/// Compute and deliver the notifications for `events`, in order.
	///
	/// An error leaves the events before the failing one delivered, and the
	/// failing event delivered to the subscriptions before the failing one.
	pub(crate) async fn replay(&self, events: &[LiveEvent]) -> Result<()> {
		let write_ctx = &self.write_ctx;
		for event in events {
			// The action selects the same source image and emits the same
			// notification action the inline engine does.
			let images = Images {
				initial: Arc::new(event.before.clone()),
				current: Arc::new(event.after.clone()),
				action: match event.action {
					LiveAction::Create => Action::Create,
					LiveAction::Update => Action::Update,
					LiveAction::Delete => Action::Delete,
				},
			};
			for at in 0..write_ctx.lives.len() {
				let Notify::Deliver(notification) =
					crate::exec::vm::notify_at(write_ctx, at, &event.id, &images)
						.await
						.map_err(into_error)?
				else {
					continue;
				};
				deliver(&self.broker, write_ctx, &write_ctx.lives[at], notification)
					.await
					.map_err(into_error)?;
			}
		}
		Ok(())
	}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::sync::Arc;
	use std::time::Duration;

	use surrealdb_cnf::ConfigMap;

	use crate::channel::Receiver;
	use crate::dbs::{Capabilities, Session};
	use crate::kvs::Datastore;
	use crate::types::{PublicNotification, PublicValue};

	/// Build a datastore with the given live-query engine and a notification
	/// channel, and without maintenance tasks. These tests drive every router
	/// pass themselves: the router task a default build starts runs passes of
	/// its own on a timer, and one that lands between a test's writes and its
	/// draining of the channel delivers notifications the drain then discards.
	async fn new_ds(engine: &str) -> (Receiver<PublicNotification>, Arc<Datastore>) {
		let (send, recv) = crate::channel::bounded(1000);
		let config = ConfigMap::empty().with_key_value("live_query_engine", engine);
		let ds = Datastore::builder()
			.without_maintenance_tasks()
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

		let (recv, ds) = new_ds("router").await;
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write).await.unwrap();
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

	/// A captured event whose table is removed before the router replays it is
	/// dropped, and neither blocks nor re-delivers any other database's events.
	/// The databases are named so the removed table's database is visited
	/// between the other two.
	#[tokio::test]
	async fn router_drops_events_for_a_removed_table() {
		use crate::types::{PublicAction, PublicNotification, PublicValue};

		fn creates(notifs: &[PublicNotification], table: &str) -> usize {
			notifs
				.iter()
				.filter(|n| {
					matches!(n.action, PublicAction::Create)
						&& matches!(&n.record, PublicValue::RecordId(r) if r.table.as_str() == table)
				})
				.count()
		}

		let (recv, ds) = new_ds("router").await;
		ds.execute("DEFINE NAMESPACE test", &Session::owner(), None).await.unwrap();
		for (db, tb) in [("aa", "ta"), ("bb", "tb"), ("cc", "tc")] {
			let sess = Session::owner().with_ns("test").with_db(db);
			ds.execute(&format!("DEFINE DATABASE {db}; DEFINE TABLE {tb}"), &sess, None)
				.await
				.unwrap();
			let live = Session::owner().with_ns("test").with_db(db).with_rt(true);
			ds.execute(&format!("LIVE SELECT * FROM {tb}"), &live, None).await.unwrap();
			ds.execute(&format!("CREATE {tb}:1"), &sess, None).await.unwrap();
		}
		let bb = Session::owner().with_ns("test").with_db("bb");
		ds.execute("REMOVE TABLE tb", &bb, None).await.unwrap();
		// The removal's `Killed` notification is sent directly, not by the router.
		while recv.try_recv().is_ok() {}

		for _ in 0..3 {
			ds.live_query_router_process().await.unwrap();
		}
		let notifs = drain(&recv).await;
		assert_eq!(creates(&notifs, "ta"), 1, "a database before the removed table: {notifs:?}");
		assert_eq!(creates(&notifs, "tb"), 0, "the removed table: {notifs:?}");
		assert_eq!(creates(&notifs, "tc"), 1, "a database after the removed table: {notifs:?}");

		// Delivery continues past the removal on later passes.
		let aa = Session::owner().with_ns("test").with_db("aa");
		ds.execute("CREATE ta:2", &aa, None).await.unwrap();
		ds.live_query_router_process().await.unwrap();
		ds.live_query_router_process().await.unwrap();
		let notifs = drain(&recv).await;
		assert_eq!(creates(&notifs, "ta"), 1, "a later write is delivered once: {notifs:?}");
	}
}
