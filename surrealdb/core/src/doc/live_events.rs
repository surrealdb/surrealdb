use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;
use tokio::sync::OnceCell;

use super::document::Extras;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::{Action, CursorDoc, Document, DocumentContext};
use crate::idx::planner::RecordStrategy;
use crate::kvs::LiveQueryEngine;
use crate::lq::event::{LiveAction, LiveEvent};

impl Document {
	/// Capture a live-query event for this record change into the dedicated
	/// live-query keyspace ([`LiveEventsKey`](crate::key::schema::LiveEventsKey)).
	///
	/// This runs only when the Router engine is selected and the table currently
	/// has at least one subscriber, so it is a no-op on the default (Inline) write
	/// path and on tables nobody is watching. The before/after values are buffered
	/// on the transaction and flushed at commit with the commit versionstamp,
	/// exactly like the changefeed — but to a separate keyspace.
	pub(super) async fn process_live_events(
		&self,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<()> {
		// Only the Router engine consumes live-query events.
		if ctx.config.datastore.live_query_engine != LiveQueryEngine::Router {
			return Ok(());
		}
		// Imports never produce live notifications.
		if opt.import {
			return Ok(());
		}
		// Only capture actual modifications.
		if !self.is_modified() {
			return Ok(());
		}
		// Resolve the namespace, database, and table for this record.
		let ns = self.doc_ctx.ns();
		let db = self.doc_ctx.db();
		let tb = self.doc_ctx.tb()?;
		// Only capture for tables that have at least one subscriber — this is the
		// gate that keeps the write path's added cost proportional to demand.
		//
		// The gate reads the committed `key::table::lq` rows within this write's
		// transaction snapshot (not a node-local cache), so it reflects the
		// cluster-wide subscription state: a write captures an event whenever ANY
		// node is subscribed to the table, including subscriptions created on other
		// nodes. This is what makes Router-mode capture correct on a shared store —
		// a node-local cache has no cross-node invalidation and could miss a remote
		// subscriber, dropping an event that can never be replayed. The check is a
		// limit-1 key scan, so its cost is independent of the subscriber count.
		if !ctx.tx().table_has_live_query(ns.namespace_id, db.database_id, &tb.name).await? {
			return Ok(());
		}
		// Buffer the before/after for this record; flushed at commit.
		if let Some(id) = &self.id {
			ctx.tx().live_event_buffer_record_change(
				ns.namespace_id,
				db.database_id,
				&tb.name,
				id.as_ref(),
				self.initial.doc.clone(),
				self.current.doc.clone(),
			);
		}
		Ok(())
	}

	/// Replay a single captured [`LiveEvent`] through the live-query
	/// notification pipeline, **off** the mutator's write path.
	///
	/// This is the heart of the inverted engine. It reconstructs a [`Document`]
	/// whose `initial`/`current` are the event's before/after values — the exact
	/// values the inline write path held when [`Self::process_live_events`]
	/// captured them — and then runs the *same* [`Self::process_table_lives`]
	/// matching / permission / projection / FETCH pipeline that the inline
	/// engine runs. Reusing that audited path verbatim (rather than forking it)
	/// means the subscriber-side compute inherits every existing security
	/// guarantee unchanged: computed-field permission filtering (#120), DIFF
	/// leakage protection, session-expiry suppression (#101), and per-subscriber
	/// error isolation.
	///
	/// `doc_ctx` must be the mutating table context
	/// ([`DocumentContext::NsDbTbMutCtx`]); it carries the table's
	/// [`StoredSubscriptionDefinition`](crate::catalog::StoredSubscriptionDefinition)s that
	/// `process_table_lives` iterates. `ctx` must carry the (read) transaction
	/// and the notification broker; `opt` only needs the namespace/database set,
	/// because the per-subscription auth is applied inside the pipeline from each
	/// subscription's stored snapshot.
	pub(crate) async fn replay_live_event(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: DocumentContext,
		event: &LiveEvent,
	) -> Result<()> {
		// Map the captured action back to the write-path action so
		// `process_table_lives` selects the same source view and emits the same
		// notification `Action`.
		let action = match event.action {
			LiveAction::Create => Action::Create,
			LiveAction::Update => Action::Update,
			LiveAction::Delete => Action::Delete,
		};
		// Reconstruct the document with the captured before/after values. The id
		// is shared by both cursors, mirroring how `Document::new` clones the
		// initial cursor into the current one.
		let id = Arc::new(event.id.clone());
		let initial = CursorDoc::new(Some(Arc::clone(&id)), None, event.before.clone());
		let current = CursorDoc::new(Some(Arc::clone(&id)), None, event.after.clone());
		let mut doc = Document {
			doc_ctx,
			id: Some(id),
			r#gen: None,
			retry: false,
			extras: Extras::Normal,
			initial,
			current,
			initial_reduced: None,
			current_reduced: None,
			// Full record values are present (not a key-only scan), so the
			// pipeline reads them directly.
			record_strategy: RecordStrategy::KeysAndValues,
			input_data: None,
			mutated: false,
			modified: OnceCell::new(),
		};
		// Run the identical matching/permission/projection/FETCH pipeline the
		// inline write path uses — but via the inner entry, bypassing the
		// write-path engine gate (this *is* the off-path Router delivery).
		doc.process_table_lives_inner(stk, ctx, opt, action).await
	}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::time::Duration;

	use surrealdb_cnf::ConfigMap;
	use surrealdb_kvs::TransactionType::{Read, Write};

	use crate::catalog::providers::CatalogProvider;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::dbs::{Capabilities, Session};
	use crate::key::schema::LiveEventsPrefix;
	use crate::kvs::Datastore;
	use crate::lq::event::{LiveAction, LiveEvent};
	use crate::types::PublicValue;

	/// Build an in-memory datastore with the given live-query engine selected.
	async fn new_ds(engine: &str) -> Datastore {
		build_ds(engine, None).await
	}

	/// Build an in-memory datastore with a specific engine and (optional)
	/// live-query retention (e.g. `"1us"`).
	async fn build_ds(engine: &str, retention: Option<&str>) -> Datastore {
		let (send, _recv) = crate::channel::bounded(1000);
		let mut config = ConfigMap::empty().with_key_value("live_query_engine", engine);
		if let Some(r) = retention {
			config = config.with_key_value("live_query_retention", r);
		}
		Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_auth(false)
			.with_notify(send)
			.with_config(config)
			.build_with_path("memory")
			.await
			.unwrap()
	}

	/// Create ns/db/table, returning the numeric ids for scanning the keyspace.
	async fn setup(ds: &Datastore, ns: &str, db: &str, tb: &str) -> (NamespaceId, DatabaseId) {
		let tx = ds.transaction(Write).await.unwrap();
		let dbdef = tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let ses = Session::owner().with_ns(ns).with_db(db);
		ds.execute(&format!("DEFINE TABLE {tb}"), &ses, None).await.unwrap();
		(dbdef.namespace_id, dbdef.database_id)
	}

	/// Read every live-query event persisted for the database's dedicated keyspace.
	async fn live_events(ds: &Datastore, ns: NamespaceId, db: DatabaseId) -> Vec<LiveEvent> {
		let tx = ds.transaction(Read).await.unwrap();
		let range = LiveEventsPrefix {
			ns,
			db,
		}
		.range()
		.unwrap();
		let mut events = Vec::new();
		for (_k, v) in tx.scan(range, 1000, 0, None).await.unwrap() {
			events.extend(v.0);
		}
		tx.cancel().await.unwrap();
		events
	}

	#[tokio::test]
	async fn router_captures_live_events_only_for_subscribed_tables() {
		let ds = new_ds("router").await;
		let (ns, db, tb) = ("test", "test", "person");
		let (ns_id, db_id) = setup(&ds, ns, db, tb).await;
		let owner = Session::owner().with_ns(ns).with_db(db);
		// A write before any subscriber must NOT be captured.
		ds.execute(&format!("CREATE {tb}:0 SET n = 0"), &owner, None).await.unwrap();
		assert!(
			live_events(&ds, ns_id, db_id).await.is_empty(),
			"nothing should be captured before a subscriber exists"
		);
		// Register a subscriber, then write — now it must be captured.
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute(&format!("LIVE SELECT * FROM {tb}"), &live, None).await.unwrap();
		ds.execute(&format!("CREATE {tb}:1 SET n = 1"), &owner, None).await.unwrap();
		let events = live_events(&ds, ns_id, db_id).await;
		assert_eq!(events.len(), 1, "exactly one live event captured for the subscribed write");
		assert_eq!(events[0].action, LiveAction::Create);
		assert!(events[0].before.is_none(), "create has no before value");
		assert!(!events[0].after.is_none(), "create carries the after value");
	}

	/// The cluster-wide-correctness gate: two compute nodes sharing one storage
	/// backend (the SurrealDS model). A `LIVE` registered only on node B must
	/// cause a write on node A to capture an event — because the capture gate
	/// reads the durable, shared subscription rows, not a node-local cache. A
	/// node-local index (which has no cross-node invalidation) could never know
	/// about node B's subscription, so this is exactly the case option (b) fixes.
	#[tokio::test]
	async fn router_captures_for_subscription_created_on_another_node() {
		let ds_a = new_ds("router").await;
		let (ns, db, tb) = ("test", "test", "person");
		let (ns_id, db_id) = setup(&ds_a, ns, db, tb).await;
		// A second compute node attached to the same storage backend.
		let ds_b = ds_a.fork_for_test_with_node_id(uuid::Uuid::new_v4());
		// Subscribe on node B only.
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds_b.execute(&format!("LIVE SELECT * FROM {tb}"), &live, None).await.unwrap();
		// Write on node A: its durable capture gate observes node B's row.
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds_a.execute(&format!("CREATE {tb}:1 SET n = 1"), &owner, None).await.unwrap();
		let events = live_events(&ds_a, ns_id, db_id).await;
		assert_eq!(
			events.len(),
			1,
			"node A must capture an event for a subscription created on node B",
		);
		assert_eq!(events[0].action, LiveAction::Create);
	}

	#[tokio::test]
	async fn router_retains_before_values_for_update_and_delete() {
		let ds = new_ds("router").await;
		let (ns, db, tb) = ("test", "test", "person");
		let (ns_id, db_id) = setup(&ds, ns, db, tb).await;
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute(&format!("LIVE SELECT * FROM {tb}"), &live, None).await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute(&format!("CREATE {tb}:1 SET n = 1"), &owner, None).await.unwrap();
		ds.execute(&format!("UPDATE {tb}:1 SET n = 2"), &owner, None).await.unwrap();
		ds.execute(&format!("DELETE {tb}:1"), &owner, None).await.unwrap();
		let events = live_events(&ds, ns_id, db_id).await;
		// The whole point of the dedicated keyspace: before-values are always kept,
		// regardless of any user `CHANGEFEED` / `INCLUDE ORIGINAL` setting.
		let update = events
			.iter()
			.find(|e| e.action == LiveAction::Update)
			.expect("an update event should be captured");
		assert!(!update.before.is_none(), "update retains the before value");
		assert!(!update.after.is_none(), "update carries the after value");
		let delete = events
			.iter()
			.find(|e| e.action == LiveAction::Delete)
			.expect("a delete event should be captured");
		assert!(!delete.before.is_none(), "delete retains the before value (pre-image)");
	}

	#[tokio::test]
	async fn gc_collects_events_after_subscribers_leave() {
		// Tiny retention so events written "now" age out immediately; bootstrap so
		// this node owns the changefeed-cleanup lease that drives GC.
		let ds = build_ds("router", Some("1us")).await;
		ds.bootstrap().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		let (ns_id, db_id) = setup(&ds, ns, db, tb).await;
		// Subscribe, capture one event, then remove the only subscriber.
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let mut res = ds.execute(&format!("LIVE SELECT * FROM {tb}"), &live, None).await.unwrap();
		let lqid = match res.remove(0).result.unwrap() {
			PublicValue::Uuid(u) => u,
			other => panic!("LIVE should return a uuid, got {other:?}"),
		};
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute(&format!("CREATE {tb}:1 SET n = 1"), &owner, None).await.unwrap();
		assert_eq!(
			live_events(&ds, ns_id, db_id).await.len(),
			1,
			"event captured while subscribed"
		);
		// Kill the subscription: the in-memory index for this table is now empty,
		// but the already-written event must still be collected by retention.
		ds.execute(&format!("KILL u'{lqid}'"), &live, None).await.unwrap();
		tokio::time::sleep(Duration::from_millis(500)).await;
		ds.changefeed_process(&Duration::from_secs(1)).await.unwrap();
		assert!(
			live_events(&ds, ns_id, db_id).await.is_empty(),
			"orphaned live-query events must be GC'd by retention even with no current subscriber"
		);
	}

	#[tokio::test]
	async fn inline_engine_captures_no_live_events() {
		let ds = new_ds("inline").await;
		let (ns, db, tb) = ("test", "test", "person");
		let (ns_id, db_id) = setup(&ds, ns, db, tb).await;
		let live = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute(&format!("LIVE SELECT * FROM {tb}"), &live, None).await.unwrap();
		let owner = Session::owner().with_ns(ns).with_db(db);
		ds.execute(&format!("CREATE {tb}:1 SET n = 1"), &owner, None).await.unwrap();
		assert!(
			live_events(&ds, ns_id, db_id).await.is_empty(),
			"the Inline engine must not write to the live-query keyspace"
		);
	}
}
