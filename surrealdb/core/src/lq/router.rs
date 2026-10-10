//! Per-node live-query router.
//!
//! The router is the delivery half of the inverted engine. While the write path
//! only captures before/after values into the dedicated `lqe` keyspace
//! ([`crate::lq::event`]), the router tails that keyspace **off** the write path,
//! reconstructs each change, and runs the matching/permission/projection/FETCH
//! pipeline on the subscriber side ([`crate::lq::subscriber`]). Under the Router
//! engine this is the *only* delivery path — the write path's inline
//! notification stage becomes a no-op — so a
//! write's cost no longer scales with the number of live subscribers.
//!
//! The router runs on every node with **no lease**: each node tails the `lqe`
//! entries readable on its local replica and delivers only to the subscriptions
//! it owns (enforced by `MessageBroker::should_emit`). One pass is driven per
//! tick by the engine's background task ([`Datastore::live_query_router_process`]).

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use parking_lot::Mutex;
use surrealdb_kvs::TransactionType::Read;
use surrealdb_strand::TableName;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{DatabaseDefinition, DatabaseId, NamespaceId};
use crate::dbs::MessageBroker;
use crate::key::schema::{LiveEventsKey, LiveEventsPrefix};
use crate::key::{KVKeyDecode, KVValue};
use crate::kvs::{Datastore, Transaction};
use crate::lq::event::LiveEvents;
use crate::lq::subscriber::TableReplay;

/// Per-node live-query router state: how far delivery has reached through the
/// global versionstamp space of the `lqe` keyspace.
///
/// The state is **in-memory**. Its baseline is set to the safe watermark when
/// the datastore is built, so a freshly started node does not replay retained
/// history to its currently-connected subscribers — matching the inline
/// engine, which only notifies writes that occur after a subscription is
/// registered. Durable, resumable cursors (which let a reconnecting client
/// replay a gap) are the subject of the at-least-once delivery phase.
#[derive(Debug, Default)]
pub(crate) struct LiveQueryRouter {
	/// Held for the whole of a pass, so passes never overlap: two passes that
	/// began from the same progress would both deliver what lies beyond it.
	pass: tokio::sync::Mutex<()>,
	progress: Mutex<Progress>,
}

/// How far delivery has reached: one cursor shared by every table, and the
/// tables held behind it because their delivery failed.
///
/// Progress is kept per table so that one table that cannot be delivered
/// neither blocks nor re-delivers any other: a pass always moves the shared
/// cursor to its watermark, and a table it failed on resumes on later passes
/// from the last versionstamp it delivered.
#[derive(Debug, Default, Clone)]
struct Progress {
	/// Highest versionstamp delivered for every table not held in `behind`.
	/// `None` until the baseline is established.
	cursor: Option<u128>,
	/// The databases holding at least one table behind `cursor`.
	behind: HashMap<(NamespaceId, DatabaseId), Behind>,
}

/// The tables of one database whose delivery is behind the shared cursor.
#[derive(Debug, Default, Clone)]
struct Behind {
	/// Set when the database's events could not be read: every table not in
	/// `tables` is delivered up to here.
	database: Option<u128>,
	/// Highest versionstamp delivered for each table whose replay failed.
	tables: HashMap<TableName, u128>,
}

impl Behind {
	/// The highest versionstamp delivered for `tb`.
	fn table(&self, tb: &TableName, cursor: u128) -> u128 {
		self.tables.get(tb).copied().or(self.database).unwrap_or(cursor)
	}

	/// The lowest versionstamp delivered for any table of the database, which
	/// the database's scan starts from.
	fn floor(&self, cursor: u128) -> u128 {
		self.tables.values().copied().chain(self.database).fold(cursor, u128::min)
	}
}

impl LiveQueryRouter {
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// Begin a pass against the safe watermark `safe_vs`: returns the progress
	/// to deliver from, or `None` when this is the first (baseline) pass —
	/// which sets the cursor to the watermark and delivers nothing, so a freshly
	/// started node does not replay history to its currently-connected
	/// subscribers.
	///
	/// Synchronous on purpose: the [`MutexGuard`](parking_lot::MutexGuard) is
	/// `!Send` and must never be held across an `await` in [`process`].
	fn begin_pass(&self, safe_vs: u128) -> Option<Progress> {
		let mut guard = self.progress.lock();
		match guard.cursor {
			None => {
				guard.cursor = Some(safe_vs);
				None
			}
			Some(_) => Some(guard.clone()),
		}
	}

	/// Record the progress a pass finished with. Passes never overlap, so
	/// nothing else wrote it since [`Self::begin_pass`]. Synchronous for the
	/// same `!Send` reason.
	fn finish_pass(&self, progress: Progress) {
		*self.progress.lock() = progress;
	}

	/// Seed the baseline cursor eagerly, before the datastore can accept any
	/// connection. Establishing the baseline at construction (rather than lazily
	/// on the first pass) guarantees that every subscription — and therefore
	/// every write a subscriber expects to observe — happens *after* the
	/// baseline, so no captured event is ever skipped as "history" while a
	/// subscriber was already waiting for it.
	pub(crate) fn set_baseline(&self, safe_vs: u128) {
		self.progress.lock().cursor = Some(safe_vs);
	}
}

/// Run one tail-and-deliver pass for the Router engine.
///
/// Delivers every `lqe` event with a versionstamp in `(delivered, W]`, where
/// `delivered` is the table's progress and `W` is the backend's **safe
/// watermark** (`Transaction::safe_timestamp`) — the versionstamp at or below
/// which every commit is final and visible. Bounding delivery and cursor
/// advancement by `W` (rather than by the highest event seen) is what makes
/// this correct on a backend whose commit log is non-linear: the router never
/// advances past a commit that could still become visible later with a lower
/// versionstamp. On a single-oracle backend `W` is simply "now", so every
/// committed event is delivered immediately.
///
/// Events are grouped per table (preserving ascending versionstamp scan order)
/// and replayed through the subscriber-side compute using the datastore's
/// broker. A failure is contained to the table, or to the database when its
/// events cannot be read: it is logged, that table or database is held at what
/// it delivered, and the pass continues. Only a failure before anything is
/// delivered — opening the transaction or listing the databases — fails the
/// pass, and it leaves the progress unchanged.
pub(crate) async fn process(ds: &Datastore, router: &LiveQueryRouter) -> Result<()> {
	// Without a delivery broker there are no subscribers to notify.
	let Some(broker) = ds.live_query_broker() else {
		return Ok(());
	};
	let _pass = router.pass.lock().await;
	let txn = Arc::new(ds.transaction(Read).await?);
	let res = deliver_pass(ds, router, &txn, broker).await;
	let _ = txn.cancel().await;
	res
}

async fn deliver_pass(
	ds: &Datastore,
	router: &LiveQueryRouter,
	txn: &Arc<Transaction>,
	broker: Arc<dyn MessageBroker>,
) -> Result<()> {
	// The safe/closed watermark: deliver and advance only up to here.
	let safe_vs = txn.safe_timestamp().await?.as_versionstamp();
	// On the first pass, set the baseline to the watermark and deliver nothing:
	// everything already committed (vs <= watermark) is treated as history.
	let Some(Progress {
		cursor: Some(cursor),
		behind: mut prior,
	}) = router.begin_pass(safe_vs)
	else {
		return Ok(());
	};
	// Nothing new is safe to deliver yet, and nothing is held behind.
	if safe_vs <= cursor && prior.is_empty() {
		return Ok(());
	}
	// List every database before delivering anything, so a failure here leaves
	// nothing delivered that a retry would deliver again.
	let mut dbs = Vec::new();
	for ns in txn.all_ns(None).await?.iter() {
		dbs.push((ns.name.clone(), txn.all_db(ns.namespace_id, None).await?));
	}
	// Databases that no longer exist drop out of the progress here, since only
	// the databases listed are carried into it.
	let mut behind = HashMap::new();
	for (ns, dbs) in &dbs {
		for db in dbs.iter() {
			let key = (db.namespace_id, db.database_id);
			let prior = prior.remove(&key).unwrap_or_default();
			let held =
				match deliver_database(ds, txn, &broker, ns, db, cursor, &prior, safe_vs).await {
					Ok(held) => held,
					Err(e) => {
						error!(
							"Live-query router could not read the events of database `{}` in namespace `{ns}`: {e}",
							db.name
						);
						Behind {
							database: Some(prior.database.unwrap_or(cursor)),
							tables: prior.tables,
						}
					}
				};
			if held.database.is_some() || !held.tables.is_empty() {
				behind.insert(key, held);
			}
		}
	}
	// Every table not held behind has delivered everything up to the watermark,
	// and nothing at or below it can appear later, so the next pass resumes from
	// here without re-delivering or skipping.
	router.finish_pass(Progress {
		cursor: Some(safe_vs.max(cursor)),
		behind,
	});
	Ok(())
}

/// Deliver one database's events above each table's progress, up to `safe_vs`.
///
/// Returns the tables whose replay failed, each at the highest versionstamp it
/// delivered. An error means the database's events could not be read, and
/// nothing was delivered.
#[allow(clippy::too_many_arguments)]
async fn deliver_database(
	ds: &Datastore,
	txn: &Arc<Transaction>,
	broker: &Arc<dyn MessageBroker>,
	ns: &str,
	db: &DatabaseDefinition,
	cursor: u128,
	prior: &Behind,
	safe_vs: u128,
) -> Result<Behind> {
	let ts_impl = txn.timestamp_impl();
	// Encode the lower bound (the lowest progress of any table) for the range
	// scan. The bound is inclusive, so events at exactly that versionstamp are
	// scanned; those were already delivered, so the scan loop skips them.
	let floor = prior.floor(cursor);
	let floor_ts = ts_impl.create_from_versionstamp(floor).unwrap_or_else(|| ts_impl.earliest());
	let mut floor_buf = [0u8; _];
	let floor_bytes = floor_ts.encode(&mut floor_buf);
	// Everything from the floor to the end of this database's events.
	let range = LiveEventsPrefix {
		ns: db.namespace_id,
		db: db.database_id,
	}
	.range_where(Cow::Borrowed(floor_bytes)..)?;
	// Group this database's events per table, each batch tagged with its
	// versionstamp, preserving the ascending (versionstamp, table) scan order
	// within each table's vec.
	let mut per_table: HashMap<TableName, Vec<(u128, LiveEvents)>> = HashMap::new();
	// The values stay encoded through the window check below: the lower bound
	// is inclusive and the upper bound is the end of the band, so most of what
	// this scan returns is outside the delivery window and is dropped unread.
	for (k, v) in txn.scan_raw(range, u32::MAX, 0, None).await? {
		let key = LiveEventsKey::decode_key(&k)?;
		let vs = ts_impl.decode(key.ts.as_ref())?.as_versionstamp();
		// Skip already-delivered events and anything not yet safe.
		if vs <= prior.table(&key.tb, cursor) || vs > safe_vs {
			continue;
		}
		match LiveEvents::kv_decode_value(&v, ()) {
			Ok(events) => per_table.entry(key.tb.into_owned()).or_default().push((vs, events)),
			// No later pass can decode it either, so holding the table behind it
			// would stop the table's delivery until the event is collected.
			Err(e) => error!(
				"Live-query router skipped an undecodable event for table `{}` in database `{}`: {e}",
				key.tb, db.name
			),
		}
	}
	let mut held = Behind::default();
	for (tb, batches) in per_table {
		let mut delivered = prior.table(&tb, cursor);
		let res = async {
			let Some(replay) =
				TableReplay::resolve(ds, Arc::clone(txn), ns, &db.name, &tb, Arc::clone(broker))
					.await?
			else {
				// The table was removed, and its subscriptions with it.
				return Ok(());
			};
			for (vs, events) in &batches {
				replay.replay(&events.0).await?;
				delivered = *vs;
			}
			anyhow::Ok(())
		}
		.await;
		if let Err(e) = res {
			error!(
				"Live-query router could not deliver the events of table `{tb}` in database `{}`: {e}",
				db.name
			);
			held.tables.insert(tb, delivered);
		}
	}
	Ok(held)
}
