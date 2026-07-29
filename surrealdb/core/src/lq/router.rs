//! Per-node live-query router.
//!
//! The router is the delivery half of the inverted engine. While the write path
//! only captures before/after values into the dedicated `lqe` keyspace
//! ([`crate::lq::event`]), the router tails that keyspace **off** the write path,
//! reconstructs each change, and runs the matching/permission/projection/FETCH
//! pipeline on the subscriber side ([`crate::lq::subscriber`]). Under the Router
//! engine this is the *only* delivery path — the inline
//! [`Document::process_table_lives`](crate::doc::Document) becomes a no-op — so a
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
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKeyDecode, KVRange, KVValue, lqe};
use crate::kvs::Datastore;
use crate::lq::event::{LiveEvent, LiveEvents};
use crate::lq::subscriber::replay_table_live_events;

/// Per-node live-query router state: the tail cursor over the global
/// versionstamp space of the `lqe` keyspace.
///
/// The cursor is **in-memory** and is initialised to "now" on the first pass so
/// a freshly started node does not replay retained history to its
/// currently-connected subscribers — matching the inline engine, which only
/// notifies writes that occur after a subscription is registered. Durable,
/// resumable cursors (which let a reconnecting client replay a gap) are the
/// subject of the at-least-once delivery phase.
#[derive(Debug, Default)]
pub(crate) struct LiveQueryRouter {
	/// Highest versionstamp already delivered; the next pass scans strictly
	/// after it. `None` until the first pass establishes the baseline.
	cursor: Mutex<Option<u128>>,
}

impl LiveQueryRouter {
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// Begin a pass against the safe watermark `safe_vs`: returns the cursor
	/// (highest versionstamp already delivered) to scan strictly after, or `None`
	/// when this is the first (baseline) pass — which sets the cursor to the
	/// watermark and delivers nothing, so a freshly started node does not replay
	/// history to its currently-connected subscribers.
	///
	/// Synchronous on purpose: the [`MutexGuard`](parking_lot::MutexGuard) is
	/// `!Send` and must never be held across an `await` in [`process`].
	fn begin_pass(&self, safe_vs: u128) -> Option<u128> {
		let mut guard = self.cursor.lock();
		match *guard {
			None => {
				*guard = Some(safe_vs);
				None
			}
			Some(c) => Some(c),
		}
	}

	/// Advance the cursor to the safe watermark this pass delivered up to.
	/// Synchronous for the same `!Send` reason as [`Self::begin_pass`].
	fn advance(&self, safe_vs: u128) {
		*self.cursor.lock() = Some(safe_vs);
	}

	/// Seed the baseline cursor eagerly, before the datastore can accept any
	/// connection. Establishing the baseline at construction (rather than lazily
	/// on the first pass) guarantees that every subscription — and therefore
	/// every write a subscriber expects to observe — happens *after* the
	/// baseline, so no captured event is ever skipped as "history" while a
	/// subscriber was already waiting for it.
	pub(crate) fn set_baseline(&self, safe_vs: u128) {
		*self.cursor.lock() = Some(safe_vs);
	}
}

/// Run one tail-and-deliver pass for the Router engine.
///
/// Delivers every `lqe` event with a versionstamp in `(cursor, W]`, where `W` is
/// the backend's **safe watermark** (`Transaction::safe_timestamp`) — the
/// versionstamp at or below which every commit is final and visible. Bounding
/// delivery and cursor advancement by `W` (rather than by the highest event
/// seen) is what makes this correct on a backend whose commit log is non-linear:
/// the router never advances past a commit that could still become visible later
/// with a lower versionstamp. On a single-oracle backend `W` is simply "now", so
/// every committed event is delivered immediately.
///
/// Events are grouped per table (preserving ascending versionstamp scan order)
/// and replayed through the subscriber-side compute using the datastore's broker.
pub(crate) async fn process(ds: &Datastore, router: &LiveQueryRouter) -> Result<()> {
	// Without a delivery broker there are no subscribers to notify.
	let Some(broker) = ds.live_query_broker() else {
		return Ok(());
	};
	let txn = Arc::new(ds.transaction(Read).await?);
	let ts_impl = txn.timestamp_impl();
	// The safe/closed watermark: deliver and advance only up to here.
	let safe_vs = txn.safe_timestamp().await?.as_versionstamp();

	// On the first pass, set the baseline to the watermark and deliver nothing:
	// everything already committed (vs <= watermark) is treated as history.
	let Some(cursor) = router.begin_pass(safe_vs) else {
		txn.cancel().await?;
		return Ok(());
	};
	// Nothing new is safe to deliver yet.
	if safe_vs <= cursor {
		txn.cancel().await?;
		return Ok(());
	}

	// Encode the lower bound (the cursor) for the range scans. `prefix_ts(cursor)`
	// includes events at exactly `cursor`; those were already delivered, so the
	// scan loop skips `vs <= cursor`.
	let cursor_ts = ts_impl.create_from_versionstamp(cursor).unwrap_or_else(|| ts_impl.earliest());
	let mut cursor_buf = [0u8; _];
	let cursor_bytes = cursor_ts.encode(&mut cursor_buf);

	let nss = txn.all_ns(None).await?;
	for ns in nss.iter() {
		let dbs = txn.all_db(ns.namespace_id, None).await?;
		for db in dbs.iter() {
			let start = lqe::LqeTsRange {
				prefix: DatabaseRoot {
					ns: db.namespace_id,
					db: db.database_id,
				},

				ts: Cow::Borrowed(cursor_bytes),
			}
			.encode_bound()?;
			let end = lqe::LqePrefix {
				prefix: DatabaseRoot {
					ns: db.namespace_id,
					db: db.database_id,
				},
			}
			.encode_bound()?
			.next_neighbour_expect();
			// Group this database's events per table, preserving the ascending
			// (versionstamp, table) scan order within each table's vec.
			let mut per_table: HashMap<TableName, Vec<LiveEvent>> = HashMap::new();
			for (k, v) in txn.scan((start..end).into(), u32::MAX, 0, None).await? {
				let key = lqe::Lqe::decode_key(&k)?;
				let vs = ts_impl.decode(key.ts.as_ref())?.as_versionstamp();
				// Skip already-delivered events and anything not yet safe.
				if vs <= cursor || vs > safe_vs {
					continue;
				}
				let events = LiveEvents::kv_decode_value(&v, ())?;
				per_table.entry(key.tb.into_owned()).or_default().extend(events.0);
			}
			for (tb, events) in per_table {
				replay_table_live_events(
					ds,
					Arc::clone(&txn),
					&ns.name,
					&db.name,
					&tb,
					&events,
					Arc::clone(&broker),
				)
				.await?;
			}
		}
	}
	txn.cancel().await?;

	// Advance the cursor to the watermark: everything at or below it is now
	// considered (delivered or pre-baseline), and nothing below it can appear
	// later, so the next pass resumes from here without re-delivering or skipping.
	router.advance(safe_vs);
	Ok(())
}
