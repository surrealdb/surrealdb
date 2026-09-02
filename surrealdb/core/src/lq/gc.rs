use std::borrow::Cow;
use std::time::Duration;

use anyhow::Result;
use surrealdb_kvs::TransactionType::Read;
use tokio_util::sync::CancellationToken;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::key::TypedRange;
use crate::key::schema::LiveEventsPrefix;
use crate::kvs::paging::{PageCompanion, PagedDelete, PagedOutcome};
use crate::kvs::tasklease::LeaseHandler;
use crate::kvs::{CHANGEFEED_GC_BATCH_SIZE, Datastore};
use crate::lq::event::LiveEvents;

/// Garbage-collect the dedicated live-query event keyspace.
///
/// For every database, deletes live-query events older than `retention`. This
/// runs regardless of whether the database *currently* has subscribers: events
/// written while a table had subscribers must still be collected once they age
/// out, even after every subscriber has disconnected or been killed (and after a
/// table is dropped). Gating on current subscriber presence would orphan those
/// events forever, so the only knob here is the retention window.
///
/// Databases that never produced live-query events scan an empty range, which is
/// a cheap no-op. This mirrors the changefeed GC (`crate::cf::gc`) but operates
/// on the separate `lqe` keyspace with its own retention, so it never affects
/// changefeed entries or `SHOW CHANGES`. The caller gates invocation on the
/// Router engine being active.
///
/// The backlog is sized by write throughput, so it is deleted in bounded pages,
/// each committed before the next is scanned (see [`crate::kvs::paging`]).
/// `budget` bounds the whole pass and is shared across the databases it visits,
/// so one large backlog cannot take the whole pass. As in the changefeed
/// collector, every database visited is granted a share of what the budget has
/// left, so each makes progress on every pass however many databases there are.
#[instrument(level = "trace", target = "surrealdb::core::lq", skip_all)]
pub async fn gc_all_at(
	ds: &Datastore,
	lh: &LeaseHandler,
	retention: Duration,
	canceller: &CancellationToken,
	budget: &mut u64,
) -> Result<()> {
	// A zero retention would delete everything up to "now"; treat it as disabled.
	if retention.is_zero() {
		return Ok(());
	}
	let stale = stale_ranges(ds, retention).await?;
	let mut remaining = stale.len() as u64;
	for range in stale {
		Datastore::ensure_not_cancelled(canceller)?;
		// This database's slice of what the pass has left, across the databases
		// still to spend from it, capped at the budget so the tail of a pass
		// hands out only what remains. Floored at one key rather than one page:
		// the page size caps a transaction rather than setting a minimum, so a
		// sub-page share still deletes, and flooring at a page would let the
		// leading `budget / page` databases spend the whole pass and leave the
		// rest of the catalog permanently uncollected.
		let share = (*budget / remaining.max(1)).max(1).min(*budget);
		remaining = remaining.saturating_sub(1);
		if share == 0 {
			break;
		}
		let mut left = share;
		let outcome = PagedDelete {
			window: range,
			companion: PageCompanion::None,
			expunge: false,
			page: CHANGEFEED_GC_BATCH_SIZE,
		}
		.run(ds, lh, canceller, &mut left)
		.await?;
		// Charge what the database deleted, not what it was offered.
		*budget = budget.saturating_sub(share.saturating_sub(left));
		// The backlog is collected in key order from a fixed lower bound, so
		// handing the rest to the next lease holder costs nothing: it resumes at
		// the head of what survives. The handover is read from the delete's own
		// outcome, because a lease check is throttled to at most one datastore
		// read per maintenance period and so a fresh check here can answer from
		// the throttle rather than from the datastore.
		if outcome == PagedOutcome::LeaseLost || *budget == 0 {
			break;
		}
		yield_now!();
	}
	Ok(())
}

/// Every database's live-query events older than the retention watermark.
///
/// Read in one short-lived read transaction, so no snapshot is held across the
/// write transactions the deletes then commit.
async fn stale_ranges(ds: &Datastore, retention: Duration) -> Result<Vec<TypedRange<LiveEvents>>> {
	let txn = ds.transaction(Read).await?;
	let res = async {
		let ts_impl = txn.timestamp_impl();
		let ts = txn.timestamp().await?;
		// Watermark cutoff = now - retention.
		let watermark = ts.sub_checked(retention).unwrap_or_else(|| ts_impl.earliest());
		// The storage engine's earliest timestamp, the fixed lower bound of every
		// database's stale range. A pass interrupted part way needs no cursor
		// against a fixed bound: everything it deleted is committed and gone, so
		// the next scan from here lands on the first event that survives.
		let mut buf = [0u8; _];
		let beg = ts_impl.earliest().encode(&mut buf).to_vec();
		let mut buf = [0u8; _];
		let end = watermark.encode(&mut buf);
		let mut ranges = Vec::new();
		for ns in txn.all_ns(None).await?.as_ref() {
			for db in txn.all_db(ns.namespace_id, None).await?.as_ref() {
				// Up to, but not including, the watermark: events at the
				// watermark are still inside the retention window.
				ranges.push(
					LiveEventsPrefix {
						ns: db.namespace_id,
						db: db.database_id,
					}
					.range_where(Cow::Borrowed(beg.as_slice())..Cow::Borrowed(end))?,
				);
			}
		}
		Ok(ranges)
	}
	.await;
	let _ = txn.cancel().await;
	res
}
