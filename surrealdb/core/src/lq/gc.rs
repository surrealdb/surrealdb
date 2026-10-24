use std::borrow::Cow;
use std::time::Duration;

use anyhow::Result;
use surrealdb_kvs::TransactionType::Read;
use tokio_util::sync::CancellationToken;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::TypedRange;
use crate::key::schema::LiveEventsPrefix;
use crate::kvs::paging::{PageCompanion, PagedDelete, delete_sharing_budget};
use crate::kvs::tasklease::LeaseHandler;
use crate::kvs::{CHANGEFEED_GC_BATCH_SIZE, Datastore};
use crate::lq::event::LiveEvents;

/// Garbage-collect the dedicated live-query event keyspace.
///
/// For each of `dbs`, deletes live-query events older than `retention`. `dbs`
/// is the page of the catalog a pass collects, by id: the databases the
/// collector's sweep of the catalog reached this pass, so a sweep visits every
/// database once however many there are and a pass reads no more of the
/// catalog than its steps — with those the sweep reported by id, since the
/// window here is the datastore's and no catalog definition bears on it: a
/// database skipped whole for a table definition the sweep cannot read, and
/// every database subtree the sweep's probe of the keyspace reaches once its
/// walk has passed a namespace or database definition it cannot read, which
/// hides the subtree's id; see [`Datastore::live_query_gc_process`]. This runs
/// regardless of whether the database *currently* has subscribers: events
/// written while a table had subscribers must still be collected once they age
/// out, even after every subscriber has disconnected or been killed (and after a
/// table is dropped). Gating on current subscriber presence would orphan those
/// events forever, so the only knob here is the retention window. Nothing holds
/// an event for a router either: a node's router that has yet to deliver an
/// event when the window passes it loses it, and reports a lap found behind
/// the window; see [`crate::lq::router`].
///
/// Databases that never produced live-query events scan an empty range, which is
/// a cheap no-op. This mirrors the changefeed GC (`crate::cf::gc`) but operates
/// on the separate `lqe` keyspace with its own retention and its own schedule,
/// so it never affects changefeed entries or `SHOW CHANGES`, and disabling
/// changefeed collection leaves it running. The caller runs it whichever
/// engine delivers live queries, so the events the Router engine captured
/// before the datastore was restarted under another engine still age out.
///
/// The backlog is sized by write throughput, so it is deleted in bounded pages,
/// each committed before the next is scanned (see [`crate::kvs::paging`]).
/// `budget` bounds the whole pass and is shared across the databases it visits,
/// so one large backlog cannot take the whole pass. As in the changefeed
/// collector, every database visited is granted a share of what the budget has
/// left, so each makes progress on every pass however many databases there are,
/// and what one leaves goes to those with backlog remaining (see
/// [`delete_sharing_budget`]).
///
/// Reports whether the lease is still held, as the changefeed collector does,
/// so a pass that lost it is reported under the holder's standing rather than
/// its own. A lost lease cannot be re-derived from
/// [`LeaseHandler::try_maintain_lease`] here: the check that lost it consumed
/// this maintenance period's one datastore read, so the next call answers
/// `true` from the throttle without asking.
#[cfg_attr(
	feature = "observability",
	tracing::instrument(level = "trace", target = "surrealdb::core::lq", skip_all)
)]
pub async fn gc_databases(
	ds: &Datastore,
	lh: &LeaseHandler,
	retention: Duration,
	canceller: &CancellationToken,
	budget: &mut u64,
	dbs: &[(NamespaceId, DatabaseId)],
) -> Result<bool> {
	// A zero retention would delete everything up to "now"; treat it as disabled.
	if retention.is_zero() {
		return Ok(true);
	}
	let stale = stale_ranges(ds, retention, dbs).await?;
	// The backlog is collected in key order from a fixed lower bound, so
	// handing the rest to the next lease holder costs nothing: it resumes at
	// the head of what survives.
	delete_sharing_budget(ds, lh, canceller, budget, stale, |range| PagedDelete {
		window: range.clone(),
		companion: PageCompanion::None,
		expunge: false,
		page: CHANGEFEED_GC_BATCH_SIZE,
	})
	.await
}

/// Each of `dbs`'s live-query events older than the retention watermark.
///
/// Read in one short-lived read transaction, so no snapshot is held across the
/// write transactions the deletes then commit.
async fn stale_ranges(
	ds: &Datastore,
	retention: Duration,
	dbs: &[(NamespaceId, DatabaseId)],
) -> Result<Vec<TypedRange<LiveEvents>>> {
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
		for (ns, db) in dbs {
			// Up to, but not including, the watermark: events at the
			// watermark are still inside the retention window.
			ranges.push(
				LiveEventsPrefix {
					ns: *ns,
					db: *db,
				}
				.range_where(Cow::Borrowed(beg.as_slice())..Cow::Borrowed(end))?,
			);
		}
		Ok(ranges)
	}
	.await;
	let _ = txn.cancel().await;
	res
}
