use std::borrow::Cow;
use std::time::Duration;

use anyhow::Result;
use surrealdb_kvs::TransactionType::Read;
use tokio_util::sync::CancellationToken;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{DatabaseDefinition, DatabaseId, NamespaceId, TableDefinition};
use crate::cf::TableMutations;
use crate::key::TypedRange;
use crate::key::schema::ChangeFeedPrefix;
use crate::kvs::paging::{PageCompanion, PagedDelete, PagedOutcome};
use crate::kvs::tasklease::LeaseHandler;
use crate::kvs::{CHANGEFEED_GC_BATCH_SIZE, Datastore, Transaction};

/// One database's stale changefeed entries: everything from the storage engine's
/// earliest timestamp up to, but not including, the database's retention
/// watermark.
struct Target<R> {
	ns: NamespaceId,
	db: DatabaseId,
	stale: R,
	/// The encoded watermark `stale`'s upper bound was built from. Revalidated
	/// inside every page transaction, because the retention behind it is catalog
	/// state a concurrent definition can extend.
	watermark: Vec<u8>,
}

/// Delete every changefeed entry that is stale as of now, across every database.
///
/// The backlog behind a database's retention watermark is sized by write
/// throughput, so it is deleted in bounded pages, each committed before the next
/// is scanned (see [`crate::kvs::paging`]). `budget` bounds the whole pass and is
/// shared across the databases it visits, so one large backlog cannot take the
/// whole pass; what a pass does not reach is collected by the next, starting at
/// the head of what survives.
///
/// Every database a pass visits is granted a share of what the budget has left,
/// so each one makes progress on every pass however many there are: the share
/// shrinks with the number of databases rather than the tail of the catalog going
/// uncollected. A share below one page still deletes, since the page size bounds
/// a transaction rather than setting a minimum.
///
/// Reports whether the lease is still held, so a caller that runs another
/// collector on the same lease stops instead of starting it. A lost lease cannot
/// be re-derived from [`LeaseHandler::try_maintain_lease`] here: the check that
/// lost it consumed this maintenance period's one datastore read, so the next
/// call answers `true` from the throttle without asking.
#[instrument(level = "trace", target = "surrealdb::core::cfs", skip_all)]
pub async fn gc_all_at(
	ds: &Datastore,
	lh: &LeaseHandler,
	canceller: &CancellationToken,
	budget: &mut u64,
) -> Result<bool> {
	let targets = targets(ds).await?;
	let mut remaining = targets.len() as u64;
	for target in targets {
		Datastore::ensure_not_cancelled(canceller)?;
		trace!("Performing changefeed garbage collection on {}:{}", target.ns, target.db);
		// This database's slice of what the pass has left, across the databases
		// still to spend from it, capped at the budget so the tail of a pass
		// hands out only what remains. A share smaller than a page is still
		// progress — the page size caps a transaction, it is not a minimum — so
		// the share is floored at one key rather than one page. Flooring it at a
		// page would let the first `budget / page` databases spend the whole
		// pass, and since targets are rebuilt in catalog order every pass with
		// no position carried between them, the databases behind that prefix
		// would never be visited at all.
		let share = (*budget / remaining.max(1)).max(1).min(*budget);
		remaining = remaining.saturating_sub(1);
		if share == 0 {
			break;
		}
		let mut left = share;
		let outcome = PagedDelete {
			window: target.stale,
			companion: PageCompanion::ChangefeedRetention {
				ns: target.ns,
				db: target.db,
				watermark: &target.watermark,
			},
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
		// Budget exhaustion ends the pass with the lease still held; losing the
		// lease ends it without.
		if outcome == PagedOutcome::LeaseLost {
			return Ok(false);
		}
		if *budget == 0 {
			break;
		}
		yield_now!();
	}
	Ok(true)
}

/// The databases with a changefeed retention, each with the range its watermark
/// has made stale.
///
/// A database's retention is the longest of its own and those of its tables, so a
/// table configured to keep changes longer than its database is honoured.
/// Databases with no retention anywhere are omitted: they have nothing to
/// collect.
///
/// Read in one short-lived read transaction, so no snapshot is held across the
/// write transactions the deletes then commit.
async fn targets(ds: &Datastore) -> Result<Vec<Target<TypedRange<TableMutations>>>> {
	let txn = ds.transaction(Read).await?;
	let res = async {
		let ts_impl = txn.timestamp_impl();
		let ts = txn.timestamp().await?;
		// The storage engine's earliest timestamp, the fixed lower bound of every
		// database's stale range. A pass interrupted part way needs no cursor
		// against a fixed bound: everything it deleted is committed and gone, so
		// the next scan from here lands on the first entry that survives.
		let mut buf = [0u8; _];
		let earliest = ts_impl.earliest().encode(&mut buf).to_vec();
		let mut targets = Vec::new();
		for ns in txn.all_ns(None).await?.as_ref() {
			for db in txn.all_db(ns.namespace_id, None).await?.as_ref() {
				let tbs = txn.all_tb(db.namespace_id, db.database_id, None).await?;
				let expiry = retention_of(db, tbs.as_ref());
				if expiry.is_zero() {
					continue;
				}
				let watermark = ts.sub_checked(expiry).unwrap_or_else(|| ts_impl.earliest());
				let mut buf = [0u8; _];
				let end = watermark.encode(&mut buf);
				// Up to, but not including, the watermark: the watermark's own
				// changes are still needed.
				let stale = ChangeFeedPrefix {
					ns: db.namespace_id,
					db: db.database_id,
				}
				.range_where(Cow::Borrowed(earliest.as_slice())..Cow::Borrowed(end))?;
				targets.push(Target {
					ns: db.namespace_id,
					db: db.database_id,
					stale,
					watermark: end.to_vec(),
				});
			}
		}
		Ok(targets)
	}
	.await;
	let _ = txn.cancel().await;
	res
}

/// A database's changefeed retention: the longest of its own and those of its
/// tables, so a table configured to keep changes longer than its database is
/// honoured. `Duration::ZERO` means no retention is configured anywhere, and so
/// nothing to collect.
fn retention_of(db: &DatabaseDefinition, tbs: &[TableDefinition]) -> Duration {
	let db_expiry = db.changefeed.map(|v| v.expiry).unwrap_or_default();
	let tb_expiry = tbs
		.iter()
		.filter_map(|tb| tb.changefeed.as_ref())
		.map(|cf| cf.expiry)
		.filter(|&dur| !dur.is_zero())
		.max()
		.unwrap_or(Duration::ZERO);
	db_expiry.max(tb_expiry)
}

/// Whether this database's retention still makes everything below `watermark`
/// stale.
///
/// The watermark a stale range was built from is only valid while the retention
/// behind it stands. A `DEFINE DATABASE` or `DEFINE TABLE` committed since can
/// have extended it, which moves the watermark earlier and makes entries the
/// range still names live again — and a deleted changefeed entry cannot be
/// recovered. Read inside the transaction that deletes a page, so the answer
/// covers that page and, on a conflict-serializing backend, arms the commit
/// against a policy landing after this read.
///
/// A watermark computed now sits *later* than the one a pass started with
/// whenever retention is unchanged, since wall-clock has advanced. Only an
/// extended retention moves it earlier, which is what this discriminates.
///
/// Reports `false` — stop deleting — for a database that has since lost its
/// retention or been removed outright: the first has nothing to collect, and the
/// second is reclaimed by its own tombstone rather than page by page here.
pub(crate) async fn retention_still_reaches(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	watermark: &[u8],
) -> Result<bool> {
	// Give this page and any retention change a key in common, so a change that
	// commits between the read below and this page's commit rejects the page
	// rather than being ignored. The catalog reads alone do not: nothing writes
	// them here, and a last-writer-wins backend validates no read set.
	txn.arm_changefeed_retention_fence(ns, db).await?;
	let dbs = txn.all_db(ns, None).await?;
	let Some(def) = dbs.as_ref().iter().find(|d| d.database_id == db) else {
		return Ok(false);
	};
	let tbs = txn.all_tb(ns, db, None).await?;
	let expiry = retention_of(def, tbs.as_ref());
	if expiry.is_zero() {
		return Ok(false);
	}
	let ts_impl = txn.timestamp_impl();
	let ts = txn.timestamp().await?;
	let current = ts.sub_checked(expiry).unwrap_or_else(|| ts_impl.earliest());
	let mut buf = [0u8; _];
	Ok(current.encode(&mut buf) >= watermark)
}
