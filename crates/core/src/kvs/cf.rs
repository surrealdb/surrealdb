use crate::cf::Fence;
use crate::err::Error;
use crate::kvs::Datastore;
use crate::kvs::Key;
use crate::kvs::{LockType::*, TransactionType::*};
use crate::vs::VersionStamp;
use std::ops::Range;

const TARGET: &str = "surrealdb::core::kvs::ds";

// The number of changefeed keys collected in one committed page.
// Every page is its own transaction, so this bounds the write set of each
// transaction the collector opens independently of how large a backlog has
// grown: the retention of a database is set by its write traffic, and years of
// accumulation must not decide the size of one transaction.
const CHANGEFEED_GC_BATCH_SIZE: u32 = 1_000;

// The number of keys one collection of one range may destroy.
// A range whose upper bound is open ends where the database's writes have
// reached, which moves while the collection runs, so a database written to at
// or above the rate this deletes would never see its range drained. The
// collection returns when this is spent and the tick after it resumes, which
// bounds a pass by the catalog rather than by a writer, keeps the ranges behind
// it from being starved, and lets the task see a shutdown between two passes.
const CHANGEFEED_GC_PASS_KEY_BUDGET: u64 = 100_000;

impl Datastore {
	/// Saves the current timestamp for each database's current versionstamp.
	///
	/// A `ts` key correlates a wall-clock second with the database versionstamp
	/// current at that moment. Its only readers are changefeed garbage
	/// collection, which needs it to turn a retention window into a watermark
	/// versionstamp, and `SHOW CHANGES SINCE <datetime>`, which needs it to turn
	/// a datetime into a scan start. Both are meaningful only where changefeed
	/// entries exist, so a `ts` key is written only for a database which defines
	/// a changefeed on itself or on at least one of its tables. A database which
	/// defines none records no changefeed entries, and so has no versionstamp
	/// timeline to keep.
	///
	/// The length of the retention window does not come into it: `CHANGEFEED 0s`
	/// is a changefeed that records an entry per write and keeps it until the
	/// next collection, and `SHOW CHANGES SINCE <datetime>` on it needs the same
	/// timeline as any other.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub(crate) async fn changefeed_versionstamp(
		&self,
		ts: u64,
	) -> Result<Option<VersionStamp>, Error> {
		// Store the latest versionstamp
		let mut vs: Option<VersionStamp> = None;
		// Create a new transaction
		let txn = self.transaction(Write, Optimistic).await?;
		// Fetch all namespaces
		let nss = catch!(txn, txn.all_ns().await);
		// Loop over all namespaces
		for ns in nss.iter() {
			// Get the namespace name
			let ns = &ns.name;
			// Fetch all namespaces
			let dbs = catch!(txn, txn.all_db(ns).await);
			// Loop over all databases
			for db in dbs.iter() {
				// Get whether the database itself defines a changefeed
				let db_cf = db.changefeed.is_some();
				// Get the database name
				let db = &db.name;
				// Fetch all tables
				let tbs = catch!(txn, txn.all_tb(ns, db, None).await);
				// Get whether any of its tables defines one
				let tb_cf = tbs.as_ref().iter().any(|tb| tb.changefeed.is_some());
				// Skip databases which define no changefeed at all
				if !db_cf && !tb_cf {
					continue;
				}
				// TODO(SUR-341): This is incorrect, it's a [ns,db] to vs pair
				// It's safe for now, as it is unused but either the signature must change
				// to include {(ns, db): (ts, vs)} mapping, or we don't return it
				//
				// The transaction lock is released before `catch!` runs, because
				// cancelling the transaction takes that same lock.
				let res = txn.lock().await.set_timestamp_for_versionstamp(ts, ns, db).await;
				vs = Some(catch!(txn, res));
			}
		}
		// Commit the changes
		catch!(txn, txn.commit().await);
		// Return the version
		Ok(vs)
	}

	/// Deletes all change feed entries that are older than the timestamp.
	///
	/// The stale ranges are chosen from the catalog in a read transaction and
	/// then drained in committed pages, each its own transaction. The catalog
	/// bounds the first half of that; nothing bounds the second, so it is paged.
	///
	/// Each range is drained independently, so a database that cannot be collected
	/// costs its own range and not the ranges behind it. The plan is in catalog
	/// order, so giving up on the first failure would starve every database sorting
	/// after one that fails consistently, for as long as the failure lasts. The
	/// first error is still returned, once every range has been attempted.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub(crate) async fn changefeed_cleanup(&self, ts: u64) -> Result<(), Error> {
		// Choose the ranges that are stale at this timestamp
		let txn = self.transaction(Read, Optimistic).await?;
		let plan = catch!(txn, crate::cf::gc_all_at(&txn, ts).await);
		txn.cancel().await?;
		// Drain each of them a committed page at a time
		let mut failure = None;
		for (rng, fence) in plan {
			let object = format!("{}/{}", fence.ns, fence.db);
			if let Err(e) = self.changefeed_collect(rng, fence).await {
				error!(
					target: TARGET,
					"Error collecting the changefeed entries of {object}: {e}",
				);
				failure.get_or_insert(e);
			}
		}
		match failure {
			Some(e) => Err(e),
			None => Ok(()),
		}
	}

	/// Deletes one changefeed range in committed pages of at most
	/// [`CHANGEFEED_GC_BATCH_SIZE`] keys.
	///
	/// Each page resumes strictly after the last key the page before it deleted,
	/// so an interrupted collection costs a rescan of one page rather than the
	/// whole range.
	///
	/// The range was chosen from the catalog in a transaction that has since
	/// been cancelled, so every page re-establishes the [`Fence`] it carries in
	/// the transaction that deletes and stops when it no longer holds: the
	/// retention that chose the range, and for a range ending at a watermark,
	/// the timeline entry it was cut at.
	///
	/// The fence is re-read against the deleting transaction's own snapshot, so
	/// it orders a page after a `DEFINE ... CHANGEFEED` it can see. It is not a
	/// conflict with one it cannot: the backends on this line validate write
	/// sets only, so a catalog read arms nothing. What bounds the exposure is
	/// that a page deletes at most one batch, so at most one batch can be
	/// deleted under a retention that has already changed.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub(crate) async fn changefeed_collect(
		&self,
		rng: Range<Vec<u8>>,
		fence: Fence,
	) -> Result<(), Error> {
		self.changefeed_collect_within(rng, fence, CHANGEFEED_GC_PASS_KEY_BUDGET).await
	}

	/// [`Self::changefeed_collect`] with the key budget of one collection given
	/// explicitly, so that a test can reach the cap without first writing
	/// [`CHANGEFEED_GC_PASS_KEY_BUDGET`] keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub(crate) async fn changefeed_collect_within(
		&self,
		rng: Range<Vec<u8>>,
		fence: Fence,
		mut budget: u64,
	) -> Result<(), Error> {
		let mut cursor: Option<Key> = None;
		loop {
			// A range still being written to has no end this could reach, so
			// what is left belongs to the next tick
			if budget == 0 {
				trace!(
					"Changefeed collection on {}:{} stopped: it spent what one pass may delete",
					fence.ns,
					fence.db
				);
				return Ok(());
			}
			let txn = self.transaction(Write, Optimistic).await?;
			let res = crate::cf::retention(&txn, &fence.ns, &fence.db).await;
			if catch!(txn, res) != fence.retention {
				txn.cancel().await?;
				trace!(
					"Changefeed collection on {}:{} stopped: its retention changed",
					fence.ns,
					fence.db
				);
				return Ok(());
			}
			// A database removed and defined again under the same name is a
			// different database, however much of the old one's shape it
			// repeats. Its catalog id is the one thing it cannot repeat: ids
			// come from a sequence that is never given one back, and the
			// sequence is keyed outside every prefix a removal orphans. The
			// timeline check below cannot stand in for this, because a
			// watermark names a whole second and a replacement that restarts
			// its versionstamps can write the same pair inside it.
			//
			// A database that carries no id predates them, and is left to the
			// timeline check alone.
			if let Some(id) = fence.id {
				let res = txn.get_db(&fence.ns, &fence.db).await;
				let held = match res {
					Ok(dbv) => dbv.id,
					Err(Error::DbNotFound {
						..
					}) => None,
					Err(e) => {
						txn.cancel().await?;
						return Err(e);
					}
				};
				if held != Some(id) {
					txn.cancel().await?;
					trace!(
						"Changefeed collection on {}:{} stopped: it is not the database planned for",
						fence.ns,
						fence.db
					);
					return Ok(());
				}
			}
			// A watermark belongs to the timeline that produced it, and a
			// database removed and defined again under the same name and
			// retention has none of that timeline left. The entry is read back
			// by the second it is keyed by, so this costs one get per page
			// rather than a search of the whole timestamp prefix on a backend
			// that cannot scan in reverse.
			if let Some(w) = &fence.watermark {
				let key = crate::key::database::ts::new(&fence.ns, &fence.db, w.ts);
				let res = txn.get(key, None).await;
				let held = catch!(txn, res)
					.map(|v| VersionStamp::from_slice(&v))
					.transpose()
					.map_err(Error::from);
				if catch!(txn, held).as_ref() != Some(&w.vs) {
					txn.cancel().await?;
					trace!(
						"Changefeed collection on {}:{} stopped: its timeline changed",
						fence.ns,
						fence.db
					);
					return Ok(());
				}
			}
			// Every version goes, not just the visible one. A timestamp key is
			// named by the second it records and a change key by the
			// versionstamp it was written at, so neither is ever written twice
			// and a tombstone would leave every one of them on disk for good on
			// a backend that retains versions. Reclaiming that is what this
			// collection is for.
			let limit = budget.min(CHANGEFEED_GC_BATCH_SIZE as u64) as u32;
			let res = txn.delp_bounded(rng.clone(), cursor.as_deref(), limit, true);
			let (last, count, drained) = catch!(txn, res.await);
			catch!(txn, txn.commit().await);
			budget = budget.saturating_sub(count);
			// A short page means the range is drained
			if drained {
				return Ok(());
			}
			// Resume strictly after the last key this page deleted
			cursor = last;
			yield_now!();
		}
	}
}
