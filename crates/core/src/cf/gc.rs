use crate::err::Error;
use crate::key::change;
use crate::key::debug::Sprintable;
use crate::kvs::Transaction;
use crate::vs::VersionStamp;
use std::ops::Range;
use std::str;
use std::time::Duration;

/// The catalog state a planned range was chosen under.
///
/// A range is planned in a read transaction and drained in transactions of its
/// own, so what decided where the range ends can change in between. Every page
/// re-establishes this before it deletes and stops when it no longer holds.
///
/// Re-reading it orders a page after a catalog change it can see. It is not a
/// conflict with one it cannot: the backends on this line validate write sets
/// only, so a catalog read arms nothing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Fence {
	pub ns: String,
	pub db: String,
	/// The retention the range was chosen under.
	///
	/// A window that grew retains events the planned range still covers, and
	/// any other move would have produced a different range, so a page stops
	/// whichever way it went. The next tick replans against whatever the
	/// catalog says then.
	pub retention: Duration,
	/// The catalog id of the database the range was planned against.
	///
	/// This is what tells one generation of a reused name from the next. Ids
	/// are drawn from a sequence that is never given one back, and the sequence
	/// itself is keyed outside every prefix a removal orphans, so a database
	/// defined again under the same name draws an id no database has held. Its
	/// retention and its timeline can both be reproduced; this cannot.
	///
	/// Carrying `None` is common rather than exceptional: only an explicit
	/// `DEFINE DATABASE` draws an id. A database the catalog creates on demand,
	/// which is what a write through an undefined name does, is stored with the
	/// default definition and has none, as does one inside a namespace without
	/// an id. Such a database is told apart by its timeline alone, which a
	/// replacement can reproduce, so the identity check covers explicitly
	/// defined databases and not the ones a write brings into being.
	pub id: Option<u32>,
	/// The point on the database's timeline the range was cut at, for a range
	/// that ends at a watermark.
	///
	/// Names are reused on this line, so a database removed and defined again
	/// under the same name and retention answers the retention check as though
	/// it were the one planned. Its versionstamps start again from the bottom
	/// of the range about to be deleted, which would take the events of the
	/// replacement rather than the ones planned for. The timeline that produced
	/// the watermark is destroyed with the rest of the prefix, so a page
	/// re-reads it to tell the two apart.
	///
	/// A range chosen because the database retains nothing carries none: it
	/// covers the whole prefix, and a replacement that retains nothing has
	/// nothing to lose, while one that retains something fails the retention
	/// check above.
	pub watermark: Option<Watermark>,
}

/// The timeline entry a watermark range was cut at.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Watermark {
	/// The wall clock second the entry is keyed by.
	///
	/// This is the second the entry was written at, at or before the one the
	/// plan asked about, so that a fence can read the entry back by name
	/// instead of searching for it again.
	pub ts: u64,
	/// The versionstamp that entry holds.
	pub vs: VersionStamp,
}

/// A planned range and the catalog state it was chosen under.
pub type PlannedRange = (Range<Vec<u8>>, Fence);

/// The changefeed retention of a database: the longest window its own
/// definition or any of its tables asks for.
///
/// A database the catalog no longer holds retains nothing. Every other failure
/// is a failure to read the catalog rather than an answer about it, and is
/// propagated: treating one as zero retention would plan the deletion of a live
/// changefeed's whole history.
pub async fn retention(tx: &Transaction, ns: &str, db: &str) -> Result<Duration, Error> {
	let dbv = match tx.get_db(ns, db).await {
		Ok(dbv) => dbv,
		Err(Error::DbNotFound {
			..
		}) => return Ok(Duration::ZERO),
		Err(e) => return Err(e),
	};
	let db_cf_expiry = dbv.changefeed.map(|v| v.expiry).unwrap_or_default();
	let tbs = tx.all_tb(ns, db, None).await?;
	let tb_cf_expiry = tbs.as_ref().iter().fold(Duration::ZERO, |acc, tb| match &tb.changefeed {
		None => acc,
		Some(cf) => acc.max(cf.expiry),
	});
	Ok(db_cf_expiry.max(tb_cf_expiry))
}

/// The key ranges that hold changefeed data which is stale at `ts`.
///
/// This only reads the catalog. How much a database has accumulated is set by
/// its write traffic rather than by anything the catalog holds, so the ranges
/// are returned to be drained in transactions of their own rather than deleted
/// here: a backlog of any size must not decide the size of one transaction.
/// Each range carries the [`Fence`] a page must establish before it deletes.
#[allow(unused)]
#[instrument(level = "trace", target = "surrealdb::core::cfs", skip(tx))]
pub async fn gc_all_at(tx: &Transaction, ts: u64) -> Result<Vec<PlannedRange>, Error> {
	// The ranges collected so far
	let mut plan = Vec::new();
	// Fetch all namespaces
	let nss = tx.all_ns().await?;
	// Loop over each namespace
	for ns in nss.as_ref() {
		// Trace for debugging
		trace!("Planning garbage collection on {} for timestamp {ts}", ns.name);
		// Process the namespace
		gc_ns(tx, ts, &ns.name, &mut plan).await?;
		// Pause execution
		yield_now!();
	}
	Ok(plan)
}

/// Adds the stale ranges of one namespace's databases to `plan`.
#[allow(unused)]
#[instrument(level = "trace", target = "surrealdb::core::cfs", skip(tx, plan))]
pub async fn gc_ns(
	tx: &Transaction,
	ts: u64,
	ns: &str,
	plan: &mut Vec<PlannedRange>,
) -> Result<(), Error> {
	// Fetch all databases
	let dbs = tx.all_db(ns).await?;
	// Loop over each database
	for db in dbs.as_ref() {
		// Trace for debugging
		trace!("Planning garbage collection on {ns}:{} for timestamp {ts}", db.name);
		// Calculate the maximum changefeed expiration
		let cf_expiry = retention(tx, ns, &db.name).await?;
		// The retention every range below is chosen under, which its pages
		// re-read before they delete. The names are how a page finds the
		// database again, so they are the raw ones the catalog is keyed by
		// rather than the escaped form an `Ident` displays as.
		let fence = Fence {
			ns: ns.to_owned(),
			db: db.name.to_raw(),
			retention: cf_expiry,
			id: db.id,
			watermark: None,
		};
		// A database with no changefeed retention retains nothing, so its whole
		// change range and the timestamps correlating it with versionstamps go.
		// No timestamp is written for such a database, so that range is what a
		// database whose changefeed was dropped left behind and is drained once.
		// A changefeed defined with no retention at all still records a change
		// for every write, so the change range has no end the collection can
		// reach and it is bounded by what one pass may delete instead.
		if cf_expiry.is_zero() {
			plan_range(tx, change_range(ns, &db.name, VersionStamp::MAX)?, fence.clone(), plan)
				.await?;
			plan_range(tx, timestamp_range(ns, &db.name)?, fence, plan).await?;
			yield_now!();
			continue;
		}
		// Watermarks are unix seconds, so the retention window is applied whole
		let cf_expiry = cf_expiry.as_secs();
		// Ignore this database if the expiry is greater
		if ts < cf_expiry {
			continue;
		}
		// Calculate the watermark expiry window
		let watermark_ts = ts - cf_expiry;
		// Calculate the watermark versionstamp
		let watermark = tx.lock().await.get_timestamp_entry(watermark_ts, ns, &db.name).await?;
		// If a versionstamp exists, then garbage collect
		if let Some((found_ts, watermark_vs)) = watermark {
			let fence = Fence {
				// The second the entry is keyed by, which is at or before the
				// one asked about. A fence checking the entry is still there
				// needs the key it was found at to read it back by name.
				watermark: Some(Watermark {
					ts: found_ts,
					vs: watermark_vs,
				}),
				..fence
			};
			plan_range(tx, change_range(ns, &db.name, watermark_vs)?, fence, plan).await?;
		}
		// Yield execution
		yield_now!();
	}
	Ok(())
}

/// Adds a range to the plan, unless it is already empty.
///
/// A planned range is drained in write transactions of its own, each of which
/// re-reads the catalog to re-establish the retention it was chosen under before
/// it deletes anything. An empty range costs all of that to discover it holds
/// nothing, and most databases define no changefeed at all: every one of those
/// plans two ranges that are empty on every tick, on every node, for as long as
/// the deployment runs. Asking here, in the transaction the plan is read in,
/// turns each of them into one read.
async fn plan_range(
	tx: &Transaction,
	rng: Range<Vec<u8>>,
	fence: Fence,
	plan: &mut Vec<PlannedRange>,
) -> Result<(), Error> {
	// The pages which drain a range destroy the versions of what they delete,
	// so a range holding only tombstoned keys is not empty to them
	if tx.any_version_in(rng.clone()).await? {
		plan.push((rng, fence));
	}
	Ok(())
}

/// The range of change entries of one database below the watermark `vt`.
pub fn change_range(ns: &str, db: &str, vt: VersionStamp) -> Result<Range<Vec<u8>>, Error> {
	// Calculate the range
	let beg = change::prefix_ts(ns, db, VersionStamp::ZERO)?;
	let end = change::prefix_ts(ns, db, vt)?;
	// Trace for debugging
	trace!(
		"Collecting changes on {ns}:{db} for watermark {vt:?}, between {} and {}",
		beg.sprint(),
		end.sprint()
	);
	Ok(beg..end)
}

/// The range of timestamp keys correlating wall clock seconds with the
/// versionstamps of a database that no longer keeps a changefeed.
///
/// The keys only exist to turn a retention window into a watermark versionstamp,
/// and a datetime into a scan start for `SHOW CHANGES SINCE`. Neither is
/// meaningful once a database retains no changes, and nothing writes another one
/// until it does again.
pub fn timestamp_range(ns: &str, db: &str) -> Result<Range<Vec<u8>>, Error> {
	let beg = crate::key::database::ts::prefix(ns, db)?;
	let end = crate::key::database::ts::suffix(ns, db)?;
	Ok(beg..end)
}

/// Deletes the change entries of one database below the watermark `vt`, in the
/// caller's transaction.
///
/// The write set is the whole range, so this is for a caller that already knows
/// the range is small. The collector uses [`change_range`] and drains it in
/// committed pages instead.
#[instrument(level = "trace", target = "surrealdb::core::cfs", skip(tx))]
pub async fn gc_range(tx: &Transaction, ns: &str, db: &str, vt: VersionStamp) -> Result<(), Error> {
	tx.delr(change_range(ns, db, vt)?).await?;
	Ok(())
}
