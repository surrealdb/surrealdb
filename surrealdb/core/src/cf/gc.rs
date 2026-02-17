use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::change;
use crate::key::debug::Sprintable;
use crate::kvs::tasklease::LeaseHandler;
use crate::kvs::{BoxTimeStamp, BoxTimeStampImpl, KVKey, Transaction};

// gc_all_at deletes all change feed entries that become stale at the given
// current time.
#[instrument(level = "trace", target = "surrealdb::core::cfs", skip_all)]
pub async fn gc_all_at(lh: &LeaseHandler, tx: &Transaction) -> Result<()> {
	// Fetch all namespaces
	let nss = tx.all_ns().await?;

	let ts_impl = tx.timestamp_impl();
	// Loop over each namespace
	for ns in nss.as_ref() {
		// Trace for debugging
		trace!("Performing garbage collection on {}", ns.name);
		// Fetch all databases
		let dbs = tx.all_db(ns.namespace_id).await?;
		// Loop over each database
		for db in dbs.as_ref() {
			// Trace for debugging
			trace!("Performing garbage collection on {}:{}", ns.name, db.name);
			// Fetch all tables
			let tbs = tx.all_tb(db.namespace_id, db.database_id, None).await?;
			// Get the database changefeed expiration
			let db_cf_expiry = db.changefeed.map(|v| v.expiry).unwrap_or_default();
			// Get the maximum table changefeed expiration
			let tb_cf_expiry = tbs
				.as_ref()
				.iter()
				.filter_map(|tb| tb.changefeed.as_ref())
				.map(|cf| cf.expiry)
				.filter(|&dur| !dur.is_zero())
				.max()
				.unwrap_or(Duration::ZERO);
			// Calculate the maximum changefeed expiration
			let cf_expiry = db_cf_expiry.max(tb_cf_expiry);
			// Skip if no retention policy configured
			if cf_expiry.is_zero() {
				continue;
			}

			let ts = tx.timestamp().await?;
			// Calculate the changefeed watermark cutoff time
			let watermark_ts = ts.sub_checked(cf_expiry).unwrap_or_else(|| ts_impl.earliest());
			// Garbage collect all entries older than the watermark
			gc_range(tx, db.namespace_id, db.database_id, &watermark_ts, &ts_impl).await?;
			// Possibly renew the lease
			lh.try_maintain_lease().await?;
			// Yield execution
			yield_now!();
		}
		// Possibly renew the lease
		lh.try_maintain_lease().await?;
		// Pause execution
		yield_now!();
	}
	Ok(())
}

// gc_range deletes all change feed entries in the given database that are older
// than the given watermark time.
// The time is converted to bytes using the storage engine's specific encoding.
#[instrument(level = "trace", target = "surrealdb::core::cfs", skip_all, fields(ns = %ns, db = %db))]
pub async fn gc_range(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ts: &BoxTimeStamp,
	ts_impl: &BoxTimeStampImpl,
) -> Result<()> {
	// Fetch the earliest timestamp from the storage engine
	let mut buf = [0u8; _];
	let beg_ts = ts_impl.earliest().encode(&mut buf);
	// Fetch the watermark timestamp from the storage engine
	let mut buf = [0u8; _];
	let end_ts = ts.encode(&mut buf);
	// Create the changefeed range key prefix
	let beg = change::prefix_ts(ns, db, beg_ts).encode_key()?;
	let end = change::prefix_ts(ns, db, end_ts).encode_key()?;
	// Trace for debugging
	trace!(
		"Performing garbage collection on {ns}:{db} for watermark time {}, between {} and {}",
		ts.as_datetime().unwrap_or(DateTime::<Utc>::MIN_UTC),
		beg.sprint(),
		end.sprint()
	);
	// Delete the entire range in grouped batches
	tx.delr(beg..end).await?;
	// Ok all good
	Ok(())
}
