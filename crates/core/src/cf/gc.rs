use anyhow::Result;
use chrono::{DateTime, Duration, Utc};

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::change;
use crate::key::debug::Sprintable;
use crate::kvs::tasklease::LeaseHandler;
use crate::kvs::{KVKey, Transaction};

// gc_all_at deletes all change feed entries that become stale at the given
// current time.
#[instrument(level = "trace", target = "surrealdb::core::cfs", skip_all)]
pub async fn gc_all_at(lh: &LeaseHandler, tx: &Transaction) -> Result<()> {
	// Fetch all namespaces
	let nss = tx.all_ns().await?;
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
			let db_cf_expiry = db.changefeed.map(|v| v.expiry.as_secs()).unwrap_or_default();
			// Get the maximum table changefeed expiration
			let tb_cf_expiry = tbs
				.as_ref()
				.iter()
				.filter_map(|tb| tb.changefeed.as_ref())
				.map(|cf| cf.expiry.as_secs())
				.filter(|&secs| secs > 0)
				.max()
				.unwrap_or(0);
			// Calculate the maximum changefeed expiration (in seconds)
			let cf_expiry_secs = db_cf_expiry.max(tb_cf_expiry);
			// Skip if no retention policy configured
			if cf_expiry_secs == 0 {
				continue;
			}
			// Get current datetime from storage engine
			let current_time = tx.timestamp().await?.to_datetime();
			// Age the datetime by the maximum changefeed expiration
			let changefeed_age = Duration::seconds(cf_expiry_secs as i64);
			// Calculate the changefeed watermark cutoff time
			let watermark_time = current_time - changefeed_age;
			// Garbage collect all entries older than the watermark
			gc_range(tx, db.namespace_id, db.database_id, watermark_time).await?;
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
#[instrument(level = "trace", target = "surrealdb::core::cfs", skip_all, fields(ns = %ns, db = %db, dt = %dt))]
pub async fn gc_range(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	dt: DateTime<Utc>,
) -> Result<()> {
	// Fetch the earliest timestamp from the storage engine
	let beg_ts = tx.timestamp_bytes_from_versionstamp(0).await?;
	// Fetch the watermark timestamp from the storage engine
	let end_ts = tx.timestamp_bytes_from_datetime(dt).await?;
	// Create the changefeed range key prefix
	let beg = change::prefix_ts(ns, db, &beg_ts).encode_key()?;
	let end = change::prefix_ts(ns, db, &end_ts).encode_key()?;
	// Trace for debugging
	trace!(
		"Performing garbage collection on {ns}:{db} for watermark time {dt}, between {} and {}",
		beg.sprint(),
		end.sprint()
	);
	// Delete the entire range in grouped batches
	tx.delr(beg..end).await?;
	// Ok all good
	Ok(())
}
