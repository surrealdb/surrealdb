use anyhow::Result;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::change;
use crate::key::debug::Sprintable;
use crate::kvs::tasklease::LeaseHandler;
use crate::kvs::{KVKey, Transaction};
use crate::vs::VersionStamp;

// gc_all_at deletes all change feed entries that become stale at the given
// timestamp.
#[instrument(level = "trace", target = "surrealdb::core::cfs", skip(lh, tx))]
pub async fn gc_all_at(lh: Option<&LeaseHandler>, tx: &Transaction, ts: u64) -> Result<()> {
	// Fetch all namespaces
	let nss = tx.all_ns().await?;
	// Loop over each namespace
	for ns in nss.as_ref() {
		// Trace for debugging
		trace!("Performing garbage collection on {} for timestamp {ts}", ns.name);
		// Process the namespace
		gc_ns(lh, tx, ts, ns.namespace_id).await?;
		// Possibly renew the lease
		if let Some(lh) = lh {
			lh.try_maintain_lease().await?;
		}
		// Pause execution
		yield_now!();
	}
	Ok(())
}

// gc_ns deletes all change feed entries in the given namespace that are older
// than the given watermark.
#[instrument(level = "trace", target = "surrealdb::core::cfs", skip(tx, lh))]
pub async fn gc_ns(
	lh: Option<&LeaseHandler>,
	tx: &Transaction,
	ts: u64,
	ns: NamespaceId,
) -> Result<()> {
	// Fetch all databases
	let dbs = tx.all_db(ns).await?;
	// Loop over each database
	for db in dbs.as_ref() {
		// Trace for debugging
		trace!("Performing garbage collection on {ns}:{} for timestamp {ts}", db.name);
		// Fetch all tables
		let tbs = tx.all_tb(db.namespace_id, db.database_id, None).await?;
		// Get the database changefeed expiration
		let db_cf_expiry = db.changefeed.map(|v| v.expiry.as_secs()).unwrap_or_default();
		// Get the maximum table changefeed expiration
		let tb_cf_expiry = tbs.as_ref().iter().fold(0, |acc, tb| match &tb.changefeed {
			None => acc,
			Some(cf) => {
				if cf.expiry.is_zero() {
					acc
				} else {
					acc.max(cf.expiry.as_secs())
				}
			}
		});
		// Calculate the maximum changefeed expiration
		let cf_expiry = db_cf_expiry.max(tb_cf_expiry);
		// Ignore this database if the expiry is greater
		if ts < cf_expiry {
			continue;
		}
		// Calculate the watermark expiry window
		let watermark_ts = ts - cf_expiry;
		// Calculate the watermark versionstamp
		let watermark_vs = tx
			.lock()
			.await
			.get_versionstamp_from_timestamp(watermark_ts, db.namespace_id, db.database_id)
			.await?;
		// If a versionstamp exists, then garbage collect
		if let Some(watermark_vs) = watermark_vs {
			gc_range(tx, db.namespace_id, db.database_id, watermark_vs).await?;
		}
		// Possibly renew the lease
		if let Some(lh) = lh {
			lh.try_maintain_lease().await?;
		}
		// Yield execution
		yield_now!();
	}
	Ok(())
}

// gc_db deletes all change feed entries in the given database that are older
// than the given watermark.
#[instrument(level = "trace", target = "surrealdb::core::cfs", skip(tx))]
pub async fn gc_range(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	vt: VersionStamp,
) -> Result<()> {
	// Calculate the range
	let beg = change::prefix_ts(ns, db, VersionStamp::ZERO).encode_key()?;
	let end = change::prefix_ts(ns, db, vt).encode_key()?;
	// Trace for debugging
	trace!(
		"Performing garbage collection on {ns}:{db} for watermark {vt:?}, between {} and {}",
		beg.sprint(),
		end.sprint()
	);
	// Delete the entire range in grouped batches
	tx.delr(beg..end).await?;
	// Ok all good
	Ok(())
}
