use std::borrow::Cow;
use std::time::Duration;

use anyhow::Result;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVRange, lqe};
use crate::kvs::tasklease::LeaseHandler;
use crate::kvs::{BoxTimeStamp, BoxTimeStampImpl, Transaction};

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
#[instrument(level = "trace", target = "surrealdb::core::lq", skip_all)]
pub async fn gc_all_at(lh: &LeaseHandler, tx: &Transaction, retention: Duration) -> Result<()> {
	// A zero retention would delete everything up to "now"; treat it as disabled.
	if retention.is_zero() {
		return Ok(());
	}
	let ts_impl = tx.timestamp_impl();
	// Fetch all namespaces
	let nss = tx.all_ns(None).await?;
	for ns in nss.as_ref() {
		// Fetch all databases
		let dbs = tx.all_db(ns.namespace_id, None).await?;
		for db in dbs.as_ref() {
			let ts = tx.timestamp().await?;
			// Watermark cutoff = now - retention
			let watermark_ts = ts.sub_checked(retention).unwrap_or_else(|| ts_impl.earliest());
			gc_range(tx, db.namespace_id, db.database_id, &watermark_ts, &ts_impl).await?;
			lh.try_maintain_lease().await?;
			yield_now!();
		}
		lh.try_maintain_lease().await?;
		yield_now!();
	}
	Ok(())
}

/// Delete live-query events for a database older than the given watermark.
async fn gc_range(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ts: &BoxTimeStamp,
	ts_impl: &BoxTimeStampImpl,
) -> Result<()> {
	let mut buf = [0u8; _];
	let beg_ts = ts_impl.earliest().encode(&mut buf);
	let mut buf = [0u8; _];
	let end_ts = ts.encode(&mut buf);
	let beg = lqe::LqeTsRange {
		prefix: DatabaseRoot {
			ns,
			db,
		},
		ts: Cow::Borrowed(beg_ts),
	}
	.encode_bound()?;
	let end = lqe::LqeTsRange {
		prefix: DatabaseRoot {
			ns,
			db,
		},
		ts: Cow::Borrowed(end_ts),
	}
	.encode_bound()?;

	tx.delr((beg..end).into()).await?;
	Ok(())
}
