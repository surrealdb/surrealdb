//! Timestamp oracle contracts: `timestamp()` monotonicity, the
//! `safe_timestamp()` watermark bound that the live-query router relies on,
//! and the `timestamp_impl()` conversion of a datetime into the stamps
//! `timestamp()` mints, which `SHOW CHANGES ... SINCE <datetime>` relies on.

use chrono::{DateTime, TimeDelta, Utc};
use surrealdb_kvs::TransactionType::*;
use surrealdb_kvs::timestamp::{IncTimeStamp, MAX_TIMESTAMP_BYTES};
use web_time::{SystemTime, UNIX_EPOCH};

use crate::{TestBackend, kvs_test};

/// `timestamp()` is strictly increasing, both within a transaction and
/// across transactions.
async fn monotonic(b: &TestBackend) {
	let ds = b.create_ds().await;
	let mut last = None;
	for _ in 0..3 {
		let tx = ds.transaction(Write).await.unwrap();
		for _ in 0..5 {
			let ts = tx.timestamp().await.unwrap().as_versionstamp();
			if let Some(last) = last {
				assert!(ts > last, "timestamps must be strictly increasing ({ts} <= {last})");
			}
			last = Some(ts);
		}
		tx.cancel().await.unwrap();
	}
}

kvs_test!(monotonic);

/// `safe_timestamp()` is a closed watermark: at or below the current
/// timestamp, and non-decreasing across calls.
async fn safe_timestamp_bounded(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	let mut last_safe = None;
	for _ in 0..5 {
		let safe = tx.safe_timestamp().await.unwrap().as_versionstamp();
		let current = tx.timestamp().await.unwrap().as_versionstamp();
		assert!(safe <= current, "the safe watermark must not exceed a freshly minted timestamp");
		if let Some(last_safe) = last_safe {
			assert!(safe >= last_safe, "the safe watermark must not move backwards");
		}
		last_safe = Some(safe);
	}
	tx.cancel().await.unwrap();
}

kvs_test!(safe_timestamp_bounded);

/// How far the timestamp oracle's clock may stand from this process's. An
/// oracle need not read this process's clock — TiKV's reads the placement
/// driver's — while a conversion in the wrong unit misplaces an instant by
/// years.
const CLOCK_SKEW: TimeDelta = TimeDelta::seconds(5);

/// This process's clock, read as `HlcTimeStamp` reads it.
fn now() -> DateTime<Utc> {
	let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
	DateTime::from_timestamp(since_epoch.as_secs() as i64, since_epoch.subsec_nanos()).unwrap()
}

/// `timestamp_impl()` converts a datetime into the stamps `timestamp()`
/// mints: an instant before a stamp was minted converts to a stamp that sorts
/// at or below it, and an instant after converts to one that sorts above it.
/// `SHOW CHANGES ... SINCE <datetime>` starts its scan at that conversion, so
/// one made in another unit starts the scan years away from the change sets.
///
/// `version_impl()` is not held to this: it converts into a backend's read
/// versions, which need not be the stamps `timestamp()` mints.
async fn datetime_brackets_timestamp(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	let before = now() - CLOCK_SKEW;
	let ts = tx.timestamp().await.unwrap();
	let after = now() + CLOCK_SKEW;
	let ts_impl = tx.timestamp_impl();
	let lo = ts_impl.create_from_datetime(before);
	let hi = ts_impl.create_from_datetime(after);
	tx.cancel().await.unwrap();
	// `test-inc-timestamp` swaps in a count of calls, which no instant converts
	// to. A build that compiles core's tests turns it on for every crate in it.
	let Err(ts) = ts.downcast::<IncTimeStamp>() else {
		return;
	};
	let lo = lo.expect("an instant before the stamp converts to a stamp");
	let hi = hi.expect("an instant after the stamp converts to a stamp");
	let mut bufs = [[0u8; MAX_TIMESTAMP_BYTES]; 3];
	let [lo_buf, ts_buf, hi_buf] = &mut bufs;
	// Compared as encoded, the order the change feed's keys are scanned in.
	let (lo_key, ts_key, hi_key) = (lo.encode(lo_buf), ts.encode(ts_buf), hi.encode(hi_buf));
	assert!(
		lo_key <= ts_key,
		"an instant before the stamp {} converts to {}, above it",
		ts.as_versionstamp(),
		lo.as_versionstamp(),
	);
	assert!(
		ts_key < hi_key,
		"an instant after the stamp {} converts to {}, at or below it",
		ts.as_versionstamp(),
		hi.as_versionstamp(),
	);
}

kvs_test!(datetime_brackets_timestamp);
