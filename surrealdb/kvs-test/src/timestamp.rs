//! Timestamp oracle contracts: `timestamp()` monotonicity and the
//! `safe_timestamp()` watermark bound that the live-query router relies on.

use surrealdb_kvs::TransactionType::*;

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
