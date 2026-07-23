//! Versioned (MVCC / time-travel) reads, for backends built with versioning
//! enabled. Consumers with such a backend register it under a dedicated name
//! (e.g. `mem_versioned` for `memory?versioned=true`) so these tests target
//! it explicitly.

use surrealdb_kvs::TransactionType::*;
use surrealdb_kvs::{Direction, Error, KeyRange};

use crate::{TestBackend, TestDs, kvs_test};

/// A wall-clock nanosecond version timestamp (SurrealQL `VERSION`
/// semantics), fenced by sleeps against clock-granularity ties.
async fn version_now() -> u64 {
	tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	let version = web_time::SystemTime::now()
		.duration_since(web_time::SystemTime::UNIX_EPOCH)
		.unwrap()
		.as_nanos() as u64;
	tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	version
}

/// Backends without versioning support must reject every read that passes
/// `version: Some(_)` with [`Error::UnsupportedVersionedQueries`].
async fn unsupported_error(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"test".into(), b"value".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	let rng = KeyRange::from(&b"a"[..]..&b"z"[..]);
	assert!(matches!(
		tx.get(b"test".into(), Some(1)).await,
		Err(Error::UnsupportedVersionedQueries)
	));
	assert!(matches!(
		tx.exists(b"test".into(), Some(1)).await,
		Err(Error::UnsupportedVersionedQueries)
	));
	assert!(matches!(
		tx.keys(rng.as_borrowed(), 10, 0, Some(1)).await,
		Err(Error::UnsupportedVersionedQueries)
	));
	assert!(matches!(
		tx.scan(rng.as_borrowed(), 10, 0, Some(1)).await,
		Err(Error::UnsupportedVersionedQueries)
	));
	assert!(matches!(tx.count(rng, Some(1)).await, Err(Error::UnsupportedVersionedQueries)));
	tx.cancel().await.unwrap();
}

kvs_test!(unsupported_error, except = [mem_versioned]);

/// Seed two generations of data around a captured version timestamp.
/// Returns the version between them.
async fn seed_two_generations(ds: &TestDs) -> u64 {
	let tx = ds.transaction(Write).await.unwrap();

	tx.set(b"a".into(), b"old-a".to_vec()).await.unwrap();
	tx.set(b"b".into(), b"old-b".to_vec()).await.unwrap();

	tx.commit().await.unwrap();

	let version = version_now().await;

	let tx = ds.transaction(Write).await.unwrap();

	tx.set(b"a".into(), b"new-a".to_vec()).await.unwrap();
	tx.del(b"b".into()).await.unwrap();
	tx.set(b"c".into(), b"new-c".to_vec()).await.unwrap();

	tx.commit().await.unwrap();
	version
}

/// A `get` at a historical version returns the value visible then.
async fn get_at_version(b: &TestBackend) {
	let ds = b.create_ds().await;
	let version = seed_two_generations(&ds).await;

	let tx = ds.transaction(Read).await.unwrap();

	// Historical view
	assert_eq!(tx.get(b"a".into(), Some(version)).await.unwrap().as_deref(), Some(&b"old-a"[..]));
	assert_eq!(tx.get(b"b".into(), Some(version)).await.unwrap().as_deref(), Some(&b"old-b"[..]));
	assert!(tx.get(b"c".into(), Some(version)).await.unwrap().is_none());

	// Current view
	assert_eq!(tx.get(b"a".into(), None).await.unwrap().as_deref(), Some(&b"new-a"[..]));
	assert!(tx.get(b"b".into(), None).await.unwrap().is_none());

	tx.cancel().await.unwrap();
}

kvs_test!(get_at_version, only = [mem_versioned]);

/// `exists` at a historical version reflects the state visible then.
async fn exists_at_version(b: &TestBackend) {
	let ds = b.create_ds().await;

	let version = seed_two_generations(&ds).await;

	let tx = ds.transaction(Read).await.unwrap();

	assert!(tx.exists(b"b".into(), Some(version)).await.unwrap());
	assert!(!tx.exists(b"c".into(), Some(version)).await.unwrap());
	assert!(!tx.exists(b"b".into(), None).await.unwrap());
	assert!(tx.exists(b"c".into(), None).await.unwrap());

	tx.cancel().await.unwrap();
}

kvs_test!(exists_at_version, only = [mem_versioned]);

/// Range reads (`keys`, `scan`, `count`) at a historical version see the
/// old world.
async fn scan_at_version(b: &TestBackend) {
	let ds = b.create_ds().await;

	let version = seed_two_generations(&ds).await;

	let tx = ds.transaction(Read).await.unwrap();

	let rng = KeyRange::from(&b"a"[..]..&b"z"[..]);
	let keys = tx.keys(rng.as_borrowed(), u32::MAX, 0, Some(version)).await.unwrap();
	assert_eq!(keys.keys, vec![b"a".to_vec(), b"b".to_vec()]);

	let scan = tx.scan(rng.as_borrowed(), u32::MAX, 0, Some(version)).await.unwrap();
	assert_eq!(
		scan.values,
		vec![(b"a".to_vec(), b"old-a".to_vec()), (b"b".to_vec(), b"old-b".to_vec())]
	);

	assert_eq!(tx.count(rng.as_borrowed(), Some(version)).await.unwrap(), 2);

	// Current view
	let keys = tx.keys(rng.as_borrowed(), u32::MAX, 0, None).await.unwrap();
	assert_eq!(keys.keys, vec![b"a".to_vec(), b"c".to_vec()]);
	assert_eq!(tx.count(rng, None).await.unwrap(), 2);

	tx.cancel().await.unwrap();
}

kvs_test!(scan_at_version, only = [mem_versioned]);

/// Versioned scans through the cursor API: a cursor opened at a historical
/// version must see exactly the rows visible at that timestamp — identically
/// via `next_batch` and `for_each`, in both directions. Covers the versioned
/// forward/backward iterator arms (e.g. `MemValsCursor::build_iter`), which no
/// other cursor test reaches.
async fn cursor_versioned_for_each_matches_next_batch(b: &TestBackend) {
	let ds = b.create_ds().await;

	// Batch A: the historical view.
	let tx = ds.transaction(Write).await.unwrap();
	let historical: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"v0".to_vec()),
		(b"a\x00".to_vec(), b"v1".to_vec()),
		(b"b".to_vec(), b"v2".to_vec()),
	];
	for (k, v) in &historical {
		tx.set(k.into(), v.clone()).await.unwrap();
	}
	tx.commit().await.unwrap();

	// Capture a version timestamp strictly between the two commits. Versions
	// are wall-clock nanoseconds (SurrealQL `VERSION` semantics); the sleeps
	// guard against clock granularity ties on either side.
	tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	let version = web_time::SystemTime::now()
		.duration_since(web_time::SystemTime::UNIX_EPOCH)
		.unwrap()
		.as_nanos() as u64;
	tokio::time::sleep(std::time::Duration::from_millis(20)).await;

	// Batch B: overwrite one row and add another — the current view.
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"a".into(), b"v0-new".to_vec()).await.unwrap();
	tx.set(b"c".into(), b"v3".to_vec()).await.unwrap();
	tx.commit().await.unwrap();

	let rng = KeyRange::from(b"a"..b"d");
	for dir in [Direction::Forward, Direction::Backward] {
		let tx = ds.transaction(Read).await.unwrap();

		// Historical view via next_batch.
		let mut c1 = tx.open_vals_cursor(rng.as_borrowed(), dir, 0, Some(version)).await.unwrap();
		let mut via_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
		loop {
			let batch = c1.next_batch(2).await.unwrap();
			if batch.is_empty() {
				break;
			}
			for (k, v) in &batch {
				via_batch.push((k.to_vec(), v.to_vec()));
			}
		}
		drop(c1);

		// Historical view via for_each.
		let mut c2 = tx.open_vals_cursor(rng.as_borrowed(), dir, 0, Some(version)).await.unwrap();
		let mut via_visit: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
		loop {
			let s = c2
				.for_each(2, &mut |k, v| {
					via_visit.push((k.to_vec(), v.to_vec()));
					Ok(std::ops::ControlFlow::Continue(()))
				})
				.await
				.unwrap();
			if s.rows == 0 {
				break;
			}
		}
		drop(c2);
		tx.cancel().await.unwrap();

		let mut expected = historical.clone();
		if matches!(dir, Direction::Backward) {
			expected.reverse();
		}
		assert_eq!(via_batch, expected, "versioned next_batch view mismatch ({dir:?})");
		assert_eq!(via_visit, via_batch, "versioned for_each diverged from next_batch ({dir:?})");
	}

	// Sanity: the current view (no version) must reflect batch B.
	let tx = ds.transaction(Read).await.unwrap();
	let mut c = tx.open_vals_cursor(rng, Direction::Forward, 0, None).await.unwrap();
	let mut current: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
	loop {
		let s = c
			.for_each(10, &mut |k, v| {
				current.push((k.to_vec(), v.to_vec()));
				Ok(std::ops::ControlFlow::Continue(()))
			})
			.await
			.unwrap();
		if s.rows == 0 {
			break;
		}
	}
	drop(c);
	tx.cancel().await.unwrap();
	let expected_current: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"v0-new".to_vec()),
		(b"a\x00".to_vec(), b"v1".to_vec()),
		(b"b".to_vec(), b"v2".to_vec()),
		(b"c".to_vec(), b"v3".to_vec()),
	];
	assert_eq!(current, expected_current, "current view should reflect the second commit");
}

kvs_test!(cursor_versioned_for_each_matches_next_batch, only = [mem_versioned]);
