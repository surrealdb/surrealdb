//! Versioned (MVCC / time-travel) read handling.
//!
//! A backend registered without versioning must reject every read carrying a version with
//! [`Error::UnsupportedVersionedQueries`]. SurrealKV opened with `?versioned=true` is registered
//! as `surrealkv_versioned`, where a read as of a version is held to what it must return.

use surrealdb_kvs::TransactionType::*;
use surrealdb_kvs::{Error, KeyRange};

use crate::{TestBackend, TestDs, kvs_test};

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
	assert!(matches!(
		tx.count_bounded(rng.as_borrowed(), Some(1), 10).await,
		Err(Error::UnsupportedVersionedQueries)
	));
	assert!(matches!(tx.count(rng, Some(1)).await, Err(Error::UnsupportedVersionedQueries)));
	tx.cancel().await.unwrap();
}

kvs_test!(unsupported_error, except = [surrealkv_versioned]);

/// The key every commit of [`commit`] writes, outside the range the reads of
/// [`range_reads_pass_over_deleted_keys`] are made over.
const MARKER: &[u8] = b"zz";

/// Commit `puts` and `dels` in one transaction and return the version of the commit: the smallest
/// version at which a read sees it. The commit also writes a marker no other commit writes, and
/// the version is where a point read of the marker first returns it, found by bisection.
async fn commit(ds: &TestDs, n: u32, puts: &[(&str, &str)], dels: &[&str]) -> u64 {
	let marker = format!("commit {n}").into_bytes();
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(MARKER.into(), marker.clone()).await.unwrap();
	for (key, val) in puts {
		tx.set(key.as_bytes().into(), val.as_bytes().to_vec()).await.unwrap();
	}
	for key in dels {
		tx.del(key.as_bytes().into()).await.unwrap();
	}
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	// Invariant: the marker is not visible at `below` and is visible at `above`
	let (mut below, mut above) = (0, u64::MAX);
	assert_eq!(tx.get(MARKER.into(), Some(above)).await.unwrap(), Some(marker.clone()));
	assert_eq!(tx.get(MARKER.into(), Some(below)).await.unwrap(), None);
	while above - below > 1 {
		let mid = below + (above - below) / 2;
		if tx.get(MARKER.into(), Some(mid)).await.unwrap() == Some(marker.clone()) {
			above = mid;
		} else {
			below = mid;
		}
	}
	tx.cancel().await.unwrap();
	above
}

/// Every read of the keys `a` to `z` as of `version` returns `live`, the keys which hold a value
/// as of it and their values, in key order: `keys`, `keysr`, `scan`, `scanr` and `count`. A point
/// read, `get` and `exists`, agrees for every key of `probed`, the deleted ones included.
async fn assert_live(ds: &TestDs, version: Option<u64>, live: &[(&str, &str)], probed: &[&str]) {
	let live: Vec<(Vec<u8>, Vec<u8>)> =
		live.iter().map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec())).collect();
	let reversed: Vec<(Vec<u8>, Vec<u8>)> = live.iter().rev().cloned().collect();
	let tx = ds.transaction(Read).await.unwrap();
	let rng = KeyRange::from(&b"a"[..]..&b"z"[..]);
	let name = format!("as of {version:?}");
	assert_eq!(tx.count(rng.as_borrowed(), version).await.unwrap(), live.len(), "count {name}");
	// A deleted key takes up neither `skip` nor `limit`, whichever way the range is read
	for skip in 0..=live.len() as u32 + 1 {
		for limit in 0..=live.len() as u32 + 1 {
			let window = |entries: &[(Vec<u8>, Vec<u8>)]| -> Vec<(Vec<u8>, Vec<u8>)> {
				entries.iter().skip(skip as usize).take(limit as usize).cloned().collect()
			};
			let keys = |entries: Vec<(Vec<u8>, Vec<u8>)>| -> Vec<Vec<u8>> {
				entries.into_iter().map(|(key, _)| key).collect()
			};
			let at = format!("{name}, skip {skip}, limit {limit}");
			let res = tx.scan(rng.as_borrowed(), limit, skip, version).await.unwrap();
			assert_eq!(res.values, window(&live), "scan {at}");
			let res = tx.scanr(rng.as_borrowed(), limit, skip, version).await.unwrap();
			assert_eq!(res.values, window(&reversed), "scanr {at}");
			let res = tx.keys(rng.as_borrowed(), limit, skip, version).await.unwrap();
			assert_eq!(res.keys, keys(window(&live)), "keys {at}");
			let res = tx.keysr(rng.as_borrowed(), limit, skip, version).await.unwrap();
			assert_eq!(res.keys, keys(window(&reversed)), "keysr {at}");
		}
	}
	for key in probed {
		let expected = live.iter().find(|(k, _)| k == key.as_bytes()).map(|(_, v)| v.clone());
		let got = tx.get(key.as_bytes().into(), version).await.unwrap();
		assert_eq!(got, expected, "get {key} {name}");
		let got = tx.exists(key.as_bytes().into(), version).await.unwrap();
		assert_eq!(got, expected.is_some(), "exists {key} {name}");
	}
	tx.cancel().await.unwrap();
}

/// A key and a value.
type Entry = (&'static str, &'static str);

/// A commit of [`range_reads_pass_over_deleted_keys`], and the keys which hold a value, in key
/// order, as of it.
struct Step {
	puts: &'static [Entry],
	dels: &'static [&'static str],
	live: &'static [Entry],
}

/// A key deleted as of a version is not part of what a range read as of it returns, nor is a key
/// which was only ever deleted, and an older version of a deleted key, which the read passes over,
/// is not either. A key written again after it was deleted is, from that version on. Each commit
/// is read as of its own version, which includes it, and as of the version before, which does not.
async fn range_reads_pass_over_deleted_keys(b: &TestBackend) {
	let ds = b.create_ds().await;
	let probed = ["a", "b", "c", "d", "e", "x", "y"];
	// `y` is only ever deleted, and `x`, the last key of the range, is written and then deleted
	let steps = [
		Step {
			puts: &[],
			dels: &[],
			live: &[],
		},
		Step {
			puts: &[("a", "a1"), ("b", "b1"), ("c", "c1"), ("d", "d1"), ("x", "x1")],
			dels: &[],
			live: &[("a", "a1"), ("b", "b1"), ("c", "c1"), ("d", "d1"), ("x", "x1")],
		},
		Step {
			puts: &[("e", "e2")],
			dels: &["b", "d", "x", "y"],
			live: &[("a", "a1"), ("c", "c1"), ("e", "e2")],
		},
		Step {
			puts: &[("b", "b3")],
			dels: &["a"],
			live: &[("b", "b3"), ("c", "c1"), ("e", "e2")],
		},
		Step {
			puts: &[("d", "d4")],
			dels: &["e"],
			live: &[("b", "b3"), ("c", "c1"), ("d", "d4")],
		},
		Step {
			puts: &[],
			dels: &["d"],
			live: &[("b", "b3"), ("c", "c1")],
		},
	];
	// The first commit is read as of the version before it, which sees nothing
	let mut previous: &[Entry] = &[];
	for (n, step) in steps.iter().enumerate() {
		let version = commit(&ds, n as u32, step.puts, step.dels).await;
		assert_live(&ds, Some(version), step.live, &probed).await;
		assert_live(&ds, Some(version - 1), previous, &probed).await;
		previous = step.live;
	}
	assert_live(&ds, Some(u64::MAX), previous, &probed).await;
	assert_live(&ds, None, previous, &probed).await;
}

kvs_test!(range_reads_pass_over_deleted_keys, only = [surrealkv_versioned]);
