//! Contract tests for the `Transactable` methods that have shared default
//! implementations (`getm`, `getr`, `delr`, `clrr`, `count`, `count_bounded`,
//! `replace`, `clr`, `clrc`). Backends may override any of these, so the contract is
//! asserted against every backend regardless.

use surrealdb_kvs::KeyRange;
use surrealdb_kvs::TransactionType::*;

use crate::{TestBackend, TestDs, kvs_test};

/// Seed the datastore with `k1`..`k5`, values matching the keys.
async fn seed(ds: &TestDs) {
	let tx = ds.transaction(Write).await.unwrap();
	for i in 1..=5u8 {
		let key = format!("k{i}");
		tx.set(key.as_bytes().into(), key.clone().into_bytes()).await.unwrap();
	}
	tx.commit().await.unwrap();
}

/// `getm` preserves input order, reports misses as `None`, and accounts
/// found records and value bytes.
async fn getm(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	let keys = [b"k3".into(), b"missing".into(), b"k1".into()];
	let res = tx.getm(&keys, None).await.unwrap();
	assert_eq!(res.values.len(), 3);
	assert_eq!(res.values[0].as_deref(), Some(&b"k3"[..]), "results must preserve input order");
	assert_eq!(res.values[1], None, "missing keys must yield None");
	assert_eq!(res.values[2].as_deref(), Some(&b"k1"[..]));
	assert_eq!(res.records, 2, "records counts only the hits");
	assert_eq!(res.value_bytes, 4, "value_bytes sums the hit values");
	tx.cancel().await.unwrap();
}

kvs_test!(getm);

/// `getm` of nothing answers nothing, with no hit counted.
async fn getm_of_no_keys(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	let res = tx.getm(&[], None).await.unwrap();
	assert!(res.values.is_empty());
	assert_eq!((res.records, res.value_bytes), (0, 0));
	tx.cancel().await.unwrap();
}

kvs_test!(getm_of_no_keys);

/// `getm` in a writable transaction answers its own buffered writes and deletes, as `get` does.
async fn getm_sees_buffered_writes(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"k2".into(), b"changed".to_vec()).await.unwrap();
	tx.del(b"k4".into()).await.unwrap();
	tx.set(b"k9".into(), b"new".to_vec()).await.unwrap();
	let keys = [b"k9".into(), b"k4".into(), b"k2".into(), b"k1".into()];
	let res = tx.getm(&keys, None).await.unwrap();
	assert_eq!(res.values[0].as_deref(), Some(&b"new"[..]));
	assert_eq!(res.values[1], None, "a deleted key reads as a miss");
	assert_eq!(res.values[2].as_deref(), Some(&b"changed"[..]));
	assert_eq!(res.values[3].as_deref(), Some(&b"k1"[..]));
	assert_eq!(res.records, 3);
	assert_eq!(res.value_bytes, 3 + 7 + 2);
	tx.cancel().await.unwrap();
}

kvs_test!(getm_sees_buffered_writes);

/// `getm` answers each key as `get` does, and a version on a store that keeps none is refused.
async fn getm_refuses_a_version_without_versioning(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	let keys = [b"k1".into(), b"k2".into()];
	assert!(matches!(
		tx.getm(&keys, Some(1)).await,
		Err(surrealdb_kvs::Error::UnsupportedVersionedQueries)
	));
	tx.cancel().await.unwrap();
}

kvs_test!(getm_refuses_a_version_without_versioning, only = [surrealkv]);

/// On a versioned store, `getm` as of a version answers each key as `get` does as of it: as of the
/// latest version each live key, as of a version before any write none, a deleted key at neither.
async fn getm_at_a_version(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.del(b"k2".into()).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	let keys = [b"k3".into(), b"k2".into(), b"missing".into(), b"k1".into()];
	let latest = tx.getm(&keys, Some(u64::MAX)).await.unwrap();
	assert_eq!(latest.values[0].as_deref(), Some(&b"k3"[..]));
	assert_eq!(latest.values[1], None, "a deleted key is a miss as of the latest version");
	assert_eq!(latest.values[2], None);
	assert_eq!(latest.values[3].as_deref(), Some(&b"k1"[..]));
	assert_eq!((latest.records, latest.value_bytes), (2, 4));
	for (at, key) in keys.iter().enumerate() {
		let one = tx.get(key.as_borrowed(), Some(u64::MAX)).await.unwrap();
		assert_eq!(latest.values[at], one, "getm answers as get does for key {at}");
	}
	let before = tx.getm(&keys, Some(1)).await.unwrap();
	assert!(before.values.iter().all(Option::is_none), "no key exists before the first write");
	assert_eq!((before.records, before.value_bytes), (0, 0));
	tx.cancel().await.unwrap();
}

kvs_test!(getm_at_a_version, only = [surrealkv_versioned]);

/// `getr` returns the same key/value pairs as `scan` over the same range,
/// and accounts key and value bytes.
async fn getr(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	let rng = KeyRange::from(b"k1"..b"k4");
	let via_getr = tx.getr(rng.as_borrowed(), None).await.unwrap();
	let via_scan = tx.scan(rng, u32::MAX, 0, None).await.unwrap();
	assert_eq!(via_getr.values, via_scan.values, "getr must match scan over the same range");
	let expected: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"k1".to_vec(), b"k1".to_vec()),
		(b"k2".to_vec(), b"k2".to_vec()),
		(b"k3".to_vec(), b"k3".to_vec()),
	];
	assert_eq!(via_getr.values, expected, "the range end is exclusive");
	assert_eq!(via_getr.key_bytes, 6);
	assert_eq!(via_getr.value_bytes, 6);
	tx.cancel().await.unwrap();
}

kvs_test!(getr);

/// `delr` deletes exactly the half-open range; keys outside are untouched.
async fn delr(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.delr(KeyRange::from(b"k2"..b"k4")).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.exists(b"k1".into(), None).await.unwrap());
	assert!(!tx.exists(b"k2".into(), None).await.unwrap());
	assert!(!tx.exists(b"k3".into(), None).await.unwrap());
	assert!(tx.exists(b"k4".into(), None).await.unwrap(), "the range end is exclusive");
	assert!(tx.exists(b"k5".into(), None).await.unwrap());
	tx.cancel().await.unwrap();
}

kvs_test!(delr);

/// `clrr` clears exactly the half-open range; keys outside are untouched.
async fn clrr(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.clrr(KeyRange::from(b"k2"..b"k4")).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.exists(b"k1".into(), None).await.unwrap());
	assert!(!tx.exists(b"k2".into(), None).await.unwrap());
	assert!(!tx.exists(b"k3".into(), None).await.unwrap());
	assert!(tx.exists(b"k4".into(), None).await.unwrap(), "the range end is exclusive");
	tx.cancel().await.unwrap();
}

kvs_test!(clrr);

/// `count` matches the number of keys a scan returns.
async fn count(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.count(KeyRange::from(b"k1"..b"k4"), None).await.unwrap(), 3);
	assert_eq!(tx.count(KeyRange::from(&b"a"[..]..&b"z"[..]), None).await.unwrap(), 5);
	assert_eq!(tx.count(KeyRange::from(&b"x"[..]..&b"z"[..]), None).await.unwrap(), 0);
	tx.cancel().await.unwrap();
}

kvs_test!(count);

/// The keys `count_bounded` counts over `rng`, call by call under `budget`,
/// each call resumed at the key the last answered: the count of each call
/// and the key each resumed at. Every call keeps within its budget, and
/// every resume key lies within the range, after the one before.
async fn count_in_calls(
	tx: &dyn surrealdb_kvs::Transactable,
	rng: &KeyRange<'static>,
	version: Option<u64>,
	budget: u32,
) -> Vec<(usize, Vec<u8>)> {
	let mut calls = Vec::new();
	let mut from = rng.start.as_slice().to_vec();
	loop {
		let part = KeyRange::from(from.clone()..rng.end.as_slice().to_vec());
		let (counted, resume) = tx.count_bounded(part, version, budget).await.unwrap();
		assert!(counted <= budget.max(1) as usize, "a call counts within its budget of {budget}");
		calls.push((counted, from.clone()));
		let Some(resume) = resume else {
			return calls;
		};
		let resume = resume.as_slice().to_vec();
		assert!(resume > from, "a resume key lies after where the call began");
		assert!(resume.as_slice() < rng.end.as_slice(), "a resume key lies within the range");
		from = resume;
	}
}

/// Seed `n` keys `c0000`.. with values matching the keys.
async fn seed_many(ds: &TestDs, n: usize) {
	let tx = ds.transaction(Write).await.unwrap();
	for i in 0..n {
		let key = format!("c{i:04}");
		tx.set(key.as_bytes().into(), key.clone().into_bytes()).await.unwrap();
	}
	tx.commit().await.unwrap();
}

/// `count_bounded`, called under any budget and resumed at each key it
/// answers, sums to `count`, and each call counts exactly the keys from where
/// it began to where it stopped.
async fn count_bounded(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed_many(&ds, 300).await;
	let tx = ds.transaction(Read).await.unwrap();
	for rng in [
		KeyRange::from(&b"c"[..]..&b"d"[..]),
		KeyRange::from(&b"c0100"[..]..&b"c0250"[..]),
		KeyRange::from(&b"x"[..]..&b"z"[..]),
	] {
		let whole = tx.count(rng.as_borrowed(), None).await.unwrap();
		for budget in [0, 1, 2, 7, 64, 149, 150, 151, 299, 300, 301, 10_000] {
			let calls = count_in_calls(tx.as_ref(), &rng, None, budget).await;
			let summed: usize = calls.iter().map(|(counted, _)| counted).sum();
			assert_eq!(summed, whole, "budget {budget} sums to the count over {rng}");
			// Each call counted exactly the keys up to where the next began.
			for pair in calls.windows(2) {
				let (counted, from) = &pair[0];
				let (_, next) = &pair[1];
				let keys = tx
					.keys(KeyRange::from(from.clone()..next.clone()), u32::MAX, 0, None)
					.await
					.unwrap()
					.keys;
				assert_eq!(keys.len(), *counted, "budget {budget}: a call resumes exactly");
			}
		}
	}
	tx.cancel().await.unwrap();
}

kvs_test!(count_bounded);

/// `count_bounded` in a writable transaction counts its own buffered writes
/// and deletes, as `count` does.
async fn count_bounded_sees_buffered_writes(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed_many(&ds, 100).await;
	let tx = ds.transaction(Write).await.unwrap();
	for i in 0..20 {
		tx.del(format!("c{:04}", i * 3).as_bytes().into()).await.unwrap();
	}
	for i in 0..30 {
		let key = format!("c{:04}x", i * 2);
		tx.set(key.as_bytes().into(), key.clone().into_bytes()).await.unwrap();
	}
	let rng = KeyRange::from(&b"c"[..]..&b"d"[..]);
	let whole = tx.count(rng.as_borrowed(), None).await.unwrap();
	assert_eq!(whole, 110);
	for budget in [1, 9, 50, 1_000] {
		let calls = count_in_calls(tx.as_ref(), &rng, None, budget).await;
		let summed: usize = calls.iter().map(|(counted, _)| counted).sum();
		assert_eq!(summed, whole, "budget {budget} counts the buffered writes");
	}
	tx.cancel().await.unwrap();
}

kvs_test!(count_bounded_sees_buffered_writes);

/// On a versioned store, `count_bounded` as of a version sums to `count` as
/// of it: as of the latest version every live key, as of a version before
/// any write none, a deleted key at neither.
async fn count_bounded_at_a_version(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed_many(&ds, 120).await;
	let tx = ds.transaction(Write).await.unwrap();
	for i in 0..40 {
		tx.del(format!("c{:04}", i * 3).as_bytes().into()).await.unwrap();
	}
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	let rng = KeyRange::from(&b"c"[..]..&b"d"[..]);
	for (version, expected) in [(u64::MAX, 80), (1, 0)] {
		let whole = tx.count(rng.as_borrowed(), Some(version)).await.unwrap();
		assert_eq!(whole, expected, "the count as of {version}");
		for budget in [1, 7, 79, 80, 81] {
			let calls = count_in_calls(tx.as_ref(), &rng, Some(version), budget).await;
			let summed: usize = calls.iter().map(|(counted, _)| counted).sum();
			assert_eq!(summed, whole, "budget {budget} as of {version}");
		}
	}
	tx.cancel().await.unwrap();
}

kvs_test!(count_bounded_at_a_version, only = [surrealkv_versioned]);

/// `replace` overwrites an existing key without `put`'s exists check.
async fn replace(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.put(b"test".into(), b"one".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Write).await.unwrap();
	// put on an existing key errors; replace does not
	assert!(tx.put(b"test".into(), b"two".to_vec()).await.is_err());
	tx.replace(b"test".into(), b"two".to_vec()).await.unwrap();
	// replace also inserts absent keys
	tx.replace(b"fresh".into(), b"three".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"two"[..]));
	assert_eq!(tx.get(b"fresh".into(), None).await.unwrap().as_deref(), Some(&b"three"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(replace);

/// `clr` removes the key entirely.
async fn clr(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"test".into(), b"value".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Write).await.unwrap();
	tx.clr(b"test".into()).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert!(!tx.exists(b"test".into(), None).await.unwrap());
	tx.cancel().await.unwrap();
}

kvs_test!(clr);

/// `clrc` clears only when the check matches: a matching value check
/// deletes, a mismatch errors, and a `None` check requires absence.
async fn clrc(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"test".into(), b"value".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Write).await.unwrap();
	// Mismatched check fails and leaves the key in place
	assert!(tx.clrc(b"test".into(), Some(b"wrong")).await.is_err());
	// A None check requires the key to be absent
	assert!(tx.clrc(b"test".into(), None).await.is_err());
	assert!(tx.clrc(b"missing".into(), None).await.is_ok());
	// Matching check deletes
	tx.clrc(b"test".into(), Some(b"value")).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert!(!tx.exists(b"test".into(), None).await.unwrap());
	tx.cancel().await.unwrap();
}

kvs_test!(clrc);
