//! Contract tests for the `Transactable` methods that have shared default
//! implementations (`getm`, `assert_absent`, `getr`, `delr`, `clrr`, `count`, `count_bounded`,
//! `replace`, `clr`, `clrc`). Backends may override any of these, so the contract is
//! asserted against every backend regardless.

use surrealdb_kvs::TransactionType::*;
use surrealdb_kvs::{KeyRange, Transactable};

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

/// `assert_absent` answers the smallest position whose key holds a value, whatever order the
/// backend reads the keys in, and `None` when none does.
async fn assert_absent(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.assert_absent(&[]).await.unwrap(), None, "no keys hold nothing");
	let missing = [b"m1".into(), b"k0".into(), b"k6".into()];
	assert_eq!(tx.assert_absent(&missing).await.unwrap(), None);
	// `k5` sorts after `k1` but stands first in the input.
	let keys = [b"m1".into(), b"k5".into(), b"m2".into(), b"k1".into()];
	assert_eq!(tx.assert_absent(&keys).await.unwrap(), Some(1), "the smallest position answers");
	let repeated = [b"m1".into(), b"m1".into(), b"k3".into(), b"k3".into()];
	assert_eq!(tx.assert_absent(&repeated).await.unwrap(), Some(2));
	tx.cancel().await.unwrap();
}

kvs_test!(assert_absent);

/// `assert_absent` in a writable transaction judges its own buffered writes and deletes, as the
/// read `put` makes does.
async fn assert_absent_sees_buffered_writes(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.del(b"k4".into()).await.unwrap();
	tx.set(b"k9".into(), b"new".to_vec()).await.unwrap();
	let deleted = [b"k4".into(), b"k8".into()];
	assert_eq!(tx.assert_absent(&deleted).await.unwrap(), None, "a deleted key is absent");
	let written = [b"k8".into(), b"k9".into()];
	assert_eq!(tx.assert_absent(&written).await.unwrap(), Some(1), "a written key is present");
	tx.cancel().await.unwrap();
}

kvs_test!(assert_absent_sees_buffered_writes);

/// `assert_absent` over a few keys spread across a span the store fills densely answers as it does
/// for keys read one at a time.
async fn assert_absent_across_a_full_span(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	for i in 0..2000u32 {
		let key = format!("s{i:05}");
		tx.set(key.as_bytes().into(), b"v".to_vec()).await.unwrap();
	}
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	let sparse =
		[b"s00010x".into(), b"s01990x".into(), b"r".into(), b"t".into(), b"s00500x".into()];
	assert_eq!(tx.assert_absent(&sparse).await.unwrap(), None);
	let late =
		[b"s00010x".into(), b"s01990x".into(), b"r".into(), b"s01999".into(), b"s00000".into()];
	assert_eq!(
		tx.assert_absent(&late).await.unwrap(),
		Some(3),
		"a present key past the walk is found"
	);
	tx.cancel().await.unwrap();
}

kvs_test!(assert_absent_across_a_full_span);

/// `assert_absent` over keys spread across a span whose keys were all deleted answers as it does
/// for keys read one at a time: the deleted keys are absent, and a key written past them is found.
/// This is the shape of a batch of inserts into a table emptied by deletes.
async fn assert_absent_across_a_deleted_span(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	for i in 0..2000u32 {
		let key = format!("s{i:05}");
		tx.set(key.as_bytes().into(), b"v".to_vec()).await.unwrap();
	}
	tx.commit().await.unwrap();
	let tx = ds.transaction(Write).await.unwrap();
	for i in 0..2000u32 {
		let key = format!("s{i:05}");
		tx.del(key.as_bytes().into()).await.unwrap();
	}
	tx.set(b"s01999z".into(), b"v".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	let deleted = [b"s00000".into(), b"s00999".into(), b"s01999".into(), b"s01000x".into()];
	assert_eq!(tx.assert_absent(&deleted).await.unwrap(), None, "deleted keys are absent");
	let past = [b"s00000".into(), b"s01999".into(), b"s01999z".into()];
	assert_eq!(
		tx.assert_absent(&past).await.unwrap(),
		Some(2),
		"a key past the deleted ones is found"
	);
	tx.cancel().await.unwrap();
}

kvs_test!(assert_absent_across_a_deleted_span);

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

/// The keys `visit_key_ranges` hands out for `ranges`, as `(range index, key)`.
async fn visited(
	tx: &dyn Transactable,
	ranges: &[KeyRange<'_>],
	limit: u32,
	stop_at: Option<&'static [u8]>,
) -> (Vec<(usize, Vec<u8>)>, u64) {
	let mut seen = Vec::new();
	let stats = tx
		.visit_key_ranges(ranges, limit, None, &mut |at: usize, key: &[u8]| {
			seen.push((at, key.to_vec()));
			Ok(match stop_at {
				Some(stop) if stop == key => std::ops::ControlFlow::Break(()),
				_ => std::ops::ControlFlow::Continue(()),
			})
		})
		.await
		.unwrap();
	assert_eq!(stats.rows as usize, seen.len(), "the stats count the keys the visitor was handed");
	(seen, stats.key_bytes)
}

fn span<'a>(start: &'a [u8], end: &'a [u8]) -> KeyRange<'a> {
	KeyRange {
		start: start.into(),
		end: end.into(),
	}
}

/// `visit_key_ranges` visits each range's keys in key order, ranges in the order given, with the
/// index of the range; an empty range visits nothing and a range holds its start and not its end.
async fn visit_key_ranges_in_range_order(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	// The ranges are out of key order, two of them are empty, one is empty because nothing lives
	// there, and one ends at a key it must not hold.
	let ranges = [span(b"k4", b"k6"), span(b"k0", b"k0"), span(b"k1", b"k3"), span(b"x", b"y")];
	let (seen, key_bytes) = visited(&*tx, &ranges, 100, None).await;
	let keys: Vec<_> = seen.iter().map(|(at, key)| (*at, key.as_slice())).collect();
	assert_eq!(keys, vec![(0, &b"k4"[..]), (0, b"k5"), (2, b"k1"), (2, b"k2")]);
	assert_eq!(key_bytes, 8);
	let (seen, _) = visited(&*tx, &[], 100, None).await;
	assert!(seen.is_empty());
	tx.cancel().await.unwrap();
}

kvs_test!(visit_key_ranges_in_range_order);

/// `limit` bounds the keys read from one range, not from the call; `Break` ends the whole read
/// after the key it was returned for.
async fn visit_key_ranges_limit_and_break(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	let ranges = [span(b"k1", b"k6"), span(b"k3", b"k6")];
	let (seen, _) = visited(&*tx, &ranges, 2, None).await;
	let keys: Vec<_> = seen.iter().map(|(at, key)| (*at, key.as_slice())).collect();
	assert_eq!(keys, vec![(0, &b"k1"[..]), (0, b"k2"), (1, b"k3"), (1, b"k4")]);
	let (seen, _) = visited(&*tx, &ranges, 100, Some(b"k2")).await;
	let keys: Vec<_> = seen.iter().map(|(at, key)| (*at, key.as_slice())).collect();
	assert_eq!(keys, vec![(0, &b"k1"[..]), (0, b"k2")], "a break reads no later range");
	let (seen, _) = visited(&*tx, &ranges, 0, None).await;
	assert!(seen.is_empty(), "a limit of zero visits nothing");
	tx.cancel().await.unwrap();
}

kvs_test!(visit_key_ranges_limit_and_break);

/// A writable transaction's ranges hold its own buffered writes and deletes.
async fn visit_key_ranges_sees_buffered_writes(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"k25".into(), b"new".to_vec()).await.unwrap();
	tx.del(b"k3".into()).await.unwrap();
	let ranges = [span(b"k2", b"k5"), span(b"k0", b"k2")];
	let (seen, _) = visited(&*tx, &ranges, 100, None).await;
	let keys: Vec<_> = seen.iter().map(|(at, key)| (*at, key.as_slice())).collect();
	assert_eq!(keys, vec![(0, &b"k2"[..]), (0, b"k25"), (0, b"k4"), (1, b"k1")]);
	tx.cancel().await.unwrap();
}

kvs_test!(visit_key_ranges_sees_buffered_writes);

/// A finished transaction reads nothing.
async fn visit_key_ranges_after_the_transaction_finished(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	tx.cancel().await.unwrap();
	let ranges = [span(b"k1", b"k3")];
	let outcome = tx
		.visit_key_ranges(&ranges, 10, None, &mut |_: usize, _: &[u8]| {
			Ok(std::ops::ControlFlow::Continue(()))
		})
		.await;
	assert!(matches!(outcome, Err(surrealdb_kvs::Error::TransactionFinished)));
}

kvs_test!(visit_key_ranges_after_the_transaction_finished);

/// `setm` writes every entry in input order: a repeated key ends up holding
/// its last value, and an empty batch changes nothing.
async fn setm(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.setm(Vec::new()).await.unwrap();
	tx.setm(vec![
		(b"a".into(), b"1".to_vec()),
		(b"b".into(), b"2".to_vec()),
		(b"a".into(), b"3".to_vec()),
	])
	.await
	.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.get(b"a".into(), None).await.unwrap().as_deref(), Some(&b"3"[..]));
	assert_eq!(tx.get(b"b".into(), None).await.unwrap().as_deref(), Some(&b"2"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(setm);

/// `delm` deletes every key, an absent one included.
async fn delm(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.delm(vec![b"k1".into(), b"missing".into(), b"k3".into()]).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.get(b"k1".into(), None).await.unwrap(), None);
	assert_eq!(tx.get(b"k3".into(), None).await.unwrap(), None);
	assert!(tx.get(b"k2".into(), None).await.unwrap().is_some());
	tx.cancel().await.unwrap();
}

kvs_test!(delm);

/// A small deterministic generator, so a failing case reproduces.
struct Lcg(u64);

impl Lcg {
	fn below(&mut self, n: usize) -> usize {
		self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		((self.0 >> 33) as usize) % n
	}
}

/// Random well-formed ranges over `c0000..` keys, overlapping ones and empty ones included.
fn random_ranges(rng: &mut Lcg, n: usize, span_of: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
	(0..n)
		.map(|_| {
			let (a, b) = (rng.below(span_of + 10), rng.below(span_of + 10));
			let (a, b) = (a.min(b), a.max(b));
			(format!("c{a:04}").into_bytes(), format!("c{b:04}").into_bytes())
		})
		.collect()
}

/// What `visit_key_ranges` hands out equals what `keys` reads range by range at the same limit,
/// for random, overlapping and empty ranges, several limits, and a `Break` at every position
/// (which ends the whole read after the key it was returned for).
async fn assert_visit_matches_keys(tx: &dyn Transactable, version: Option<u64>) {
	let mut rng = Lcg(7);
	for round in 0..20 {
		let count = 1 + rng.below(5);
		let raw = random_ranges(&mut rng, count, 60);
		let ranges: Vec<KeyRange<'_>> = raw.iter().map(|(a, b)| span(a, b)).collect();
		for limit in [1u32, 2, 5, 1000] {
			let mut expected = Vec::new();
			let mut expected_bytes = 0;
			for (at, r) in ranges.iter().enumerate() {
				let read = tx.keys(r.as_borrowed(), limit, 0, version).await.unwrap();
				expected_bytes += read.key_bytes;
				expected.extend(read.keys.into_iter().map(|k| (at, k)));
			}
			let mut seen = Vec::new();
			let stats = tx
				.visit_key_ranges(&ranges, limit, version, &mut |at: usize, key: &[u8]| {
					seen.push((at, key.to_vec()));
					Ok(std::ops::ControlFlow::Continue(()))
				})
				.await
				.unwrap();
			assert_eq!(seen, expected, "round {round}, limit {limit}");
			assert_eq!(stats.rows as usize, expected.len());
			assert_eq!(stats.key_bytes, expected_bytes);
			for stop in 0..expected.len().min(12) {
				let mut seen = Vec::new();
				let stats = tx
					.visit_key_ranges(&ranges, limit, version, &mut |at: usize, key: &[u8]| {
						seen.push((at, key.to_vec()));
						Ok(if seen.len() == stop + 1 {
							std::ops::ControlFlow::Break(())
						} else {
							std::ops::ControlFlow::Continue(())
						})
					})
					.await
					.unwrap();
				assert_eq!(seen, expected[..=stop], "round {round}, limit {limit}, break {stop}");
				assert_eq!(stats.rows as usize, stop + 1);
				let seen_bytes: u64 = seen.iter().map(|(_, key)| key.len() as u64).sum();
				assert_eq!(stats.key_bytes, seen_bytes);
			}
		}
	}
}

/// `visit_key_ranges` agrees with `keys` over each range, on committed data.
async fn visit_key_ranges_matches_keys(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed_many(&ds, 60).await;
	let tx = ds.transaction(Read).await.unwrap();
	assert_visit_matches_keys(&*tx, None).await;
	tx.cancel().await.unwrap();
}

kvs_test!(visit_key_ranges_matches_keys);

/// `visit_key_ranges` agrees with `keys` after the transaction's own writes and deletes.
async fn visit_key_ranges_matches_keys_after_own_writes(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed_many(&ds, 60).await;
	let tx = ds.transaction(Write).await.unwrap();
	for i in (0..60).step_by(4) {
		tx.del(format!("c{i:04}").as_bytes().into()).await.unwrap();
	}
	for i in (0..60).step_by(7) {
		tx.set(format!("c{i:04}x").as_bytes().into(), b"v".to_vec()).await.unwrap();
	}
	assert_visit_matches_keys(&*tx, None).await;
	tx.cancel().await.unwrap();
}

kvs_test!(visit_key_ranges_matches_keys_after_own_writes);

/// On a versioned store, `visit_key_ranges` as of a version agrees with `keys` as of it: as of
/// the latest version the live keys, as of a version before any write none.
async fn visit_key_ranges_at_a_version(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed_many(&ds, 60).await;
	let tx = ds.transaction(Write).await.unwrap();
	for i in (0..60).step_by(3) {
		tx.del(format!("c{i:04}").as_bytes().into()).await.unwrap();
	}
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	for version in [u64::MAX, 1] {
		assert_visit_matches_keys(&*tx, Some(version)).await;
	}
	let ranges = [span(b"c0000", b"c9999")];
	let mut visited_any = false;
	tx.visit_key_ranges(&ranges, 1000, Some(1), &mut |_: usize, _: &[u8]| {
		visited_any = true;
		Ok(std::ops::ControlFlow::Continue(()))
	})
	.await
	.unwrap();
	assert!(!visited_any, "no key exists before the first write");
	tx.cancel().await.unwrap();
}

kvs_test!(visit_key_ranges_at_a_version, only = [surrealkv_versioned]);

/// A version on a store that keeps none is refused, as `keys` refuses it.
async fn visit_key_ranges_refuses_a_version_without_versioning(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	let ranges = [span(b"k1", b"k3")];
	let outcome = tx
		.visit_key_ranges(&ranges, 10, Some(1), &mut |_: usize, _: &[u8]| {
			Ok(std::ops::ControlFlow::Continue(()))
		})
		.await;
	assert!(matches!(outcome, Err(surrealdb_kvs::Error::UnsupportedVersionedQueries)));
	tx.cancel().await.unwrap();
}

kvs_test!(visit_key_ranges_refuses_a_version_without_versioning, only = [surrealkv, mem]);
