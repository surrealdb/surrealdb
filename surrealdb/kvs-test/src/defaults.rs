//! Contract tests for the `Transactable` methods that have shared default
//! implementations (`getm`, `getr`, `delr`, `clrr`, `count`, `replace`,
//! `clr`, `clrc`). Backends may override any of these, so the contract is
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
