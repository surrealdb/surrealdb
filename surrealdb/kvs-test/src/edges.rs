//! Key and range edge cases: empty and inverted ranges, the `0xff` prefix
//! boundary, large values, and extreme limits.
//!
//! The empty key (`b""`) is deliberately not covered: its behaviour is
//! currently backend-defined (TiKV's storage layer rejects empty keys).

use surrealdb_kvs::TransactionType::*;
use surrealdb_kvs::{Key, KeyRange};

use crate::{TestBackend, kvs_test};

/// A range with `start == end` is empty for every range operation.
async fn start_equals_end(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();

	tx.set(b"k".into(), b"v".to_vec()).await.unwrap();
	tx.commit().await.unwrap();

	let tx = ds.transaction(Read).await.unwrap();
	let rng = KeyRange::from(b"k"..b"k");

	assert!(tx.keys(rng.as_borrowed(), 10, 0, None).await.unwrap().keys.is_empty());
	assert!(tx.scan(rng.as_borrowed(), 10, 0, None).await.unwrap().values.is_empty());
	assert_eq!(tx.count(rng.as_borrowed(), None).await.unwrap(), 0);
	assert!(tx.getr(rng, None).await.unwrap().values.is_empty());

	tx.cancel().await.unwrap();
}

kvs_test!(start_equals_end);

/// An inverted range (`start > end`) yields empty results rather than an
/// error; this test pins that contract.
async fn inverted_range(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();

	tx.set(b"k".into(), b"v".to_vec()).await.unwrap();
	tx.commit().await.unwrap();

	let tx = ds.transaction(Read).await.unwrap();
	let rng = KeyRange::from(b"z"..b"a");

	assert!(tx.keys(rng.as_borrowed(), 10, 0, None).await.unwrap().keys.is_empty());
	assert!(tx.scan(rng.as_borrowed(), 10, 0, None).await.unwrap().values.is_empty());
	assert_eq!(tx.count(rng, None).await.unwrap(), 0);

	tx.cancel().await.unwrap();
}

kvs_test!(inverted_range);

/// Keys around the `0xff` byte boundary sort and range correctly: ordering
/// is plain lexicographic on bytes, range ends are exclusive, and
/// `Key::next()` steps past a `0xff`-suffixed key.
async fn prefix_ff_boundary(b: &TestBackend) {
	let ds = b.create_ds().await;
	let keys: [&[u8]; 4] = [&[0xfe], &[0xff], &[0xff, 0x00], &[0xff, 0xff]];
	let tx = ds.transaction(Write).await.unwrap();

	for (i, k) in keys.iter().enumerate() {
		tx.set((*k).into(), vec![i as u8]).await.unwrap();
	}

	tx.commit().await.unwrap();

	let tx = ds.transaction(Read).await.unwrap();

	// Full order over the whole span
	let rng = KeyRange::from(&[0x00u8][..]..&[0xff, 0xff, 0xff][..]);
	let all = tx.keys(rng, u32::MAX, 0, None).await.unwrap();

	let got: Vec<Vec<u8>> = all.keys;
	let expected: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
	assert_eq!(got, expected, "keys around 0xff must sort lexicographically");

	// The end bound is exclusive even at the 0xff boundary
	let rng = KeyRange::from(&[0xfeu8][..]..&[0xffu8][..]);
	let got = tx.keys(rng, u32::MAX, 0, None).await.unwrap().keys;
	assert_eq!(got, vec![vec![0xfeu8]]);

	// Key::next() forms the tightest exclusive-start continuation: scanning
	// from just after [0xff] must yield the 0xff-prefixed successors.
	let start = Key::from(&[0xffu8][..]).next();
	let rng = KeyRange::from(start..Key::from(&[0xff, 0xff, 0xff][..]));
	let got = tx.keys(rng, u32::MAX, 0, None).await.unwrap().keys;
	assert_eq!(got, vec![vec![0xff, 0x00], vec![0xff, 0xff]]);

	tx.cancel().await.unwrap();
}

kvs_test!(prefix_ff_boundary);

/// Single-byte keys spanning the full byte value space round-trip and sort.
async fn single_byte_span(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();

	for byte in [0x01u8, 0x40, 0x80, 0xc0, 0xfe] {
		tx.set((&[byte][..]).into(), vec![byte]).await.unwrap();
	}
	tx.commit().await.unwrap();

	let tx = ds.transaction(Read).await.unwrap();

	let rng = KeyRange::from(&[0x00u8][..]..&[0xffu8][..]);
	let scan = tx.scan(rng, u32::MAX, 0, None).await.unwrap();
	let got: Vec<(Vec<u8>, Vec<u8>)> = scan.values;
	let expected: Vec<(Vec<u8>, Vec<u8>)> =
		[0x01u8, 0x40, 0x80, 0xc0, 0xfe].iter().map(|b| (vec![*b], vec![*b])).collect();
	assert_eq!(got, expected);

	tx.cancel().await.unwrap();
}

kvs_test!(single_byte_span);

/// A ~1 MiB value round-trips unchanged (stays under TiKV's limits).
async fn large_value_roundtrip(b: &TestBackend) {
	let ds = b.create_ds().await;
	let val: Vec<u8> = (0..(1024 * 1024)).map(|i| (i % 251) as u8).collect();

	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"large".into(), val.clone()).await.unwrap();
	tx.commit().await.unwrap();

	let tx = ds.transaction(Read).await.unwrap();
	let got = tx.get(b"large".into(), None).await.unwrap().unwrap();
	assert_eq!(got.len(), val.len());
	assert_eq!(got, val);

	tx.cancel().await.unwrap();
}

kvs_test!(large_value_roundtrip);

/// `limit == 0` returns empty results without error.
async fn zero_limit(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();

	tx.set(b"k".into(), b"v".to_vec()).await.unwrap();
	tx.commit().await.unwrap();

	let tx = ds.transaction(Read).await.unwrap();

	let rng = KeyRange::from(&b"a"[..]..&b"z"[..]);
	assert!(tx.keys(rng.as_borrowed(), 0, 0, None).await.unwrap().keys.is_empty());
	assert!(tx.scan(rng.as_borrowed(), 0, 0, None).await.unwrap().values.is_empty());
	assert!(tx.keysr(rng.as_borrowed(), 0, 0, None).await.unwrap().keys.is_empty());
	assert!(tx.scanr(rng, 0, 0, None).await.unwrap().values.is_empty());

	tx.cancel().await.unwrap();
}

kvs_test!(zero_limit);

/// `limit == u32::MAX` with a skip beyond the result count yields empty
/// results without error.
async fn max_limit_skip_past_end(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();

	for i in 1..=3u8 {
		tx.set(format!("k{i}").as_bytes().into(), vec![i]).await.unwrap();
	}

	tx.commit().await.unwrap();

	let tx = ds.transaction(Read).await.unwrap();

	let rng = KeyRange::from(&b"a"[..]..&b"z"[..]);
	assert!(tx.keys(rng.as_borrowed(), u32::MAX, 100, None).await.unwrap().keys.is_empty());
	assert!(tx.scan(rng.as_borrowed(), u32::MAX, 100, None).await.unwrap().values.is_empty());
	// A skip inside the result set still applies with the max limit
	let got = tx.keys(rng, u32::MAX, 2, None).await.unwrap().keys;
	assert_eq!(got, vec![b"k3".to_vec()]);

	tx.cancel().await.unwrap();
}

kvs_test!(max_limit_skip_past_end);
