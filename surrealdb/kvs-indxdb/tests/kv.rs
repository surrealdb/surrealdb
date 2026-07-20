#![cfg(target_family = "wasm")]
#![allow(clippy::unwrap_used)]

use surrealdb_kvs::api::Transactable;
use surrealdb_kvs::{Error, Key, KeyRange, TransactionType};
use surrealdb_kvs_indxdb::Datastore;
use wasm_bindgen::prelude::*;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen(module = "/src/js/test.js")]
extern "C" {
	#[wasm_bindgen(catch)]
	async fn clear(name: &str) -> Result<(), JsValue>;

	#[wasm_bindgen(catch, js_name = seedOldFormat)]
	async fn seed_old_format(
		name: &str,
		keys: &[u8],
		key_ends: &[u32],
		values: &[u8],
		value_ends: &[u32],
	) -> Result<(), JsValue>;
}

/// Deletes any leftover database with this name and opens a fresh one.
async fn fresh(name: &str) -> Datastore {
	clear(name).await.expect("failed to clear the test database");
	Datastore::new(name).await.expect("failed to open the test database")
}

async fn write_tx(ds: &Datastore) -> Box<dyn Transactable> {
	ds.transaction(TransactionType::Write).await.unwrap()
}

async fn read_tx(ds: &Datastore) -> Box<dyn Transactable> {
	ds.transaction(TransactionType::Read).await.unwrap()
}

fn key(k: &str) -> Key<'_> {
	Key::from(k.as_bytes())
}

fn val(v: &str) -> Vec<u8> {
	v.as_bytes().to_vec()
}

fn rng(start: &str, end: &str) -> KeyRange<'static> {
	KeyRange::from(start.as_bytes().to_vec()..end.as_bytes().to_vec())
}

async fn seed(ds: &Datastore, entries: &[(&str, &str)]) {
	let tx = write_tx(ds).await;
	for (k, v) in entries {
		tx.set(key(k), val(v)).await.unwrap();
	}
	tx.commit().await.unwrap();
}

/// Fetch all committed entries in `start..end` through a fresh read
/// transaction.
async fn committed(ds: &Datastore, start: &str, end: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
	let tx = read_tx(ds).await;
	let res = tx.scan(rng(start, end), u32::MAX, 0, None).await.unwrap();
	tx.cancel().await.unwrap();
	res.values
}

fn pairs(entries: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
	entries.iter().map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec())).collect()
}

#[wasm_bindgen_test]
async fn read_your_writes() {
	let ds = fresh("kvs-test-ryw").await;
	let tx = write_tx(&ds).await;

	assert_eq!(tx.get(key("a"), None).await.unwrap(), None);
	assert!(!tx.exists(key("a"), None).await.unwrap());

	tx.set(key("a"), val("1")).await.unwrap();
	assert_eq!(tx.get(key("a"), None).await.unwrap(), Some(val("1")));
	assert!(tx.exists(key("a"), None).await.unwrap());

	tx.set(key("a"), val("2")).await.unwrap();
	assert_eq!(tx.get(key("a"), None).await.unwrap(), Some(val("2")));

	tx.del(key("a")).await.unwrap();
	assert_eq!(tx.get(key("a"), None).await.unwrap(), None);
	assert!(!tx.exists(key("a"), None).await.unwrap());

	tx.cancel().await.unwrap();
}

#[wasm_bindgen_test]
async fn put_only_inserts() {
	let ds = fresh("kvs-test-put").await;
	seed(&ds, &[("a", "1")]).await;

	let tx = write_tx(&ds).await;
	assert!(matches!(tx.put(key("a"), val("2")).await, Err(Error::TransactionKeyAlreadyExists)));
	tx.put(key("b"), val("2")).await.unwrap();
	// Deleting within the transaction frees the key up again.
	tx.del(key("a")).await.unwrap();
	tx.put(key("a"), val("3")).await.unwrap();
	tx.commit().await.unwrap();

	assert_eq!(committed(&ds, "a", "z").await, pairs(&[("a", "3"), ("b", "2")]));
}

#[wasm_bindgen_test]
async fn conditional_ops() {
	let ds = fresh("kvs-test-cond").await;
	seed(&ds, &[("a", "1")]).await;

	let tx = write_tx(&ds).await;
	// Value must match for an existing key.
	tx.putc(key("a"), val("2"), Some(val("1"))).await.unwrap();
	assert!(matches!(
		tx.putc(key("a"), val("9"), Some(val("1"))).await,
		Err(Error::TransactionConditionNotMet)
	));
	// A `None` check requires the key to be absent.
	tx.putc(key("b"), val("1"), None).await.unwrap();
	assert!(matches!(
		tx.putc(key("b"), val("9"), None).await,
		Err(Error::TransactionConditionNotMet)
	));

	// Same for deletes.
	assert!(matches!(
		tx.delc(key("a"), Some(b"9".as_slice())).await,
		Err(Error::TransactionConditionNotMet)
	));
	tx.delc(key("a"), Some(b"2".as_slice())).await.unwrap();
	tx.delc(key("c"), None).await.unwrap();
	assert!(matches!(tx.delc(key("b"), None).await, Err(Error::TransactionConditionNotMet)));
	tx.commit().await.unwrap();

	assert_eq!(committed(&ds, "a", "z").await, pairs(&[("b", "1")]));
}

#[wasm_bindgen_test]
async fn commit_persists_cancel_discards() {
	let ds = fresh("kvs-test-commit").await;

	let tx = write_tx(&ds).await;
	tx.set(key("a"), val("1")).await.unwrap();
	tx.commit().await.unwrap();
	assert!(matches!(tx.commit().await, Err(Error::TransactionFinished)));
	assert!(tx.closed());

	let tx = write_tx(&ds).await;
	tx.set(key("b"), val("2")).await.unwrap();
	tx.cancel().await.unwrap();

	assert_eq!(committed(&ds, "a", "z").await, pairs(&[("a", "1")]));

	// Read-only transactions can read but not write or commit.
	let tx = read_tx(&ds).await;
	assert_eq!(tx.get(key("a"), None).await.unwrap(), Some(val("1")));
	assert!(matches!(tx.set(key("c"), val("3")).await, Err(Error::TransactionReadonly)));
	assert!(matches!(tx.commit().await, Err(Error::TransactionReadonly)));
}

#[wasm_bindgen_test]
async fn reopen_persists() {
	let ds = fresh("kvs-test-reopen").await;
	seed(&ds, &[("a", "1"), ("b", "2")]).await;
	ds.shutdown().await.unwrap();

	let ds = Datastore::new("kvs-test-reopen").await.unwrap();
	assert_eq!(committed(&ds, "a", "z").await, pairs(&[("a", "1"), ("b", "2")]));
}

#[wasm_bindgen_test]
async fn scan_and_keys() {
	let ds = fresh("kvs-test-scan").await;
	let entries = [
		("a", "A"),
		("b", "B"),
		("c", "C"),
		("d", "D"),
		("e", "E"),
		("f", "F"),
		("g", "G"),
		("h", "H"),
	];
	seed(&ds, &entries).await;

	let tx = read_tx(&ds).await;

	// Full forward scan.
	let res = tx.scan(rng("a", "z"), 100, 0, None).await.unwrap();
	assert_eq!(res.values, pairs(&entries));
	assert_eq!(res.key_bytes, 8);
	assert_eq!(res.value_bytes, 8);

	// Limit and skip.
	let res = tx.scan(rng("a", "z"), 3, 0, None).await.unwrap();
	assert_eq!(res.values, pairs(&entries[..3]));
	let res = tx.scan(rng("a", "z"), 3, 2, None).await.unwrap();
	assert_eq!(res.values, pairs(&entries[2..5]));

	// Reverse.
	let res = tx.scanr(rng("a", "z"), 3, 1, None).await.unwrap();
	assert_eq!(res.values, pairs(&[("g", "G"), ("f", "F"), ("e", "E")]));

	// Keys only.
	let res = tx.keys(rng("b", "e"), 100, 0, None).await.unwrap();
	assert_eq!(res.keys, vec![val("b"), val("c"), val("d")]);
	assert_eq!(res.key_bytes, 3);
	let res = tx.keysr(rng("a", "z"), 2, 1, None).await.unwrap();
	assert_eq!(res.keys, vec![val("g"), val("f")]);

	// Empty and inverted ranges.
	assert!(tx.scan(rng("c", "c"), 100, 0, None).await.unwrap().values.is_empty());
	assert!(tx.scan(rng("z", "a"), 100, 0, None).await.unwrap().values.is_empty());
	assert!(tx.keys(rng("x", "z"), 100, 0, None).await.unwrap().keys.is_empty());

	tx.cancel().await.unwrap();
}

#[wasm_bindgen_test]
async fn scan_merges_pending_state() {
	let ds = fresh("kvs-test-merge").await;
	seed(&ds, &[("a", "1"), ("c", "3"), ("e", "5")]).await;

	let tx = write_tx(&ds).await;
	tx.set(key("b"), val("2")).await.unwrap();
	tx.del(key("c")).await.unwrap();
	tx.set(key("e"), val("55")).await.unwrap();
	// Cache an absent key so the scan also merges an Empty read state.
	assert_eq!(tx.get(key("d"), None).await.unwrap(), None);

	let res = tx.scan(rng("a", "z"), 100, 0, None).await.unwrap();
	assert_eq!(res.values, pairs(&[("a", "1"), ("b", "2"), ("e", "55")]));

	let res = tx.scanr(rng("a", "z"), 100, 0, None).await.unwrap();
	assert_eq!(res.values, pairs(&[("e", "55"), ("b", "2"), ("a", "1")]));

	let res = tx.keys(rng("a", "z"), 100, 0, None).await.unwrap();
	assert_eq!(res.keys, vec![val("a"), val("b"), val("e")]);

	// Limits count merged output, not database rows.
	let res = tx.scan(rng("a", "z"), 2, 0, None).await.unwrap();
	assert_eq!(res.values, pairs(&[("a", "1"), ("b", "2")]));

	tx.commit().await.unwrap();
	assert_eq!(committed(&ds, "a", "z").await, pairs(&[("a", "1"), ("b", "2"), ("e", "55")]));
}

#[wasm_bindgen_test]
async fn scan_with_many_shadowed_rows() {
	let ds = fresh("kvs-test-shadow").await;
	let entries = [
		("a", "A"),
		("b", "B"),
		("c", "C"),
		("d", "D"),
		("e", "E"),
		("f", "F"),
		("g", "G"),
		("h", "H"),
		("i", "I"),
		("j", "J"),
	];
	seed(&ds, &entries).await;

	// Locally delete most of the range: the scan budget must account for the
	// shadowed database rows to still fill the limit.
	let tx = write_tx(&ds).await;
	for (k, _) in &entries[..8] {
		tx.del(key(k)).await.unwrap();
	}
	let res = tx.scan(rng("a", "z"), 5, 0, None).await.unwrap();
	assert_eq!(res.values, pairs(&[("i", "I"), ("j", "J")]));

	let res = tx.keysr(rng("a", "z"), 5, 0, None).await.unwrap();
	assert_eq!(res.keys, vec![val("j"), val("i")]);

	tx.cancel().await.unwrap();
}

#[wasm_bindgen_test]
async fn skip_keys_in_witness() {
	let ds = fresh("skip_keys_in_witness").await;
	let entries = [
		("a", "A"),
		("b", "B"),
		("c", "C"),
		("d", "D"),
		("e", "E"),
		("f", "F"),
		("g", "G"),
		("h", "H"),
		("i", "I"),
		("j", "J"),
	];
	seed(&ds, &entries).await;

	// Locally delete most of the range: the scan budget must account for the
	// shadowed database rows to still fill the limit.
	let tx = read_tx(&ds).await;
	let res = tx.scan(rng("a", "z"), 5, 0, None).await.unwrap();
	assert_eq!(res.values, pairs(&entries[..5]));

	let res = tx.scan(rng("a", "z"), u32::MAX, 5, None).await.unwrap();
	assert_eq!(res.values, pairs(&entries[5..]));

	tx.cancel().await.unwrap();
}

#[wasm_bindgen_test]
async fn savepoints() {
	let ds = fresh("kvs-test-savepoint").await;
	seed(&ds, &[("c", "3")]).await;

	let tx = write_tx(&ds).await;
	tx.set(key("a"), val("1")).await.unwrap();

	tx.new_save_point().await.unwrap();
	tx.set(key("a"), val("2")).await.unwrap();
	tx.set(key("b"), val("1")).await.unwrap();
	tx.del(key("c")).await.unwrap();
	assert_eq!(tx.get(key("a"), None).await.unwrap(), Some(val("2")));
	assert_eq!(tx.get(key("c"), None).await.unwrap(), None);

	tx.rollback_to_save_point().await.unwrap();
	assert_eq!(tx.get(key("a"), None).await.unwrap(), Some(val("1")));
	assert_eq!(tx.get(key("b"), None).await.unwrap(), None);
	assert_eq!(tx.get(key("c"), None).await.unwrap(), Some(val("3")));

	// Releasing a savepoint keeps its writes.
	tx.new_save_point().await.unwrap();
	tx.set(key("b"), val("9")).await.unwrap();
	tx.release_last_save_point().await.unwrap();
	assert_eq!(tx.get(key("b"), None).await.unwrap(), Some(val("9")));

	tx.commit().await.unwrap();
	assert_eq!(committed(&ds, "a", "z").await, pairs(&[("a", "1"), ("b", "9"), ("c", "3")]));
}

#[wasm_bindgen_test]
async fn nested_savepoint_release_then_outer_rollback() {
	let ds = fresh("kvs-test-savepoint-nested").await;
	seed(&ds, &[("a", "1")]).await;

	let tx = write_tx(&ds).await;

	// Outer savepoint, then an inner savepoint that first-touches keys and
	// gets released: rolling back the outer savepoint must still undo the
	// inner savepoint's writes.
	tx.new_save_point().await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(key("a"), val("2")).await.unwrap();
	tx.set(key("b"), val("1")).await.unwrap();
	tx.release_last_save_point().await.unwrap();
	assert_eq!(tx.get(key("b"), None).await.unwrap(), Some(val("1")));

	tx.rollback_to_save_point().await.unwrap();
	assert_eq!(tx.get(key("a"), None).await.unwrap(), Some(val("1")));
	assert_eq!(tx.get(key("b"), None).await.unwrap(), None);

	// A write made under the outer savepoint before the inner one existed
	// must roll back to its pre-outer state, not the inner-savepoint state.
	tx.new_save_point().await.unwrap();
	tx.set(key("c"), val("outer")).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(key("c"), val("inner")).await.unwrap();
	tx.release_last_save_point().await.unwrap();
	tx.rollback_to_save_point().await.unwrap();
	assert_eq!(tx.get(key("c"), None).await.unwrap(), None);

	tx.commit().await.unwrap();
	assert_eq!(committed(&ds, "a", "z").await, pairs(&[("a", "1")]));
}

#[wasm_bindgen_test]
async fn conflict_on_point_read() {
	let ds = fresh("kvs-test-conflict-read").await;
	seed(&ds, &[("k", "1")]).await;

	let tx1 = write_tx(&ds).await;
	assert_eq!(tx1.get(key("k"), None).await.unwrap(), Some(val("1")));

	let tx2 = write_tx(&ds).await;
	tx2.set(key("k"), val("2")).await.unwrap();
	tx2.commit().await.unwrap();

	tx1.set(key("other"), val("x")).await.unwrap();
	assert!(matches!(tx1.commit().await, Err(Error::TransactionConflict(_))));
}

#[wasm_bindgen_test]
async fn conflict_on_point_exists() {
	let ds = fresh("kvs-test-conflict-exists").await;
	seed(&ds, &[("k", "1")]).await;

	let tx1 = write_tx(&ds).await;
	assert!(tx1.exists(key("k"), None).await.unwrap());

	let tx2 = write_tx(&ds).await;
	tx2.del(key("k")).await.unwrap();
	tx2.commit().await.unwrap();

	tx1.set(key("other"), val("x")).await.unwrap();
	assert!(matches!(tx1.commit().await, Err(Error::TransactionConflict(_))));
}

#[wasm_bindgen_test]
async fn conflict_on_observed_absence() {
	let ds = fresh("kvs-test-conflict-empty").await;

	let tx1 = write_tx(&ds).await;
	assert_eq!(tx1.get(key("m"), None).await.unwrap(), None);

	let tx2 = write_tx(&ds).await;
	tx2.set(key("m"), val("1")).await.unwrap();
	tx2.commit().await.unwrap();

	tx1.set(key("other"), val("x")).await.unwrap();
	assert!(matches!(tx1.commit().await, Err(Error::TransactionConflict(_))));
}

#[wasm_bindgen_test]
async fn conflict_on_scan() {
	let ds = fresh("kvs-test-conflict-scan").await;
	seed(&ds, &[("a", "1"), ("b", "2")]).await;

	let tx1 = write_tx(&ds).await;
	let res = tx1.scan(rng("a", "z"), 100, 0, None).await.unwrap();
	assert_eq!(res.values.len(), 2);

	let tx2 = write_tx(&ds).await;
	tx2.set(key("b"), val("22")).await.unwrap();
	tx2.commit().await.unwrap();

	tx1.set(key("c"), val("3")).await.unwrap();
	assert!(matches!(tx1.commit().await, Err(Error::TransactionConflict(_))));
}

#[wasm_bindgen_test]
async fn no_conflict_on_untouched_keys() {
	let ds = fresh("kvs-test-no-conflict").await;
	seed(&ds, &[("a", "1"), ("x", "9")]).await;

	let tx1 = write_tx(&ds).await;
	assert_eq!(tx1.get(key("a"), None).await.unwrap(), Some(val("1")));

	let tx2 = write_tx(&ds).await;
	tx2.set(key("x"), val("10")).await.unwrap();
	tx2.commit().await.unwrap();

	tx1.set(key("b"), val("2")).await.unwrap();
	tx1.commit().await.unwrap();

	assert_eq!(committed(&ds, "a", "z").await, pairs(&[("a", "1"), ("b", "2"), ("x", "10")]));
}

#[wasm_bindgen_test]
async fn versioned_queries_unsupported() {
	let ds = fresh("kvs-test-versioned").await;
	let tx = read_tx(&ds).await;
	assert!(matches!(tx.get(key("a"), Some(1)).await, Err(Error::UnsupportedVersionedQueries)));
	assert!(matches!(
		tx.scan(rng("a", "z"), 10, 0, Some(1)).await,
		Err(Error::UnsupportedVersionedQueries)
	));
	tx.cancel().await.unwrap();
}

#[wasm_bindgen_test]
async fn opens_old_indxdb_format() {
	clear("kvs-test-compat").await.unwrap();
	// Entries a=1, b=2, c=3 in the layout the old indxdb-crate backend wrote.
	seed_old_format("kvs-test-compat", b"abc", &[1, 2, 3], b"123", &[1, 2, 3]).await.unwrap();

	let ds = Datastore::new("kvs-test-compat").await.unwrap();
	let tx = read_tx(&ds).await;
	assert_eq!(tx.get(key("a"), None).await.unwrap(), Some(val("1")));
	assert!(tx.exists(key("b"), None).await.unwrap());
	let res = tx.scan(rng("a", "z"), 100, 0, None).await.unwrap();
	assert_eq!(res.values, pairs(&[("a", "1"), ("b", "2"), ("c", "3")]));
	tx.cancel().await.unwrap();
}
