//! Transaction lifecycle: closed-state reporting, operations on finished
//! transactions, double commits, and read-only enforcement.

use surrealdb_kvs::TransactionType::*;
use surrealdb_kvs::{Error, KeyRange};

use crate::{TestBackend, kvs_test};

/// `closed()` flips after a commit.
async fn closed_after_commit(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	assert!(!tx.closed());
	tx.set(b"test".into(), b"value".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	assert!(tx.closed());
}

kvs_test!(closed_after_commit);

/// `closed()` flips after a cancel.
async fn closed_after_cancel(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	assert!(!tx.closed());
	tx.cancel().await.unwrap();
	assert!(tx.closed());
}

kvs_test!(closed_after_cancel);

/// Operations on a committed transaction fail. The default-implemented
/// methods are required to return [`Error::TransactionFinished`]; the
/// backend-implemented primitives must at least error.
async fn ops_after_commit_error(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"test".into(), b"value".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Backend-implemented primitives
	assert!(tx.get(b"test".into(), None).await.is_err());
	assert!(tx.set(b"test".into(), b"other".to_vec()).await.is_err());
	assert!(tx.exists(b"test".into(), None).await.is_err());
	assert!(tx.scan(KeyRange::from(&b"a"[..]..&b"z"[..]), 10, 0, None).await.is_err());
	// Default-implemented methods enforce the variant themselves
	assert!(matches!(tx.getm(&[b"test".into()], None).await, Err(Error::TransactionFinished)));
	assert!(matches!(
		tx.getr(KeyRange::from(&b"a"[..]..&b"z"[..]), None).await,
		Err(Error::TransactionFinished)
	));
	assert!(matches!(
		tx.count(KeyRange::from(&b"a"[..]..&b"z"[..]), None).await,
		Err(Error::TransactionFinished)
	));
	assert!(matches!(
		tx.delr(KeyRange::from(&b"a"[..]..&b"z"[..])).await,
		Err(Error::TransactionFinished)
	));
}

kvs_test!(ops_after_commit_error);

/// Committing twice is an error.
async fn double_commit_errors(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"test".into(), b"value".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	assert!(tx.commit().await.is_err());
	// The first commit still took effect
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.exists(b"test".into(), None).await.unwrap());
	tx.cancel().await.unwrap();
}

kvs_test!(double_commit_errors);

/// Committing a cancelled transaction is an error, and its writes stay
/// discarded.
async fn commit_after_cancel_errors(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"test".into(), b"value".to_vec()).await.unwrap();
	tx.cancel().await.unwrap();
	assert!(tx.commit().await.is_err());
	let tx = ds.transaction(Read).await.unwrap();
	assert!(!tx.exists(b"test".into(), None).await.unwrap());
	tx.cancel().await.unwrap();
}

kvs_test!(commit_after_cancel_errors);

/// Writes on a read-only transaction are rejected, and `writeable()`
/// reports the transaction type.
async fn readonly_write_errors(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	assert!(tx.writeable());
	tx.set(b"test".into(), b"value".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert!(!tx.writeable());
	assert!(tx.set(b"test".into(), b"other".to_vec()).await.is_err());
	assert!(tx.put(b"fresh".into(), b"other".to_vec()).await.is_err());
	assert!(tx.del(b"test".into()).await.is_err());
	// The default-implemented range delete enforces the variant itself
	assert!(matches!(
		tx.delr(KeyRange::from(&b"a"[..]..&b"z"[..])).await,
		Err(Error::TransactionReadonly)
	));
	// Reads still work and the data is unchanged
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"value"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(readonly_write_errors);

/// Cancel discards buffered writes.
async fn cancel_discards_writes(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"a".into(), b"1".to_vec()).await.unwrap();
	tx.put(b"b".into(), b"2".to_vec()).await.unwrap();
	tx.cancel().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert!(!tx.exists(b"a".into(), None).await.unwrap());
	assert!(!tx.exists(b"b".into(), None).await.unwrap());
	tx.cancel().await.unwrap();
}

kvs_test!(cancel_discards_writes);
