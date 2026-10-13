//! Savepoints: `new_save_point` / `rollback_to_save_point` /
//! `release_last_save_point` are required trait methods on every backend.

use surrealdb_kvs::Error;
use surrealdb_kvs::TransactionType::*;

use crate::{TestBackend, kvs_test};

/// Writes made after a savepoint are reverted by rollback; writes made
/// before it — and previously committed data — survive.
async fn rollback_reverts_writes(b: &TestBackend) {
	let ds = b.create_ds().await;
	// Committed baseline
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"committed".into(), b"before".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// In-tx: pre-savepoint write, savepoint, post-savepoint mutations
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"pre".into(), b"kept".to_vec()).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"post".into(), b"discarded".to_vec()).await.unwrap();
	tx.set(b"committed".into(), b"overwritten".to_vec()).await.unwrap();
	tx.del(b"pre".into()).await.unwrap();
	tx.rollback_to_save_point().await.unwrap();
	// In-tx visibility after rollback
	let val = tx.get(b"pre".into(), None).await.unwrap();
	assert_eq!(val.as_deref(), Some(&b"kept"[..]), "pre-savepoint write must survive rollback");
	let val = tx.get(b"post".into(), None).await.unwrap();
	assert!(val.is_none(), "post-savepoint write must be reverted");
	let val = tx.get(b"committed".into(), None).await.unwrap();
	assert_eq!(val.as_deref(), Some(&b"before"[..]), "committed value must be restored");
	tx.commit().await.unwrap();
	// Post-commit state matches the in-tx view
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.get(b"post".into(), None).await.unwrap().is_none());
	assert_eq!(tx.get(b"pre".into(), None).await.unwrap().as_deref(), Some(&b"kept"[..]));
	assert_eq!(tx.get(b"committed".into(), None).await.unwrap().as_deref(), Some(&b"before"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(rollback_reverts_writes);

/// A value written in the transaction before the savepoint — not just a
/// committed one — is what rollback restores.
async fn rollback_restores_uncommitted_previous_value(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"test".into(), b"uncommitted".to_vec()).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"test".into(), b"overwritten".to_vec()).await.unwrap();
	tx.rollback_to_save_point().await.unwrap();
	let val = tx.get(b"test".into(), None).await.unwrap();
	assert_eq!(val.as_deref(), Some(&b"uncommitted"[..]));
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"uncommitted"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(rollback_restores_uncommitted_previous_value);

/// Savepoints nest: each rollback reverts to the most recent savepoint.
async fn nested(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"outer".into(), b"a".to_vec()).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"inner".into(), b"b".to_vec()).await.unwrap();
	// Rolling back the inner savepoint reverts only the inner write
	tx.rollback_to_save_point().await.unwrap();
	assert!(tx.get(b"inner".into(), None).await.unwrap().is_none());
	assert_eq!(tx.get(b"outer".into(), None).await.unwrap().as_deref(), Some(&b"a"[..]));
	// Rolling back the outer savepoint reverts the rest
	tx.rollback_to_save_point().await.unwrap();
	assert!(tx.get(b"outer".into(), None).await.unwrap().is_none());
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.get(b"outer".into(), None).await.unwrap().is_none());
	assert!(tx.get(b"inner".into(), None).await.unwrap().is_none());
	tx.cancel().await.unwrap();
}

kvs_test!(nested);

/// Releasing a savepoint keeps its writes, which then commit normally.
async fn release_keeps_writes(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"test".into(), b"kept".to_vec()).await.unwrap();
	tx.release_last_save_point().await.unwrap();
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"kept"[..]));
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"kept"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(release_keeps_writes);

/// Releasing a nested savepoint keeps its writes undoable by the savepoint
/// that encloses it: rolling that one back reverts both scopes.
///
/// Core reaches this through a synchronous `DEFINE EVENT`. The per-document
/// savepoint taken by an INSERT or UPSERT is still open while the event's own
/// statement takes and releases one of its own, so a create attempt that fails
/// after the event has run must still undo everything it wrote.
async fn release_nested_then_rollback_enclosing(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"outer".into(), b"a".to_vec()).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"inner".into(), b"b".to_vec()).await.unwrap();
	// Release keeps the inner write visible
	tx.release_last_save_point().await.unwrap();
	assert_eq!(tx.get(b"inner".into(), None).await.unwrap().as_deref(), Some(&b"b"[..]));
	// Rolling back the enclosing savepoint reverts both scopes' writes
	tx.rollback_to_save_point().await.unwrap();
	assert!(
		tx.get(b"inner".into(), None).await.unwrap().is_none(),
		"a released savepoint's writes must still be undone by the enclosing rollback"
	);
	assert!(
		tx.get(b"outer".into(), None).await.unwrap().is_none(),
		"the enclosing savepoint's own writes must be undone"
	);
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.get(b"inner".into(), None).await.unwrap().is_none());
	assert!(tx.get(b"outer".into(), None).await.unwrap().is_none());
	tx.cancel().await.unwrap();
}

kvs_test!(release_nested_then_rollback_enclosing);

/// Releasing the outermost savepoint closes the scope, leaving nothing to roll
/// back to and no savepoint to reach past.
async fn release_outermost_then_rollback_errors(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"test".into(), b"kept".to_vec()).await.unwrap();
	tx.release_last_save_point().await.unwrap();
	assert!(
		matches!(tx.rollback_to_save_point().await, Err(Error::NoSavepoint)),
		"rollback after releasing the only savepoint must report NoSavepoint"
	);
	// The released write is untouched by the refused rollback
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"kept"[..]));
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"kept"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(release_outermost_then_rollback_errors);

/// A transaction stays usable after a rollback: later writes commit.
async fn rollback_then_continue(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"test".into(), b"discarded".to_vec()).await.unwrap();
	tx.rollback_to_save_point().await.unwrap();
	tx.set(b"test".into(), b"final".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"final"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(rollback_then_continue);

/// Rolling back with no savepoint on the stack is an error, while releasing
/// with none is accepted.
async fn underflow_errors(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	assert!(
		matches!(tx.rollback_to_save_point().await, Err(Error::NoSavepoint)),
		"rollback with no savepoint must report NoSavepoint"
	);
	assert!(
		tx.release_last_save_point().await.is_ok(),
		"release with no savepoint must be accepted"
	);
	tx.cancel().await.unwrap();
}

kvs_test!(underflow_errors);

/// Savepoint writes and the savepoint itself are discarded by cancel.
async fn discarded_by_cancel(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"test".into(), b"discarded".to_vec()).await.unwrap();
	tx.cancel().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.get(b"test".into(), None).await.unwrap().is_none());
	tx.cancel().await.unwrap();
}

kvs_test!(discarded_by_cancel);
