//! A transaction that goes out of scope still open is rolled back.

use std::future::IntoFuture;
use std::time::Duration;

use uuid::Uuid;

use super::protocol::{Client, Test};
use super::server::ROLLED_BACK;
use crate::Surreal;

async fn connect() -> Surreal<Client> {
	let db = Surreal::<Client>::init();
	db.connect::<Test>(()).with_capacity(512).await.unwrap();
	db
}

fn rollbacks_of(txn: Uuid) -> usize {
	ROLLED_BACK.lock().unwrap().iter().filter(|id| **id == txn).count()
}

async fn rolled_back(txn: Uuid) -> bool {
	for _ in 0..100 {
		if rollbacks_of(txn) > 0 {
			return true;
		}
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	false
}

#[tokio::test]
async fn dropping_a_transaction_rolls_it_back() {
	let db = connect().await;
	let tx = db.begin().await.unwrap();
	let txn = tx.id;
	drop(tx);
	assert!(rolled_back(txn).await, "the dropped transaction is rolled back");
	assert_eq!(rollbacks_of(txn), 1);
}

#[tokio::test]
async fn a_cancelled_transaction_is_rolled_back_once() {
	let db = connect().await;
	let tx = db.begin().await.unwrap();
	let txn = tx.id;
	tx.cancel().await.unwrap();
	tokio::time::sleep(Duration::from_millis(200)).await;
	assert_eq!(rollbacks_of(txn), 1, "cancel does not roll back a second time on drop");
}

#[tokio::test]
async fn a_committed_transaction_is_not_rolled_back() {
	let db = connect().await;
	let tx = db.begin().await.unwrap();
	let txn = tx.id;
	tx.commit().await.unwrap();
	tokio::time::sleep(Duration::from_millis(200)).await;
	assert_eq!(rollbacks_of(txn), 0);
}

#[tokio::test]
async fn an_unawaited_commit_rolls_the_transaction_back() {
	let db = connect().await;
	let tx = db.begin().await.unwrap();
	let txn = tx.id;
	drop(tx.commit());
	assert!(rolled_back(txn).await, "a commit that never ran leaves nothing open");
}

#[tokio::test]
async fn a_commit_dropped_while_running_rolls_the_transaction_back() {
	let db = connect().await;
	let tx = db.begin().await.unwrap();
	let txn = tx.id;
	// Polled once so the engine call has begun, then dropped before it answers.
	let mut commit = Box::pin(tx.commit().into_future());
	let _ = futures::poll!(commit.as_mut());
	drop(commit);
	assert!(rolled_back(txn).await, "a cancelled commit leaves nothing open");
}
