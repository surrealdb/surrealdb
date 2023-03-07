use dmp::new;
use futures::TryFutureExt;
use log::debug;
use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use surrealdb::sql::{json, Number, Value};
use surrealdb::{Connection, Error};
use test_log::test;

// The first transaction increments value by 1.
// This transaction uses sleep to be sure it runs longer than transaction2.
async fn transaction_isolation_1(db: String, barrier: Arc<Barrier>) -> Result<(), Error> {
	let client = new_db().await;
	client.use_ns(NS).use_db(db.clone()).await.unwrap();
	debug!("1 barrier");
	barrier.wait();
	debug!("1 execute");
	client
		.query(
			r#"
			BEGIN;
				/* 00:00 read the initial value */
				CREATE rec:1 SET value=(SELECT value FROM rec:0); 
				SELECT * FROM sleep("2s"); 
				/* 00:02 before txn2's commit */
				CREATE rec:2 SET value=(SELECT value FROM rec:0); 
				SELECT * FROM sleep("2s");
				/* 00:04 after tnx2's commit; */
				CREATE rec:3 SET value=(SELECT value FROM rec:0);
			COMMIT;"#,
		)
		.await?
		.check()?;
	debug!("1 ends");
	Ok(())
}

// The second transaction increments value by 2.
async fn transaction_isolation_2(db: String, barrier: Arc<Barrier>) -> Result<(), Error> {
	let client = new_db().await;
	client.use_ns(NS).use_db(db.clone()).await.unwrap();
	debug!("2 barrier");
	barrier.wait();
	debug!("2 execute");
	client
		.query(
			r#"
			BEGIN;
				SELECT * FROM sleep("1s");
				/* 00:01 before txn1 check the value */
				UPDATE rec:0 SET value=1;
				SELECT * FROM sleep("2s");
			/* 00:03 before txn1 check the value the second time */
			COMMIT;"#,
		)
		.await?
		.check()?;
	debug!("2 ends");
	Ok(())
}

/// This test checks if the repeatable read isolation level is being properly enforced
#[test(tokio::test(flavor = "multi_thread", worker_threads = 3))]
async fn verify_transaction_isolation() {
	let db = Ulid::new().to_string();
	let client = new_db().await;
	client.use_ns(NS).use_db(db.clone()).await.unwrap();

	// Create a document with initial values.
	client.query("CREATE rec:0 SET value=0").await.unwrap().check().unwrap();

	// The barrier is used to synchronise both transactions.
	let barrier = Arc::new(Barrier::new(3));

	// The two queries are run in parallel.
	let f1 = tokio::spawn(transaction_isolation_1(db.clone(), barrier.clone()));
	let f2 = tokio::spawn(transaction_isolation_2(db.clone(), barrier.clone()));

	// Unlock the execution of both transactions.
	barrier.wait();

	// Wait for both transaction's execution.
	let (res1, res2) = tokio::join!(f1, f2);

	// Check that both transaction ran successfully.
	res1.unwrap().unwrap();
	res2.unwrap().unwrap();

	// `rec:0.value` should be 1, set by txn2.
	assert_eq!(get_value(&client, "value", "rec:0").await, Some(1));

	// `rec:1.value should be 0, the initial value of rec:0.value
	assert_eq!(get_value(&client, "value.value", "rec:1").await, Some(0));

	// `rec:2.value should be 0, the initial value of rec:0.value
	assert_eq!(get_value(&client, "value.value", "rec:2").await, Some(0));

	// `rec:3.value should be 0, the initial value of rec:0.value
	assert_eq!(get_value(&client, "value.value", "rec:3").await, Some(0));
}

/// Helper extracting a value with a SELECT query
async fn get_value<C: Connection>(client: &Surreal<C>, proj: &str, record: &str) -> Option<i32> {
	client
		.query(format!("SELECT {} AS value FROM {}", proj, record))
		.await
		.unwrap()
		.check()
		.unwrap()
		.take::<Option<i32>>("value")
		.unwrap()
}
