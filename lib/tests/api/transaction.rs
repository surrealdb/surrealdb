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
async fn transaction_isolation_1<C>(client: Surreal<C>, barrier: Arc<Barrier>) -> Result<(), Error>
where
	C: Connection,
{
	debug!("1 barrier");
	barrier.wait();
	debug!("1 execute");
	client
		.query(
			r#"
			BEGIN;
				LET $value = (SELECT value FROM foo:bar);
				SELECT * FROM crypto::scrypt::generate('slow');
				UPDATE foo:bar SET value1=value,value=value+1;
				SELECT * FROM sleep("500ms");
			COMMIT;"#,
		)
		.await?;
	debug!("1 ends");
	Ok(())
}

// The second transaction increments value by 2.
async fn transaction_isolation_2<C>(client: Surreal<C>, barrier: Arc<Barrier>) -> Result<(), Error>
where
	C: Connection,
{
	debug!("2 barrier");
	barrier.wait();
	debug!("2 sleep");
	// Sleep 200ms to be sure transaction1 has started
	sleep(Duration::from_millis(200));
	debug!("2 execute");
	client.query("UPDATE foo:bar SET value2=value,value=value+2").await?;
	debug!("2 ends");
	Ok(())
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 3))]
async fn verify_transaction_isolation() {
	let client = new_db().await;
	client.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

	// Create a document with initial values.
	client.query("CREATE foo:bar SET value=0,value1=99,value2=99").await.unwrap();

	// The barrier is used to synchronise both transactions.
	let barrier = Arc::new(Barrier::new(3));

	// The two queries are run in parallel.
	let f1 = tokio::spawn(transaction_isolation_1(client.clone(), barrier.clone()));
	let f2 = tokio::spawn(transaction_isolation_2(client.clone(), barrier.clone()));

	// Unlock the execution of both transactions
	barrier.wait();

	// Wait for both transaction's execution.
	let (res1, res2) = tokio::join!(f1, f2);

	// Check that both transaction ran successfully
	res1.unwrap().unwrap();
	res2.unwrap().unwrap();

	let mut response = client.query("SELECT value,value1,value2 FROM foo:bar").await.unwrap();

	// Value should be 3, as it has been incremented by 1 by transaction1, and by 2 by transaction2.
	assert_eq!(response.take::<Option<i32>>("value").unwrap(), Some(3));

	let value1 = response.take::<Option<i32>>("value1").unwrap();
	let value2 = response.take::<Option<i32>>("value2").unwrap();
	match value1 {
		// If transaction1 has an initial value of 0, then transaction2 should have an initial value of 1
		Some(0) => assert_eq!(value2, Some(1)),
		// If transaction1 has an initial value of 2, then transaction2 should have an initial value of 0
		Some(2) => assert_eq!(value2, Some(0)),
		_ => assert!(false, "Unexpected value for value1 {:?}", value1),
	}
}

// The first transaction increments value by 1.
// This transaction uses sleep to be sure it runs longer than transaction2.
async fn transaction_repeatable_read_1<C>(
	client: Surreal<C>,
	barrier: Arc<Barrier>,
) -> Result<(), Error>
where
	C: Connection,
{
	debug!("1 barrier");
	barrier.wait();
	debug!("1 execute");
	client
		.query(
			r#"
			BEGIN;
				LET $f = SELECT value FROM row:42;
				SELECT * FROM sleep("500ms");
				LET $s = SELECT value from row:42;
				CREATE row:43 SET first = $f, second = $s;
			COMMIT;"#,
		)
		.await?;
	debug!("1 ends");
	Ok(())
}

// The second transaction increments value by 2.
async fn transaction_repeatable_read_2<C>(
	client: Surreal<C>,
	barrier: Arc<Barrier>,
) -> Result<(), Error>
where
	C: Connection,
{
	debug!("2 barrier");
	barrier.wait();
	debug!("2 sleep");
	// Sleep 200ms to be sure transaction1 has started
	sleep(Duration::from_millis(200));
	debug!("2 execute");
	client.query("UPDATE row:42 SET value=99").await?;
	debug!("2 ends");
	Ok(())
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 3))]
async fn verify_repeatable_read() {
	let client = new_db().await;
	client.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

	// Create a document with initial values.
	client.query("CREATE row:42 SET value=1").await.unwrap();

	// The barrier is used to synchronise both transactions.
	let barrier = Arc::new(Barrier::new(3));

	// The two queries are run in parallel.
	let f1 = tokio::spawn(transaction_repeatable_read_1(client.clone(), barrier.clone()));
	let f2 = tokio::spawn(transaction_repeatable_read_2(client.clone(), barrier.clone()));

	// Unlock the execution of both transactions
	barrier.wait();

	// Wait for both transaction's execution.
	let (res1, res2) = tokio::join!(f1, f2);

	// Check that both transaction ran successfully
	res1.unwrap().unwrap();
	res2.unwrap().unwrap();

	// row42:Value should be 99. It has been set by the second transaction.
	let mut response = client.query("SELECT value FROM row:42").await.unwrap();
	assert_eq!(response.take::<Option<i32>>("value").unwrap(), Some(99));

	// First and second should be 1 (the initial value).
	// The snapshot has been read before transaction2 has been committed.
	let mut response = client
		.query("SELECT first.value AS first, second.value AS second FROM row:43")
		.await
		.unwrap();
	debug!("{:?}", response);
	assert_eq!(response.take::<Option<i32>>("first").unwrap(), Some(1));
	assert_eq!(response.take::<Option<i32>>("second").unwrap(), Some(1));
}
