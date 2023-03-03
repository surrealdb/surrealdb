use futures::TryFutureExt;
use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use surrealdb::sql::{json, Number, Value};
use surrealdb::{Connection, Error};

fn test_log(msg: &str) {
	println!(
		"{} {}",
		SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis(),
		msg,
	);
}

// The first transaction increments value by 1.
// This transaction uses sleep to be sure it runs longer than transaction2.
async fn transaction1(db: String, barrier: Arc<Barrier>) -> Result<(), Error> {
	test_log("1 start");
	let client = new_db().await;
	client.use_ns(NS).use_db(db).await.unwrap();

	test_log("1 barrier");
	barrier.wait();
	test_log("1 execute");
	client
		.query(
			r#"
			BEGIN TRANSACTION;
				LET $value = (SELECT value FROM foo:bar);
				SELECT * FROM crypto::scrypt::generate('slow');
				UPDATE foo:bar SET value1 = value, value = value + 1;
				SELECT * FROM sleep("500ms");
			COMMIT TRANSACTION;
	"#,
		)
		.await?;
	test_log("1 ends");
	Ok(())
}

// The second transaction increments value by 2.
async fn transaction2(db: String, barrier: Arc<Barrier>) -> Result<(), Error> {
	test_log("2 start");
	let client = new_db().await;
	client.use_ns(NS).use_db(db).await.unwrap();
	test_log("2 barrier");
	barrier.wait();
	test_log("2 sleep");
	// Sleep 200ms to be sure transaction1 has started
	sleep(Duration::from_millis(200));
	test_log("2 execute");
	client.query("UPDATE foo:bar SET value2 = value, value = value + 2").await?;
	test_log("2 ends");
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn verify_transaction_isolation() {
	let db = Ulid::new().to_string();

	let client = new_db().await;
	client.use_ns(NS).use_db(db.clone()).await.unwrap();

	// Create a document with initial values.
	client.query("CREATE foo:bar SET value = 0, value1 = 99, value2 = 99").await.unwrap();

	// The barrier is used to synchronise both transactions.
	let barrier = Arc::new(Barrier::new(3));

	// The two queries are run in parallel.
	let f1 = tokio::spawn(transaction1(db.clone(), barrier.clone()));
	let f2 = tokio::spawn(transaction2(db.clone(), barrier.clone()));

	// Unlock the execution of both transactions
	barrier.wait();

	// Wait for both transaction's execution.
	let (res1, res2) = tokio::join!(f1, f2);

	// Check that both transaction ran successfully
	res1.unwrap().unwrap();
	res2.unwrap().unwrap();

	// Because when both transaction started, the value was 0,
	// the final value should be 2 if transaction 2 ends last,
	// or 1 if transaction 1 ends last.
	// A value of 3 show that the transaction isolation is not respected:
	// client1 has incremented by 1 the value set by the client2's transaction (or the opposite).
	let mut response = client.query("SELECT value,value1,value2 FROM foo:bar").await.unwrap();
	assert_eq!(response.take::<Option<i32>>("value").unwrap(), Some(3));
	let value1 = response.take::<Option<i32>>("value1").unwrap();
	let value2 = response.take::<Option<i32>>("value2").unwrap();
	// One transaction should have an initial value of 0, the other an initial value of 2.
	match value1 {
		Some(0) => assert_eq!(value2, Some(1)),
		Some(2) => assert_eq!(value2, Some(0)),
		_ => assert!(false, "Unexpected value for value1 {:?}", value1),
	}
}
