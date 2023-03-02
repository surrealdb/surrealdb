use surrealdb::sql::{Number, Value};
use surrealdb::{Connection, Error};

async fn async_query<C>(client: Surreal<C>, sql: &str) -> Result<(), Error>
where
	C: Connection,
{
	client.query(sql).await?;
	Ok(())
}

#[tokio::test]
async fn verify_transaction_isolation() {
	// We create two clients
	let client1 = new_db().await;
	client1.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

	let client2 = new_db().await;
	client2.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

	// Create a document with value set to 0
	client1.query("CREATE foo:bar SET value = 0").await.unwrap();

	// The first client will increment value by 1, but it will be delayed by the slow crypto function.
	let sql1 = async_query(
		client1,
		r#"
		BEGIN;
			SELECT * FROM crypto::scrypt::generate("slow");
			UPDATE foo:bar SET value = value + 1;
		COMMIT;
	"#,
	);
	// The second client will increment value by 2, without delay.
	let sql2 = async_query(client2, "UPDATE foo:bar SET value = value + 2");

	// The two queries are run in parallel.
	let _ = tokio::join!(tokio::spawn(sql1), tokio::spawn(sql2));

	// The final value should be 2 or 1,
	// Because when both transaction started, the value was 0.
	// A value of 3 show that the transaction isolation is not respected:
	// client1 has incremented by 1 the value set by the client2's transaction.
	let client3 = new_db().await;
	client3.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let mut response = client3.query("SELECT value FROM foo:bar").await.unwrap();
	let Some(value): Option<i64> = response.take(0).unwrap() else {
		panic!("record not found");
	};
	assert_ne!(value, 3i64);
}
