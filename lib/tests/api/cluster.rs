// Tests for exporting and importing data
// Supported by the storage engines and the HTTP protocol

use tokio::fs::remove_file;

#[tokio::test]
async fn nodes_register() {
	let db = new_db().await;
	let db2 = new_db_replica().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();
	db2.use_ns(NS).use_db(&db_name).await.unwrap();
	for i in 0..10 {
		let _: RecordId = db
			.create("user")
			.content(Record {
				name: &format!("User {i}"),
			})
			.await
			.unwrap();
	}
	let results: Vec<RecordBuf> = db2.select("user").await.unwrap();
	assert_eq!(results.len(), 10);
}
