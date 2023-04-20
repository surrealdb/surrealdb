// Tests for exporting and importing data
// Supported by the storage engines and the HTTP protocol

use tokio::fs::remove_file;

struct InfoStruct {}

#[tokio::test]
async fn nodes_register() {
	let db = new_db().await;
	let db2 = new_db_replica().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();
	db2.use_ns(NS).use_db(&db_name).await.unwrap();
	let result: Option<Info> =
		db.query("INFO FOR KV").await.unwrap().check().unwrap().take("").unwrap();
	assert_eq!(result, "testing");
}
