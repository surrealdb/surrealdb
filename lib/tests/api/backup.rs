// Tests for exporting and importing data
// Supported by the storage engines and the HTTP protocol

use tokio::fs::remove_file;

#[tokio::test]
async fn export_import() {
	let db = new_db().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();
	for i in 0..10 {
		let _: RecordId = db
			.create("user")
			.content(Record {
				name: &format!("User {i}"),
			})
			.await
			.unwrap();
	}
	let file = format!("{db_name}.sql");
	db.export(&file).await.unwrap();
	db.import(&file).await.unwrap();
	remove_file(file).await.unwrap();
}
