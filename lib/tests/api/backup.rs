// Tests for exporting and importing data
// Supported by the storage engines and the HTTP protocol

use surrealdb_core::sql::Table;
use tokio::fs::remove_file;

#[test_log::test(tokio::test)]
async fn export_import() {
	let (permit, db) = new_db().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();
	for i in 0..10 {
		let _: Vec<RecordId> = db
			.create("user")
			.content(Record {
				name: format!("User {i}"),
			})
			.await
			.unwrap();
	}
	drop(permit);
	let file = format!("{db_name}.sql");

	let res = async {
		db.export(&file).await?;
		db.query("REMOVE TABLE user").await?;
		db.import(&file).await?;
		Result::<(), Error>::Ok(())
	}
	.await;
	remove_file(file).await.unwrap();
	res.unwrap();
}

#[test_log::test(tokio::test)]
#[cfg(feature = "ml")]
async fn ml_export_import() {
	let (permit, db) = new_db().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();
	db.import("../tests/linear_test.surml").ml().await.unwrap();
	drop(permit);
	let file = format!("{db_name}.surml");
	db.export(&file).ml("Prediction", Version::new(0, 0, 1)).await.unwrap();
	db.import(&file).ml().await.unwrap();
	remove_file(file).await.unwrap();
}
