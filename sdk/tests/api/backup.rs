// Tests for exporting and importing data
// Supported by the storage engines and the HTTP protocol

use surrealdb_core::sql::Table;
use tokio::fs::remove_file;

#[tokio::test]
async fn export_import() {
	let (permit, db) = new_db().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();
	for i in 0..10 {
		let _: Option<ApiRecordId> = db
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

#[tokio::test]
async fn export_import_with_inserts_updates_deletes() {
    let (_, db) = new_db().await;
    let db_name = Ulid::new().to_string();
    db.use_ns(NS).use_db(&db_name).await.unwrap();

	let num_records = 2;

	// Insert a lot of users
    for i in 0..num_records {
		let _ = db
        .query(&format!(
            "
            CREATE user:user{i}
            SET name = 'User {i}'
            "
        ))
        .await
        .unwrap()
        .check()
        .unwrap();
	}

    // Update the same records
    for i in 0..num_records {
		let _ = db
        .query(&format!(
            "
            UPDATE user:user{i}
            SET name = 'Updated User {i}'
            "
        ))
        .await
        .unwrap()
        .check()
        .unwrap();
    }

    // Delete some records
    for i in 0..50 {
        let _ = db
            .query(&format!(
                "
                DELETE user:user{i}
                "
            ))
            .await
            .unwrap()
            .check()
            .unwrap();
    }

	// Export the database to a file
    let export_file = "backup.sql";
    db.export(export_file).await.unwrap();

    // // Remove the table to simulate a fresh import
    // db.query("REMOVE TABLE user").await.unwrap();

    // // Import the database from the file
    // db.import(export_file).await.unwrap();

    // // Verify that all records exist as expected
    // for i in 50..100 {
    //     let response = db
    //         .query(&format!(
    //             "
    //             SELECT name FROM user:user{i}
    //             "
    //         ))
    //         .await
    //         .unwrap();
    //     let record: Option<User> = response.check().unwrap().first().unwrap().result().unwrap();
    //     assert_eq!(record.unwrap().name, format!("Updated User {i}"));
    // }

    // // Verify that deleted records do not exist
    // for i in 0..50 {
    //     let response = db
    //         .query(&format!(
    //             "
    //             SELECT name FROM user:user{i}
    //             "
    //         ))
    //         .await
    //         .unwrap();
    //     let record: Option<User> = response.check().unwrap().first().unwrap().result().unwrap();
    //     assert!(record.is_none());
    // }

    // // Verify range queries
    // let response = db
    //     .query(
    //         "
    //         SELECT name FROM user ORDER BY name DESC START 50 LIMIT 10
    //         ",
    //     )
    //     .await
    //     .unwrap();
    // let records: Vec<User> = response.check().unwrap().result().unwrap();
    // for i in 50..60 {
    //     assert_eq!(records[i - 50].name, format!("Updated User {i}"));
    // }

    // // Clean up: remove the export file
    // std::fs::remove_file(export_file).unwrap();
}
