#[tokio::test]
async fn export_import_versions_with_inserts_updates_deletes() {
    let (_, db) = new_db().await;
    let db_name = Ulid::new().to_string();
    db.use_ns(NS).use_db(&db_name).await.unwrap();

    let num_records = 1000;
    let num_deleted_records = num_records / 2;

	// Insert a lot of users
    for i in 0..num_records {
		let _ = db
        .query(format!(
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
        .query(format!(
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
    for i in 0..num_deleted_records {
        let _ = db
            .query(format!(
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

    // Remove the table to simulate a fresh import
    db.query("REMOVE TABLE user").await.unwrap();

    // Import the database from the file
    db.import(export_file).await.unwrap();

    // Verify that all records exist as expected
	for i in num_deleted_records..num_records {
		let user = format!("user:user{i}");
		let mut response = db
			.query(&format!("SELECT name FROM {}", user))
			.await
			.unwrap()
			.check()
			.unwrap();
		let Some(name): Option<String> = response.take("name").unwrap() else {
			panic!("query returned no record");
		};
		assert_eq!(name, format!("Updated User {i}"));
	}

	// Verify that deleted records do not exist
	for i in 0..num_deleted_records {
		let mut response = db
			.query(&format!(
				"
				SELECT name FROM user:user{i}
				"
			))
			.await
			.unwrap();
		let name: Option<String> = response.take("name").unwrap();
		assert!(name.is_none());
	}

	// Verify range queries
	let mut response = db
		.query(
			&format!(
				"
				SELECT name FROM user ORDER BY name DESC START {} LIMIT {}
				",
				num_deleted_records, (num_records - num_deleted_records)
			),
		)
		.await
		.unwrap();

	let users: Vec<RecordName> = response.take(0).unwrap();
	let users: Vec<String> = users.into_iter().map(|user| user.name).collect();

	for (i, record) in users.iter().enumerate() {
		let expected_index = num_records - 1 - i;
		assert_eq!(*record, format!("Updated User {}", expected_index));
	}

	// Clean up: remove the export file
	remove_file(export_file).await.unwrap();
}
