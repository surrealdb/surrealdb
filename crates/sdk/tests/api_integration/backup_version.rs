#![cfg(feature = "kv-surrealkv")]

use serde::{Deserialize, Serialize};
use surrealdb_core::cnf::EXPORT_BATCH_SIZE;
use tokio::fs::remove_file;
use ulid::Ulid;

use super::{CreateDb, NS, RecordName};

pub async fn export_import_versions_with_inserts_updates_deletes(new_db: impl CreateDb) {
	let (_, db) = new_db.create_db().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();

	let num_records = (*EXPORT_BATCH_SIZE * 2) as usize;
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
	let export_file = format!("{db_name}.sql");
	db.export(&export_file).with_config().versions(true).await.unwrap();

	// Remove the table to simulate a fresh import
	db.query("REMOVE TABLE user").await.unwrap();

	// Import the database from the file
	db.import(&export_file).await.unwrap();

	// Verify that all records exist as expected
	for i in num_deleted_records..num_records {
		let user = format!("user:user{i}");
		let mut response =
			db.query(format!("SELECT name FROM {}", user)).await.unwrap().check().unwrap();
		let Some(name): Option<String> = response.take("name").unwrap() else {
			panic!("query returned no record");
		};
		assert_eq!(name, format!("Updated User {i}"));
	}

	// Verify that deleted records do not exist
	for i in 0..num_deleted_records {
		let mut response = db
			.query(format!(
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
		.query(format!(
			"
				SELECT name FROM user ORDER BY name DESC START {} LIMIT {}
				",
			num_deleted_records,
			(num_records - num_deleted_records)
		))
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

#[derive(Debug, Serialize, Deserialize)]
struct User {
	name: String,
	age: i32,
	height: f64,
	active: bool,
}

pub async fn export_import_different_data_types(new_db: impl CreateDb) {
	let (_, db) = new_db.create_db().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();

	// Insert a user with different data types
	let _ = db
		.query(
			"
            CREATE user:user1
            SET name = 'User 1', age = 30, height = 5.9, active = true
            ",
		)
		.await
		.unwrap()
		.check()
		.unwrap();

	// Export the database to a file
	let export_file = "different_data_types_backup.sql";
	db.export(export_file).with_config().versions(true).await.unwrap();

	// Remove the table to simulate a fresh import
	db.query("REMOVE TABLE user").await.unwrap();

	// Import the database from the file
	db.import(export_file).await.unwrap();

	// Verify that the record exists
	let mut response = db.query("SELECT name, age, height, active FROM user:user1").await.unwrap();
	let user: Vec<User> = response.take(0).unwrap();
	assert_eq!(user[0].name, "User 1");
	assert_eq!(user[0].age, 30);
	assert_eq!(user[0].height, 5.9);
	assert!(user[0].active);

	// Clean up: remove the export file
	remove_file(export_file).await.unwrap();
}

pub async fn export_import_multiple_tables(new_db: impl CreateDb) {
	let (_, db) = new_db.create_db().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();

	// Insert records into multiple tables
	let _ = db
		.query(
			"
            CREATE user:user1
            SET name = 'User 1'
            ",
		)
		.await
		.unwrap()
		.check()
		.unwrap();

	let _ = db
		.query(
			"
            CREATE product:product1
            SET name = 'Product 1'
            ",
		)
		.await
		.unwrap()
		.check()
		.unwrap();

	// Export the database to a file
	let export_file = "multiple_tables_backup.sql";
	db.export(export_file).with_config().versions(true).await.unwrap();

	// Remove the tables to simulate a fresh import
	db.query("REMOVE TABLE user").await.unwrap();
	db.query("REMOVE TABLE product").await.unwrap();

	// Import the database from the file
	db.import(export_file).await.unwrap();

	// Verify that the records exist
	let mut response = db.query("SELECT name FROM user:user1").await.unwrap();
	let Some(user_name): Option<String> = response.take("name").unwrap() else {
		panic!("query returned no record");
	};
	assert_eq!(user_name, "User 1");

	let mut response = db.query("SELECT name FROM product:product1").await.unwrap();
	let Some(product_name): Option<String> = response.take("name").unwrap() else {
		panic!("query returned no record");
	};
	assert_eq!(product_name, "Product 1");

	// Clean up: remove the export file
	remove_file(export_file).await.unwrap();
}

pub async fn export_import_versioned_records(new_db: impl CreateDb) {
	let (_, db) = new_db.create_db().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();

	let num_versions = (*EXPORT_BATCH_SIZE * 2) as usize;

	// Insert a user
	let _ = db
		.query(
			"
            CREATE user:user1
            SET name = 'User 1'
            ",
		)
		.await
		.unwrap()
		.check()
		.unwrap();

	// Update the user multiple times to create versions
	for i in 1..=num_versions {
		let _ = db
			.query(format!(
				"
                UPDATE user:user1
                SET name = 'Updated User {i}'
                "
			))
			.await
			.unwrap()
			.check()
			.unwrap();
	}

	// Export the database to a file
	let export_file = "versioned_records_backup.sql";
	db.export(export_file).with_config().versions(true).await.unwrap();

	// Remove the table to simulate a fresh import
	db.query("REMOVE TABLE user").await.unwrap();

	// Import the database from the file
	db.import(export_file).await.unwrap();

	// Verify that the record exists with the last update
	let mut response = db.query("SELECT name FROM user:user1").await.unwrap();
	let Some(name): Option<String> = response.take("name").unwrap() else {
		panic!("query returned no record");
	};
	assert_eq!(name, format!("Updated User {}", num_versions));

	// Clean up: remove the export file
	remove_file(export_file).await.unwrap();
}

pub async fn export_import_versioned_range_queries(new_db: impl CreateDb) {
	let (_, db) = new_db.create_db().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();

	let num_records = 100;

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

	// Update the same records to create versions
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

	// Verify range queries on versioned records
	let mut response = db
		.query(
			"
            SELECT name FROM user ORDER BY name DESC START 10 LIMIT 10
            ",
		)
		.await
		.unwrap();

	let expected_users: Vec<RecordName> = response.take(0).unwrap();
	let expected_users: Vec<String> = expected_users.into_iter().map(|user| user.name).collect();

	// Export the database to a file
	let export_file = "versioned_range_queries_backup.sql";
	db.export(export_file).with_config().versions(true).await.unwrap();

	// Remove the table to simulate a fresh import
	db.query("REMOVE TABLE user").await.unwrap();

	// Import the database from the file
	db.import(export_file).await.unwrap();

	// Verify range queries on versioned records
	let mut response = db
		.query(
			"
            SELECT name FROM user ORDER BY name DESC START 10 LIMIT 10
            ",
		)
		.await
		.unwrap();

	let users: Vec<RecordName> = response.take(0).unwrap();
	let users: Vec<String> = users.into_iter().map(|user| user.name).collect();

	assert_eq!(users.len(), expected_users.len());
	assert_eq!(users, expected_users);

	// Clean up: remove the export file
	remove_file(export_file).await.unwrap();
}

pub async fn export_import_retrieve_specific_versions(new_db: impl CreateDb) {
	let (_, db) = new_db.create_db().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();

	// Insert a user with different versions
	let versions = [
		"2024-08-19T08:00:00Z",
		"2024-08-19T09:00:00Z",
		"2024-08-19T10:00:00Z",
		"2024-08-19T11:00:00Z",
		"2024-08-19T12:00:00Z",
	];

	for (i, version) in versions.iter().enumerate() {
		let _ = db
			.query(format!(
				"
                CREATE user:user1
                SET name = 'Updated User {i}' VERSION d'{version}'
                "
			))
			.await
			.unwrap()
			.check()
			.unwrap();
	}

	// Export the database to a file
	let export_file = "retrieve_specific_versions_backup.sql";
	db.export(export_file).with_config().versions(true).await.unwrap();

	// Remove the table to simulate a fresh import
	db.query("REMOVE TABLE user").await.unwrap();

	// Import the database from the file
	db.import(export_file).await.unwrap();

	// Verify that specific versions can be retrieved
	for (i, version) in versions.iter().enumerate() {
		let mut response = db
			.query(format!(
				"
                SELECT name FROM user:user1 VERSION d'{version}'
                "
			))
			.await
			.unwrap();
		let Some(name): Option<String> = response.take("name").unwrap() else {
			panic!("query returned no record");
		};
		assert_eq!(name, format!("Updated User {i}"));
	}

	// Clean up: remove the export file
	remove_file(export_file).await.unwrap();
}

define_include_tests!(backup_version => {
	#[tokio::test]
	export_import_versions_with_inserts_updates_deletes,
	#[tokio::test]
	export_import_different_data_types,
	#[tokio::test]
	export_import_multiple_tables,
	#[tokio::test]
	export_import_versioned_records,
	#[tokio::test]
	export_import_versioned_range_queries,
	#[tokio::test]
	export_import_retrieve_specific_versions,
});
