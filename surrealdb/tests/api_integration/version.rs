#![allow(clippy::unwrap_used)]
#![cfg(feature = "kv-surrealkv")]

use surrealdb::opt::Config;
use surrealdb::types::Value;
use surrealdb_types::object;
use ulid::Ulid;

use super::CreateDb;

pub async fn select_with_version(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;
	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);

	// Create the initial version and record its timestamp.
	let _ = db.query("CREATE user:john SET name = 'John v1'").await.unwrap().check().unwrap();
	let create_ts = chrono::Utc::now();

	// Create a new version by updating the record.
	let _ = db.query("UPDATE user:john SET name = 'John v2'").await.unwrap().check().unwrap();

	// Without VERSION, SELECT should return the latest update.
	let mut response = db.query("SELECT * FROM user").await.unwrap().check().unwrap();
	let Some(name): Option<String> = response.take("name").unwrap() else {
		panic!("query returned no record");
	};
	assert_eq!(name, "John v2");

	// SELECT with VERSION of `create_ts` should return the initial record.
	let version = create_ts.to_rfc3339();
	let mut response = db
		.query(format!("SELECT * FROM user VERSION d'{}'", version))
		.await
		.unwrap()
		.check()
		.unwrap();
	let Some(name): Option<String> = response.take("name").unwrap() else {
		panic!("query returned no record");
	};
	assert_eq!(name, "John v1");

	let mut response = db
		.query(format!("SELECT name FROM user VERSION d'{}'", version))
		.await
		.unwrap()
		.check()
		.unwrap();
	let Some(name): Option<String> = response.take("name").unwrap() else {
		panic!("query returned no record");
	};
	assert_eq!(name, "John v1");

	let mut response = db
		.query(format!("SELECT name FROM user:john VERSION d'{}'", version))
		.await
		.unwrap()
		.check()
		.unwrap();
	let Some(name): Option<String> = response.take("name").unwrap() else {
		panic!("query returned no record");
	};
	assert_eq!(name, "John v1");
}

pub async fn info_for_db_with_versioned_tables(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;
	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);

	// Record the timestamp before creating a testing table.
	let ts_before_create = chrono::Utc::now().to_rfc3339();

	// Create the testing table.
	let _ = db.query("DEFINE TABLE person").await.unwrap().check().unwrap();

	// Record the timestamp after creating the testing table.
	let ts_after_create = chrono::Utc::now().to_rfc3339();

	// Check that historical query shows no table before it was created.
	let q = format!("INFO FOR DB VERSION d'{}'", ts_before_create);
	let mut response = db.query(q).await.unwrap().check().unwrap();
	let info = response.take::<Value>(0).unwrap();
	assert!(info.get("tables").is_empty());

	// Now check that the table shows up later.
	let q = format!("INFO FOR DB VERSION d'{}'", ts_after_create);
	let mut response = db.query(q).await.unwrap().check().unwrap();
	let info = response.take::<Value>(0).unwrap();
	assert_eq!(
		info.get("tables"),
		Value::Object(object! {
			person: "DEFINE TABLE person TYPE ANY SCHEMALESS PERMISSIONS NONE"
		})
	);
}

pub async fn info_for_table_with_versioned_fields(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;
	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);

	// Create the testing table.
	let _ = db.query("DEFINE TABLE person").await.unwrap().check().unwrap();

	// Record the timestamp before creating a field.
	let ts_before_field = chrono::Utc::now().to_rfc3339();
	let _ = db
		.query("DEFINE FIELD firstName ON TABLE person TYPE string")
		.await
		.unwrap()
		.check()
		.unwrap();

	// Record the timestamp after creating the field.
	let ts_after_field = chrono::Utc::now().to_rfc3339();

	// Check that historical query shows no field before it was created.
	let q = format!("INFO FOR TABLE person VERSION d'{}'", ts_before_field);
	let mut response = db.query(q).await.unwrap().check().unwrap();
	let info = response.take::<Value>(0).unwrap();
	assert!(info.get("fields").is_empty());

	// Now check that the field shows up later.
	let q = format!("INFO FOR TABLE person VERSION d'{}'", ts_after_field);
	let mut response = db.query(q).await.unwrap().check().unwrap();
	let info = response.take::<Value>(0).unwrap();
	assert_eq!(
		info.get("fields"),
		Value::Object(object! {
			firstName: "DEFINE FIELD firstName ON person TYPE string PERMISSIONS FULL"
		})
	);
}

define_include_tests!(version => {
	#[test_log::test(tokio::test)]
	select_with_version,
	#[test_log::test(tokio::test)]
	info_for_db_with_versioned_tables,
	#[test_log::test(tokio::test)]
	info_for_table_with_versioned_fields,
});
