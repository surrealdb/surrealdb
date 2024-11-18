// Tests for exporting and importing data
// Supported by the storage engines and the HTTP protocol

use surrealdb::sql::Array;
use surrealdb_core::sql::Table;
use tokio::fs::remove_file;

#[tokio::test]
async fn export_import() {
    let (permit, db) = new_db().await;
    let db_name = Ulid::new().to_string();
    db.use_ns(NS).use_db(&db_name).await.unwrap();

    // Insert records
    for i in 0..10 {
        let _: Option<ApiRecordId> = db
            .create("user")
            .content(Record {
                name: format!("User {i}"),
            })
            .await
            .unwrap();
    }

    // Drop the permit to release the database lock
    drop(permit);

    // Define the export file name
    let file = format!("{db_name}.sql");

    // Export, remove table, and import
    let res = async {
        db.export(&file).await?;
        db.query("REMOVE TABLE user").await?;
        db.import(&file).await?;
        Result::<(), Error>::Ok(())
    }
    .await;

    // Remove the export file
    remove_file(&file).await.unwrap();

    // Check the result of the export/import operations
    res.unwrap();

    // Verify that all records exist post-import
    for i in 0..10 {
        let mut response = db
            .query(&format!("SELECT name FROM user WHERE name = 'User {i}'"))
            .await
            .unwrap();
        let Some(name): Option<String> = response.take("name").unwrap() else {
            panic!("query returned no record");
        };
        assert_eq!(name, format!("User {i}"));
    }
}

#[tokio::test]
async fn export_with_config() {
    let (permit, db) = new_db().await;
    let db_name = Ulid::new().to_string();
    db.use_ns(NS).use_db(&db_name).await.unwrap();

    // Insert records
    for i in 0..10 {
        // Record on "user" table
        let _: Option<ApiRecordId> = db
            .create("user")
            .content(Record {
                name: format!("User {i}"),
            })
            .await
            .unwrap();

        // Record on "group" table
        let _: Option<ApiRecordId> = db
            .create("group")
            .content(Record {
                name: format!("Group {i}"),
            })
            .await
            .unwrap();
    }

    // Drop the permit to release the database lock
    drop(permit);

    // Define the export file name
    let file = format!("{db_name}.sql");

    // Export, remove table, and import
    let res = async {
        db.export(&file).with_tables(vec!["user".to_string()]).await?;
        db.query("REMOVE TABLE user; REMOVE TABLE group;").await?;
        db.import(&file).await?;
        Result::<(), Error>::Ok(())
    }
    .await;

    // Remove the export file
    remove_file(&file).await.unwrap();

    // Check the result of the export/import operations
    res.unwrap();

    // Verify that no group records were imported
    let mut response = db.query(&format!("SELECT id FROM group")).await.unwrap();
    let tmp: Option<Value> = response.take(0).unwrap();
    assert_eq!(tmp, None);

    // Verify that all user records exist post-import
    for i in 0..10 {
        let mut response = db
            .query(&format!("SELECT name FROM user WHERE name = 'User {i}'"))
            .await
            .unwrap();
        let Some(name): Option<String> = response.take("name").unwrap() else {
            panic!("query returned no record");
        };
        assert_eq!(name, format!("User {i}"));
    }
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
