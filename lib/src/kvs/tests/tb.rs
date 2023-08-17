use crate::key::database::tb;
use crate::key::database::tb::Tb;
use crate::sql::statements::DefineTableStatement;

#[tokio::test]
#[serial]
async fn table_definitions_can_be_scanned() {
	// Setup
	let test = init().await.unwrap();
	let mut tx = test.db.transaction(true, false).await.unwrap();

	// Create a table definition
	let namespace = "test_namespace";
	let database = "test_database";
	let table = "test_table";
	let key = Tb::new(namespace, database, table);
	let value = DefineTableStatement {
		name: Default::default(),
		drop: false,
		full: false,
		view: None,
		permissions: Default::default(),
		changefeed: None,
	};
	tx.set(&key, &value).await.unwrap();

	// Validate with scan
	match tx.scan(tb::prefix(namespace, database)..tb::suffix(namespace, database), 1000).await {
		Ok(scan) => {
			assert_eq!(scan.len(), 1);
			let read = DefineTableStatement::from(&scan[0].1);
			assert_eq!(&read, &value);
		}
		Err(e) => panic!("{:?}", e),
	}
	tx.commit().await.unwrap();
}

#[tokio::test]
#[serial]
async fn table_definitions_can_be_deleted() {
	// Setup
	let test = init().await.unwrap();
	let mut tx = test.db.transaction(true, false).await.unwrap();

	// Create a table definition
	let namespace = "test_namespace";
	let database = "test_database";
	let table = "test_table";
	let key = Tb::new(namespace, database, table);
	let value = DefineTableStatement {
		name: Default::default(),
		drop: false,
		full: false,
		view: None,
		permissions: Default::default(),
		changefeed: None,
	};
	tx.set(&key, &value).await.unwrap();

	// Validate delete
	tx.del(&key).await.unwrap();

	// Should not exist
	match tx.get(&key).await {
		Ok(None) => {}
		Ok(Some(o)) => panic!("Should not exist but was {:?}", o),
		Err(e) => panic!("Unexpected error on get {:?}", e),
	};
	tx.commit().await.unwrap();
}
