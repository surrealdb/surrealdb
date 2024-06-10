use crate::key::database::tb;
use crate::key::database::tb::Tb;
use crate::kvs::ScanPage;
use crate::sql::statements::DefineTableStatement;
use crate::sql::TableType;

#[tokio::test]
#[serial]
async fn table_definitions_can_be_scanned() {
	// Setup
	let node_id = Uuid::parse_str("f7b2ba17-90ed-45f9-9aa2-906c6ba0c289").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let test = init(node_id, clock).await.unwrap();
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();

	// Create a table definition
	let namespace = "test_namespace";
	let database = "test_database";
	let table = "test_table";
	let key = Tb::new(namespace, database, table);
	let value = DefineTableStatement {
		name: Default::default(),
		drop: false,
		full: false,
		id: None,
		view: None,
		permissions: Default::default(),
		changefeed: None,
		..Default::default()
	};
	tx.set(&key, &value).await.unwrap();

	// Validate with scan
	match tx
		.scan_paged(
			ScanPage::from(tb::prefix(namespace, database)..tb::suffix(namespace, database)),
			1000,
		)
		.await
	{
		Ok(scan) => {
			assert_eq!(scan.values.len(), 1);
			let read = DefineTableStatement::from(&scan.values[0].1);
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
	let node_id = Uuid::parse_str("13c0e650-1710-489e-bb80-f882bce50b56").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let test = init(node_id, clock).await.unwrap();
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap().inner();

	// Create a table definition
	let namespace = "test_namespace";
	let database = "test_database";
	let table = "test_table";
	let key = Tb::new(namespace, database, table);
	let value = DefineTableStatement {
		name: Default::default(),
		drop: false,
		full: false,
		id: None,
		view: None,
		permissions: Default::default(),
		changefeed: None,
		comment: None,
		if_not_exists: false,
		kind: TableType::Any,
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
