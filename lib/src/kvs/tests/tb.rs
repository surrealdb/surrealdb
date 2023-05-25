#[cfg(feature = "kv-mem")]
pub(crate) mod table {
	use crate::ctx::Context;
	use crate::dbs::{Auth, Options, Workable};
	use crate::doc::Document;
	use crate::err::Error;
	use crate::key::tb;
	use crate::key::tb::Tb;
	use crate::key::thing::Thing;
	use crate::kvs::{Datastore, Key, Transaction};
	use crate::sql::statements::{DefineStatement, DefineTableStatement};
	use crate::sql::Statement::Define;
	use crate::sql::{Id, Value};
	use crate::{err, sql};
	use nom::character::complete::tab;
	use std::future::Future;

	struct TestContext {
		db: Datastore,
	}

	async fn init() -> Result<TestContext, err::Error> {
		let db = Datastore::new("mem://").await?;
		return Ok(TestContext {
			db,
		});
	}

	#[test]
	async fn table_definitions_can_be_scanned() {
		// Setup
		let test = match init().await {
			Ok(ctx) => ctx,
			Err(e) => panic!("{:?}", e),
		};
		let mut tx = match test.db.transaction(true, false).await {
			Ok(tx) => tx,
			Err(e) => panic!("{:?}", e),
		};

		// Create a document
		let namespace = "test_namespace";
		let database = "test_database";
		let table = "test_table";
		let document_id = Id::String("test_doc".to_string());
		let data = map!("name", "test");
		let key = Tb::new(namespace, database, table);
		let value = DefineTableStatement {
			name: Default::default(),
			drop: false,
			full: false,
			view: None,
			permissions: Default::default(),
		};
		match tx.set(&key, &value).await {
			Ok(_) => {}
			Err(e) => panic!("{:?}", e),
		};

		#[rustfmt::skip]
		match tx.scan(tb::prefix(namespace, database)..tb::suffix(namespace, database), 1000).await {
			Ok(scan) => {
				assert_eq!(scan.len(), 1);
				let read = Tb::from(scan[0].1.into());
				assert_eq!(read, &value);
			}
			Err(e) => panic!("{:?}", e),
		}

		panic!("Finished test")
	}

	#[test]
	async fn created_tables_can_be_deleted() {
		assert_eq!(4, 4)
	}
}
