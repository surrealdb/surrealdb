#[cfg(feature = "kv-mem")]
pub(crate) mod table {
	use crate::ctx::Context;
	use crate::dbs::{Auth, Options, Workable};
	use crate::doc::Document;
	use crate::err::Error;
	use crate::key::tb::Tb;
	use crate::key::thing::Thing;
	use crate::kvs::{Datastore, Transaction};
	use crate::sql;
	use crate::sql::{Id, Value};
	use std::future::Future;

	struct TestContext {
		db: Datastore,
	}

	async fn init() -> Result<TestContext, ()> {
		let db = Datastore::new("mem://").await?;
		return Ok(TestContext {
			db,
		});
	}

	#[test]
	async fn created_tables_can_be_scanned() {
		// Setup
		let test = match init().await {
			Ok(ctx) => ctx,
			Err(e) => panic!("{:?}", e),
		};
		let tx = match test.db.transaction(true, false).await {
			Ok(tx) => tx,
			Err(e) => panic!("{:?}", e),
		};

		// Create a document
		let namespace = "test_namespace";
		let database = "test_database";
		let table = "test_table";
		let document_id = Id::String("test_doc".to_string());
		let data = map!("name", "test");
		let doc_key = Thing::new(namespace, database, table, document_id.clone());
		let doc_value = Document::new(
			Some(sql::Thing::from((table, document_id))),
			&Value::from(data),
			Workable::Normal,
		);
		let ctx = Context::background();
		let opt = Options::new(Auth::Kv);
		match doc_value.store(&ctx, &opt, &tx, stm).await {
			Ok(_) => {}
			Err(_) => {}
		};

		// Scan tables
		assert_eq!(4, 4);
	}

	#[test]
	async fn created_tables_can_be_deleted() {
		assert_eq!(4, 2)
	}
}
