#[cfg(any(feature = "kv-tikv", feature = "kv-rocksdb", feature = "kv-fdb"))]
pub(crate) mod transaction {
	use crate::dbs::{Response, Session};
	use crate::kvs::ds::Inner;
	use crate::kvs::Datastore;
	use crate::sql::json;
	use log::debug;
	use std::sync::{Arc, Barrier};
	use std::time::SystemTime;
	use ulid::Ulid;

	// The first transaction increments value by 1.
	// This transaction uses sleep to be sure it runs longer than transaction2.
	async fn transaction_isolation_1(client: TestClient, barrier: Arc<Barrier>) {
		debug!("1 barrier");
		barrier.wait();
		debug!("1 execute");
		client
			.execute(
				r#"
			BEGIN;
				/* 00:00 read the initial value */
				CREATE rec:1 SET value=(SELECT value FROM rec:0);
				SELECT * FROM sleep("2s");
				/* 00:02 before txn2's commit */
				CREATE rec:2 SET value=(SELECT value FROM rec:0);
				SELECT * FROM sleep("2s");
				/* 00:04 after tnx2's commit; */
				CREATE rec:3 SET value=(SELECT value FROM rec:0);
			COMMIT;"#,
			)
			.await;
		debug!("1 ends");
	}

	// The second transaction increments value by 2.
	async fn transaction_isolation_2(client: TestClient, barrier: Arc<Barrier>) {
		debug!("2 barrier");
		barrier.wait();
		debug!("2 execute");
		client
			.execute(
				r#"
			BEGIN;
				SLEEP 1s;
				/* 00:01 before txn1 check the value */
				UPDATE rec:0 SET value=1;
				SLEEP 2s;
				/* 00:03 before txn1 check the value the second time */
			COMMIT;"#,
			)
			.await;
		debug!("2 ends");
	}

	struct TestClient {
		ds_path: String,
		ds: Datastore,
		ses: Session,
	}

	impl TestClient {
		async fn new(db: String, ds_path: String) -> Self {
			let ds = Datastore::new(&ds_path).await.unwrap();
			let ses = Session::for_kv().with_ns("test").with_db(&db);
			Self {
				ds_path,
				ds,
				ses,
			}
		}

		async fn execute(&self, txt: &str) -> Vec<Response> {
			self.ds.execute(txt, &self.ses, None, false).await.unwrap()
		}

		async fn clone(&self) -> Self {
			let ds = match &self.ds.inner {
				#[cfg(feature = "kv-rocksdb")]
				Inner::RocksDB(ds) => Datastore {
					inner: Inner::RocksDB(ds.clone()),
				},
				#[cfg(feature = "kv-tikv")]
				Inner::TiKV(_) => Datastore::new(&self.ds_path).await.unwrap(),
				#[cfg(feature = "kv-fdb")]
				Inner::FDB(_) => Datastore::new(&self.ds_path).await.unwrap(),
				_ => panic!("Datastore not supported"),
			};
			Self {
				ds_path: self.ds_path.clone(),
				ds,
				ses: self.ses.clone(),
			}
		}
	}

	fn assert_eq_value(mut res: Vec<Response>, expected: &str) {
		let value = json(expected).unwrap();
		assert_eq!(res.remove(0).result.unwrap(), value)
	}

	/// This test checks if the repeatable read isolation level is being properly enforced
	// https://github.com/surrealdb/surrealdb/issues/1620
	pub(crate) async fn verify_transaction_isolation(ds_path: &str) {
		let db = Ulid::new().to_string();
		let client = TestClient::new(db, ds_path.to_string()).await;

		// Create a document with initial values.
		client.execute("CREATE rec:0 SET value=0").await;

		// The barrier is used to synchronise both transactions.
		let barrier = Arc::new(Barrier::new(3));

		// The two queries are run in parallel.
		let f1 = tokio::spawn(transaction_isolation_1(client.clone().await, barrier.clone()));
		let f2 = tokio::spawn(transaction_isolation_2(client.clone().await, barrier.clone()));

		// Unlock the execution of both transactions.
		let time = SystemTime::now();
		barrier.wait();

		// Wait for both transaction's execution.
		let (res1, res2) = tokio::join!(f1, f2);

		if time.elapsed().unwrap().as_secs() > 6 {
			panic!(
				"The test should not take more than 6 seconds.\
		It probably means that the two transactions has not been run in parallel."
			)
		}
		// Check that both transaction ran successfully.
		res1.unwrap();
		res2.unwrap();

		// `rec:0.value` should be 1, set by txn2.
		assert_eq_value(client.execute("SELECT value FROM rec:0").await, r#"[{"value": 1}]"#);

		// `rec:1.value should be 0, the initial value of rec:0.value
		assert_eq_value(
			client.execute("SELECT value FROM rec:1").await,
			r#"[{"value": {"value": 0}}]"#,
		);

		// `rec:2.value should be 0, the initial value of rec:0.value
		assert_eq_value(
			client.execute("SELECT value FROM rec:2").await,
			r#"[{"value": {"value": 0}}]"#,
		);

		// `rec:3.value should be 0, the initial value of rec:0.value
		assert_eq_value(
			client.execute("SELECT value FROM rec:3").await,
			r#"[{"value": {"value": 0}}]"#,
		);
	}
}
