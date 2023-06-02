#[cfg(all(test, feature = "kv-mem"))]
pub(crate) mod helper {
	use crate::dbs::cl::Timestamp;
	use crate::err::Error;
	use crate::kvs::{Datastore, Transaction};
	use tracing::Level;
	use tracing_subscriber;

	pub struct TestContext {
		pub(crate) db: Datastore,
	}

	/// TestContext is a container for an initialised test context
	/// Anything stateful (such as storage layer and logging) can be tied with this
	impl TestContext {
		pub(crate) async fn bootstrap_at_time(
			&self,
			node_id: &uuid::Uuid,
			time: Timestamp,
		) -> Result<(), Error> {
			let mut tx = self.db.transaction(true, true).await?;
			let archived = self.db.register_remove_and_archive(&mut tx, node_id, time).await?;
			tx.commit().await?;
			let mut tx = self.db.transaction(true, true).await?;
			self.db.remove_archived(&mut tx, archived).await?;
			Ok(tx.commit().await?)
		}
	}

	/// Initialise logging and prepare a useable datastore
	/// In the future it would be nice to handle multiple datastores
	pub(crate) async fn init() -> Result<TestContext, Error> {
		// Set tracing for tests for debug, but only do it once
		let _subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).init();

		let db = Datastore::new("memory").await?;
		return Ok(TestContext {
			db,
		});
	}

	/// Scan the entire storage layer displaying keys
	/// Useful to debug scans ;)
	async fn _debug_scan(tx: &mut Transaction, message: &str) {
		let r = tx.scan(vec![0]..vec![u8::MAX], 100000).await.unwrap();
		println!("START OF RANGE SCAN - {}", message);
		for (k, _v) in r.iter() {
			println!("{}", crate::key::debug::sprint_key(k.as_ref()));
		}
		println!("END OF RANGE SCAN - {}", message);
	}
}
