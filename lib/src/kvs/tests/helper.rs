use crate::dbs::node::Timestamp;
use crate::err::Error;

pub struct TestContext {
	pub(crate) db: Datastore,
	// A string identifier for this context.
	// It will usually be a uuid or combination of uuid and fixed string identifier.
	// It is useful for separating test setups when environments are shared.
	pub(crate) context_id: String,
}

/// TestContext is a container for an initialised test context
/// Anything stateful (such as storage layer and logging) can be tied with this
impl TestContext {
	pub(crate) async fn bootstrap_at_time(
		&self,
		node_id: crate::sql::uuid::Uuid,
		time: Timestamp,
	) -> Result<(), Error> {
		let mut tx = self.db.transaction(true, true).await?;
		let archived = self.db.register_remove_and_archive(&mut tx, &node_id, time).await?;
		tx.commit().await?;
		let mut tx = self.db.transaction(true, true).await?;
		self.db.remove_archived(&mut tx, archived).await?;
		Ok(tx.commit().await?)
	}

	// Use this to generate strings that have the test uuid associated with it
	pub fn test_str(&self, prefix: &str) -> String {
		return format!("{}-{}", prefix, self.context_id);
	}
}

/// Initialise logging and prepare a useable datastore
/// In the future it would be nice to handle multiple datastores
pub(crate) async fn init() -> Result<TestContext, Error> {
	let db = new_ds().await;
	return Ok(TestContext {
		db,
		context_id: Uuid::new_v4().to_string(), // The context does not always have to be a uuid
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
