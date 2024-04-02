use crate::dbs::node::Timestamp;
use crate::err::Error;
use crate::kvs::clock::{FakeClock, SizedClock};

#[non_exhaustive]
pub struct TestContext {
	pub(crate) db: Datastore,
	pub(crate) kvs: Kvs,
	// A string identifier for this context.
	// It will usually be a uuid or combination of uuid and fixed string identifier.
	// It is useful for separating test setups when environments are shared.
	pub(crate) context_id: String,
}

/// TestContext is a container for an initialised test context
/// Anything stateful (such as storage layer and logging) can be tied with this
impl TestContext {
	// Use this to generate strings that have the test uuid associated with it
	pub fn test_str(&self, prefix: &str) -> String {
		format!("{}-{}", prefix, self.context_id)
	}
}

/// Initialise logging and prepare a useable datastore
/// In the future it would be nice to handle multiple datastores
pub(crate) async fn init(node_id: Uuid, clock: Arc<SizedClock>) -> Result<TestContext, Error> {
	let (db, kvs) = new_ds(node_id, clock).await;
	Ok(TestContext {
		db,
		kvs,
		context_id: node_id.to_string(), // The context does not always have to be a uuid
	})
}

/// Scan the entire storage layer displaying keys
/// Useful to debug scans ;)
async fn _debug_scan(tx: &mut Transaction, message: &str) {
	let r = tx.scan_paged(ScanPage::from(vec![0]..vec![u8::MAX]), u32::MAX).await.unwrap();
	let r = r.values;
	println!("START OF RANGE SCAN - {}", message);
	for (k, _v) in r.iter() {
		println!("{}", crate::key::debug::sprint_key(k));
	}
	println!("END OF RANGE SCAN - {}", message);
}
