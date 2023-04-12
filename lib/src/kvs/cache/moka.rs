use crate::kvs::cache::{Cache, Entry};
use crate::kvs::Key;
use async_trait_fn::async_trait;

pub struct MokaCache(pub moka::future::Cache<Key, Entry>);

impl MokaCache {
	pub fn new() -> MokaCache {
		MokaCache(moka::future::Cache::builder().build())
	}
}

#[async_trait]
impl Cache for MokaCache {
	async fn exi(&mut self, key: &Key) -> bool {
		self.0.contains_key(key)
	}

	async fn set(&mut self, key: Key, val: Entry) {
		self.0.insert(key, val).await
	}

	async fn get(&mut self, key: &Key) -> Option<Entry> {
		self.0.get(key)
	}

	async fn del(&mut self, key: &Key) -> Option<Entry> {
		let old = self.0.get(key);
		self.0.invalidate(key).await;
		old
	}
}
