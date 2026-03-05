use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::RwLock;

use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait KVStore: Send + Sync {
	async fn get(&self, key: String) -> Result<Option<surrealdb_types::Value>>;
	async fn set(&self, key: String, value: surrealdb_types::Value) -> Result<()>;
	async fn del(&self, key: String) -> Result<()>;
	async fn exists(&self, key: String) -> Result<bool>;

	async fn del_rng(&self, start: Bound<String>, end: Bound<String>) -> Result<()>;

	async fn get_batch(&self, keys: Vec<String>) -> Result<Vec<Option<surrealdb_types::Value>>>;
	async fn set_batch(&self, entries: Vec<(String, surrealdb_types::Value)>) -> Result<()>;
	async fn del_batch(&self, keys: Vec<String>) -> Result<()>;

	async fn keys(&self, start: Bound<String>, end: Bound<String>) -> Result<Vec<String>>;
	async fn values(
		&self,
		start: Bound<String>,
		end: Bound<String>,
	) -> Result<Vec<surrealdb_types::Value>>;
	async fn entries(
		&self,
		start: Bound<String>,
		end: Bound<String>,
	) -> Result<Vec<(String, surrealdb_types::Value)>>;
	async fn count(&self, start: Bound<String>, end: Bound<String>) -> Result<u64>;
}

/// In-memory BTreeMap implementation of KVStore.
///
/// Uses `RwLock` for interior mutability since the `KVStore` trait
/// requires `&self` receivers and `Sync`.
pub struct BTreeMapStore {
	inner: RwLock<BTreeMap<String, surrealdb_types::Value>>,
}

impl BTreeMapStore {
	pub fn new() -> Self {
		Self {
			inner: RwLock::new(BTreeMap::new()),
		}
	}
}

impl Default for BTreeMapStore {
	fn default() -> Self {
		Self::new()
	}
}

#[async_trait]
impl KVStore for BTreeMapStore {
	async fn get(&self, key: String) -> Result<Option<surrealdb_types::Value>> {
		let map = self.inner.read().map_err(|_| anyhow::anyhow!("KV store lock poisoned"))?;
		Ok(map.get(&key).cloned())
	}

	async fn set(&self, key: String, value: surrealdb_types::Value) -> Result<()> {
		let mut map = self.inner.write().map_err(|_| anyhow::anyhow!("KV store lock poisoned"))?;
		map.insert(key, value);
		Ok(())
	}

	async fn del(&self, key: String) -> Result<()> {
		let mut map = self.inner.write().map_err(|_| anyhow::anyhow!("KV store lock poisoned"))?;
		map.remove(&key);
		Ok(())
	}

	async fn exists(&self, key: String) -> Result<bool> {
		let map = self.inner.read().map_err(|_| anyhow::anyhow!("KV store lock poisoned"))?;
		Ok(map.contains_key(&key))
	}

	async fn del_rng(&self, start: Bound<String>, end: Bound<String>) -> Result<()> {
		let mut map = self.inner.write().map_err(|_| anyhow::anyhow!("KV store lock poisoned"))?;
		let keys: Vec<String> = map.range((start, end)).map(|(k, _)| k.clone()).collect();
		for key in keys {
			map.remove(&key);
		}
		Ok(())
	}

	async fn get_batch(&self, keys: Vec<String>) -> Result<Vec<Option<surrealdb_types::Value>>> {
		let map = self.inner.read().map_err(|_| anyhow::anyhow!("KV store lock poisoned"))?;
		Ok(keys.iter().map(|key| map.get(key).cloned()).collect())
	}

	async fn set_batch(&self, entries: Vec<(String, surrealdb_types::Value)>) -> Result<()> {
		let mut map = self.inner.write().map_err(|_| anyhow::anyhow!("KV store lock poisoned"))?;
		for (key, value) in entries {
			map.insert(key, value);
		}
		Ok(())
	}

	async fn del_batch(&self, keys: Vec<String>) -> Result<()> {
		let mut map = self.inner.write().map_err(|_| anyhow::anyhow!("KV store lock poisoned"))?;
		for key in keys {
			map.remove(&key);
		}
		Ok(())
	}

	async fn keys(&self, start: Bound<String>, end: Bound<String>) -> Result<Vec<String>> {
		let map = self.inner.read().map_err(|_| anyhow::anyhow!("KV store lock poisoned"))?;
		Ok(map.range((start, end)).map(|(k, _)| k.clone()).collect())
	}

	async fn values(
		&self,
		start: Bound<String>,
		end: Bound<String>,
	) -> Result<Vec<surrealdb_types::Value>> {
		let map = self.inner.read().map_err(|_| anyhow::anyhow!("KV store lock poisoned"))?;
		Ok(map.range((start, end)).map(|(_, v)| v.clone()).collect())
	}

	async fn entries(
		&self,
		start: Bound<String>,
		end: Bound<String>,
	) -> Result<Vec<(String, surrealdb_types::Value)>> {
		let map = self.inner.read().map_err(|_| anyhow::anyhow!("KV store lock poisoned"))?;
		Ok(map.range((start, end)).map(|(k, v)| (k.clone(), v.clone())).collect())
	}

	async fn count(&self, start: Bound<String>, end: Bound<String>) -> Result<u64> {
		let map = self.inner.read().map_err(|_| anyhow::anyhow!("KV store lock poisoned"))?;
		Ok(map.range((start, end)).count() as u64)
	}
}
