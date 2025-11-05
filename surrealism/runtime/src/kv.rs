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

/// In-memory BTreeMap implementation of KVStore
pub struct BTreeMapStore {
	inner: RwLock<BTreeMap<String, surrealdb_types::Value>>,
}

impl BTreeMapStore {
	/// Create a new empty BTreeMap store
	pub fn new() -> Self {
		Self {
			inner: RwLock::new(BTreeMap::new()),
		}
	}

	/// Create a BTreeMap store with initial capacity
	pub fn with_capacity(_capacity: usize) -> Self {
		// BTreeMap doesn't have with_capacity, but we keep the method for API compatibility
		Self {
			inner: RwLock::new(BTreeMap::new()),
		}
	}

	/// Helper function to check if a key falls within a range
	fn in_range(&self, key: &str, start: &Bound<String>, end: &Bound<String>) -> bool {
		match start {
			Bound::Included(start_key) => {
				if key < start_key.as_str() {
					return false;
				}
			}
			Bound::Excluded(start_key) => {
				if key <= start_key.as_str() {
					return false;
				}
			}
			Bound::Unbounded => {}
		}

		match end {
			Bound::Included(end_key) => {
				if key > end_key.as_str() {
					return false;
				}
			}
			Bound::Excluded(end_key) => {
				if key >= end_key.as_str() {
					return false;
				}
			}
			Bound::Unbounded => {}
		}
		true
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
		let map = self
			.inner
			.read()
			.map_err(|_| anyhow::anyhow!("Failed to get from KV store: Could not acquire lock"))?;
		Ok(map.get(&key).cloned())
	}

	async fn set(&self, key: String, value: surrealdb_types::Value) -> Result<()> {
		let mut map = self
			.inner
			.write()
			.map_err(|_| anyhow::anyhow!("Failed to set in KV store: Could not acquire lock"))?;
		map.insert(key, value);
		Ok(())
	}

	async fn del(&self, key: String) -> Result<()> {
		let mut map = self.inner.write().map_err(|_| {
			anyhow::anyhow!("Failed to delete from KV store: Could not acquire lock")
		})?;
		map.remove(&key);
		Ok(())
	}

	async fn exists(&self, key: String) -> Result<bool> {
		let map = self.inner.read().map_err(|_| {
			anyhow::anyhow!("Failed to check if key exists in KV store: Could not acquire lock")
		})?;
		Ok(map.contains_key(&key))
	}

	async fn del_rng(&self, start: Bound<String>, end: Bound<String>) -> Result<()> {
		let mut map = self.inner.write().map_err(|_| {
			anyhow::anyhow!("Failed to delete range from KV store: Could not acquire lock")
		})?;
		let keys_to_remove: Vec<String> =
			map.keys().filter(|key| self.in_range(key, &start, &end)).cloned().collect();
		for key in keys_to_remove {
			map.remove(&key);
		}
		Ok(())
	}

	async fn get_batch(&self, keys: Vec<String>) -> Result<Vec<Option<surrealdb_types::Value>>> {
		let map = self.inner.read().map_err(|_| {
			anyhow::anyhow!("Failed to get batch from KV store: Could not acquire lock")
		})?;
		let mut results = Vec::with_capacity(keys.len());
		for key in keys {
			results.push(map.get(&key).cloned());
		}
		Ok(results)
	}

	async fn set_batch(&self, entries: Vec<(String, surrealdb_types::Value)>) -> Result<()> {
		let mut map = self.inner.write().map_err(|_| {
			anyhow::anyhow!("Failed to set batch in KV store: Could not acquire lock")
		})?;
		for (key, value) in entries {
			map.insert(key, value);
		}
		Ok(())
	}

	async fn del_batch(&self, keys: Vec<String>) -> Result<()> {
		let mut map = self.inner.write().map_err(|_| {
			anyhow::anyhow!("Failed to delete batch from KV store: Could not acquire lock")
		})?;
		for key in keys {
			map.remove(&key);
		}
		Ok(())
	}

	async fn keys(&self, start: Bound<String>, end: Bound<String>) -> Result<Vec<String>> {
		let map = self.inner.read().map_err(|_| {
			anyhow::anyhow!("Failed to collect keys from KV store: Could not acquire lock")
		})?;
		let keys: Vec<String> =
			map.keys().filter(|key| self.in_range(key, &start, &end)).cloned().collect();
		Ok(keys)
	}

	async fn values(
		&self,
		start: Bound<String>,
		end: Bound<String>,
	) -> Result<Vec<surrealdb_types::Value>> {
		let map = self.inner.read().map_err(|_| {
			anyhow::anyhow!("Failed to collect values from KV store: Could not acquire lock")
		})?;
		let values: Vec<surrealdb_types::Value> = map
			.iter()
			.filter(|(key, _)| self.in_range(key, &start, &end))
			.map(|(_, value)| value.clone())
			.collect();
		Ok(values)
	}

	async fn entries(
		&self,
		start: Bound<String>,
		end: Bound<String>,
	) -> Result<Vec<(String, surrealdb_types::Value)>> {
		let map = self.inner.read().map_err(|_| {
			anyhow::anyhow!("Failed to collect entries from KV store: Could not acquire lock")
		})?;
		let entries: Vec<(String, surrealdb_types::Value)> = map
			.iter()
			.filter(|(key, _)| self.in_range(key, &start, &end))
			.map(|(key, value)| (key.clone(), value.clone()))
			.collect();
		Ok(entries)
	}

	async fn count(&self, start: Bound<String>, end: Bound<String>) -> Result<u64> {
		let map = self.inner.read().map_err(|_| {
			anyhow::anyhow!("Failed to get count from KV store: Could not acquire lock")
		})?;
		let count = map.keys().filter(|key| self.in_range(key, &start, &end)).count();
		Ok(count as u64)
	}
}
