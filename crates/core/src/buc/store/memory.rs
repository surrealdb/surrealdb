use std::{
	collections::BTreeMap,
	future::Future,
	pin::Pin,
	sync::{Arc, RwLock},
};

use bytes::Bytes;
use url::Url;

use crate::sql::Datetime;

use super::{Key, ListOptions, ObjectMeta, ObjectStore};

#[derive(Clone, Debug, Default)]
pub struct Entry {
	bytes: Bytes,
	updated: Datetime,
}

impl From<Bytes> for Entry {
	fn from(bytes: Bytes) -> Self {
		Self {
			bytes,
			..Default::default()
		}
	}
}

#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
	store: Arc<RwLock<BTreeMap<Key, Entry>>>,
}

impl MemoryStore {
	pub fn new() -> Self {
		MemoryStore::default()
	}

	pub fn parse_url(url: &str) -> bool {
		if url == "memory" {
			return true;
		}

		let Ok(url) = Url::parse(url) else {
			return false;
		};

		url.scheme() == "memory"
	}
}

impl ObjectStore for MemoryStore {
	fn prefix(&self) -> Option<Key> {
		None
	}

	fn put<'a>(
		&'a self,
		key: &'a Key,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			store.insert(key.clone(), data.into());
			Ok(())
		})
	}

	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a Key,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			store.entry(key.clone()).or_insert_with(|| data.into());

			Ok(())
		})
	}

	fn get<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>> {
		Box::pin(async move {
			let store = self.store.read().map_err(|_| "Failed to lock object store".to_string())?;
			let data = store.get(key).map(|v| v.bytes.clone());
			Ok(data)
		})
	}

	fn head<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
		Box::pin(async move {
			let store = self.store.read().map_err(|_| "Failed to lock object store".to_string())?;

			let data = store.get(key).map(|v| ObjectMeta {
				size: v.bytes.len() as u64,
				updated: v.updated.clone(),
				key: key.to_owned(),
			});

			Ok(data)
		})
	}

	fn delete<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			store.remove(key);
			Ok(())
		})
	}

	fn exists<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>> {
		Box::pin(async move {
			let store = self.store.read().map_err(|_| "Failed to lock object store".to_string())?;
			let exists = store.contains_key(key);
			Ok(exists)
		})
	}

	fn copy<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			if let Some(x) = store.get(key) {
				let data = x.clone();
				store.insert(target.clone(), data);
			}

			Ok(())
		})
	}

	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			if !store.contains_key(target) {
				if let Some(x) = store.get(key) {
					let data = x.clone();
					store.insert(target.clone(), data);
				}
			}

			Ok(())
		})
	}

	fn rename<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			if let Some(data) = store.remove(key) {
				store.insert(target.clone(), data);
			}

			Ok(())
		})
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			if !store.contains_key(target) {
				if let Some(data) = store.remove(key) {
					store.insert(target.clone(), data);
				}
			}

			Ok(())
		})
	}

	fn list<'a>(
		&'a self,
		opts: &'a ListOptions,
	) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectMeta>, String>> + Send + 'a>> {
		Box::pin(async move {
			let store = self.store.read().map_err(|_| "Failed to lock object store".to_string())?;
			let mut objects = Vec::new();

			// If prefix is provided, filter keys that start with the prefix
			// Otherwise, include all keys
			let prefix_str = opts.prefix.as_ref().map(|p| p.as_str()).unwrap_or("");

			// Get the start key as string if provided
			let start_str = opts.start.as_ref().map(|s| s.as_str());

			// Collect and sort keys first to ensure consistent ordering
			let mut all_keys: Vec<_> = store
				.iter()
				.filter(|(key, _)| {
					// Filter by prefix
					let key_matches_prefix = key.as_str().starts_with(prefix_str);

					// Filter by start key if provided
					let key_after_start = if let Some(start_s) = start_str {
						key.as_str() > start_s
					} else {
						true
					};

					key_matches_prefix && key_after_start
				})
				.collect();

			// Sort keys lexicographically for consistent ordering
			all_keys.sort_by(|(key_a, _), (key_b, _)| key_a.as_str().cmp(&key_b.as_str()));

			// Apply limit if specified
			let limited_keys = if let Some(limit_val) = opts.limit {
				all_keys.into_iter().take(limit_val).collect::<Vec<_>>()
			} else {
				all_keys
			};

			// Convert to ObjectMeta
			for (key, entry) in limited_keys {
				objects.push(ObjectMeta {
					key: key.clone(),
					size: entry.bytes.len() as u64,
					updated: entry.updated.clone(),
				});
			}

			Ok(objects)
		})
	}
}
