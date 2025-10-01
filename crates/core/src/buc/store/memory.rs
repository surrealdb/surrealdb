use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use dashmap::DashMap;
use url::Url;

use super::{ListOptions, ObjectKey, ObjectMeta, ObjectStore};
use crate::val::Datetime;

#[derive(Clone, Debug)]
pub struct Entry {
	bytes: Bytes,
	updated: Datetime,
}

impl From<Bytes> for Entry {
	fn from(bytes: Bytes) -> Self {
		Self {
			bytes,
			updated: Datetime::now(),
		}
	}
}

#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
	store: DashMap<ObjectKey, Entry>,
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
	fn put<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			self.store.insert(key.clone(), data.into());
			Ok(())
		})
	}

	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			self.store.entry(key.clone()).or_insert_with(|| data.into());

			Ok(())
		})
	}

	fn get<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>> {
		Box::pin(async move {
			let data = self.store.get(key).map(|v| v.bytes.clone());
			Ok(data)
		})
	}

	fn head<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
		Box::pin(async move {
			let data = self.store.get(key).map(|v| ObjectMeta {
				size: v.bytes.len() as u64,
				updated: v.updated.clone(),
				key: key.to_owned(),
			});

			Ok(data)
		})
	}

	fn delete<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			self.store.remove(key);
			Ok(())
		})
	}

	fn exists<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>> {
		Box::pin(async move {
			let exists = self.store.contains_key(key);
			Ok(exists)
		})
	}

	fn copy<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			// This is intentionally somewhat verbosely written to ensure the lock is being
			// properly handled.
			let entry = {
				let Some(entry) = self.store.get(key) else {
					return Ok(());
				};
				entry.clone()
			};

			self.store.insert(target.clone(), entry);

			Ok(())
		})
	}

	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			if !self.store.contains_key(target) {
				// This is intentionally somewhat verbosely written to ensure the lock is being
				// properly handled.
				let entry = {
					let Some(entry) = self.store.get(key) else {
						return Ok(());
					};
					entry.clone()
				};

				self.store.insert(target.clone(), entry);
			}

			Ok(())
		})
	}

	fn rename<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			if let Some((_, data)) = self.store.remove(key) {
				self.store.insert(target.clone(), data);
			}

			Ok(())
		})
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			if !self.store.contains_key(target) {
				if let Some((_, data)) = self.store.remove(key) {
					self.store.insert(target.clone(), data);
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
			let mut objects = Vec::new();

			// If prefix is provided, filter keys that start with the prefix
			// Otherwise, include all keys
			let prefix_str = opts.prefix.as_ref().map(|p| p.as_str()).unwrap_or("");

			// Get the start key as string if provided
			let start_str = opts.start.as_ref().map(|s| s.as_str());

			// Collect and sort keys first to ensure consistent ordering
			let mut all_keys: Vec<_> = self
				.store
				.iter()
				.filter(|x| {
					let key = x.key();
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
			all_keys.sort_by(|a, b| a.key().as_str().cmp(b.key().as_str()));

			// Apply limit if specified
			let limited_keys = if let Some(limit_val) = opts.limit {
				all_keys.into_iter().take(limit_val).collect::<Vec<_>>()
			} else {
				all_keys
			};

			// Convert to ObjectMeta
			for x in limited_keys {
				let entry = x.value();
				objects.push(ObjectMeta {
					key: x.key().clone(),
					size: entry.bytes.len() as u64,
					updated: entry.updated.clone(),
				});
			}

			Ok(objects)
		})
	}
}
