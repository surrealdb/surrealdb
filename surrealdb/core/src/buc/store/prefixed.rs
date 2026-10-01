//! Prefixed object store wrapper.
//!
//! This module provides a wrapper that adds a prefix to all object keys,
//! allowing multiple logical buckets to share a single physical storage backend.

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;

use super::{ListOptions, ObjectKey, ObjectMeta, ObjectStore};

/// A wrapper that adds a prefix to all keys in an underlying [`ObjectStore`].
///
/// This is used to namespace objects within a shared storage backend, typically
/// for global bucket configurations where all buckets share a single backend
/// but need isolated key spaces.
///
/// All operations automatically prepend the prefix to keys and strip it from
/// results, making the prefixing transparent to callers.
///
/// # Example
/// If the prefix is `/ns1/db1/bucket1` and you store data at key `/file.txt`,
/// the actual key in the underlying store will be `/ns1/db1/bucket1/file.txt`.
#[derive(Clone, Debug)]
pub struct PrefixedStore<T: ObjectStore> {
	prefix: ObjectKey,
	store: T,
}

impl<T: ObjectStore> PrefixedStore<T> {
	/// Creates a new prefixed store wrapping the given store with the specified prefix.
	///
	/// # Arguments
	/// * `store` - The underlying object store to wrap
	/// * `prefix` - The prefix to prepend to all keys
	pub fn new(store: T, prefix: ObjectKey) -> Self {
		Self {
			store,
			prefix,
		}
	}
}

impl<T: ObjectStore> ObjectStore for PrefixedStore<T> {
	fn put<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		if let Err(e) = key.check_no_traversal() {
			return Box::pin(async move { Err(e) });
		}
		let full_key = self.prefix.join(key);

		Box::pin(async move { self.store.put(&full_key, data).await })
	}

	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		if let Err(e) = key.check_no_traversal() {
			return Box::pin(async move { Err(e) });
		}
		let full_key = self.prefix.join(key);

		Box::pin(async move { self.store.put_if_not_exists(&full_key, data).await })
	}

	fn get<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>> {
		if let Err(e) = key.check_no_traversal() {
			return Box::pin(async move { Err(e) });
		}
		let full_key = self.prefix.join(key);

		Box::pin(async move { self.store.get(&full_key).await })
	}

	fn head<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
		if let Err(e) = key.check_no_traversal() {
			return Box::pin(async move { Err(e) });
		}
		let full_key = self.prefix.join(key);

		Box::pin(async move {
			Ok(self.store.head(&full_key).await?.map(|mut meta| {
				meta.key = meta.key.strip_prefix(&self.prefix).unwrap_or(meta.key);
				meta
			}))
		})
	}

	fn delete<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		if let Err(e) = key.check_no_traversal() {
			return Box::pin(async move { Err(e) });
		}
		let full_key = self.prefix.join(key);

		Box::pin(async move { self.store.delete(&full_key).await })
	}

	fn exists<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>> {
		if let Err(e) = key.check_no_traversal() {
			return Box::pin(async move { Err(e) });
		}
		let full_key = self.prefix.join(key);

		Box::pin(async move { self.store.exists(&full_key).await })
	}

	fn copy<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		if let Err(e) = key.check_no_traversal().and_then(|_| target.check_no_traversal()) {
			return Box::pin(async move { Err(e) });
		}
		let full_key = self.prefix.join(key);
		let full_target = self.prefix.join(target);

		Box::pin(async move { self.store.copy(&full_key, &full_target).await })
	}

	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		if let Err(e) = key.check_no_traversal().and_then(|_| target.check_no_traversal()) {
			return Box::pin(async move { Err(e) });
		}
		let full_key = self.prefix.join(key);
		let full_target = self.prefix.join(target);

		Box::pin(async move { self.store.copy_if_not_exists(&full_key, &full_target).await })
	}

	fn rename<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		if let Err(e) = key.check_no_traversal().and_then(|_| target.check_no_traversal()) {
			return Box::pin(async move { Err(e) });
		}
		let full_key = self.prefix.join(key);
		let full_target = self.prefix.join(target);

		Box::pin(async move { self.store.rename(&full_key, &full_target).await })
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		if let Err(e) = key.check_no_traversal().and_then(|_| target.check_no_traversal()) {
			return Box::pin(async move { Err(e) });
		}
		let full_key = self.prefix.join(key);
		let full_target = self.prefix.join(target);

		Box::pin(async move { self.store.rename_if_not_exists(&full_key, &full_target).await })
	}

	fn list<'a>(
		&'a self,
		opts: &'a ListOptions,
	) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectMeta>, String>> + Send + 'a>> {
		if let Some(ref req_prefix) = opts.prefix
			&& let Err(e) = req_prefix.check_no_traversal()
		{
			return Box::pin(async move { Err(e) });
		}
		Box::pin(async move {
			// Combine the store's prefix with the request prefix
			let prefix = match opts.prefix {
				Some(ref req_prefix) => self.prefix.join(req_prefix),
				// `join` lands the request prefix on a segment boundary, but the
				// bucket prefix on its own does not end in one. Backends that
				// match prefixes as raw strings would then also return objects
				// owned by a sibling bucket whose name extends this one's
				// (`/ns/db/data` matching `/ns/db/data_private/...`), so
				// terminate the prefix explicitly.
				None => self.prefix.join(&ObjectKey::new("/")),
			};

			let opts = ListOptions {
				start: opts.start.clone(),
				prefix: Some(prefix),
				limit: opts.limit,
			};

			// Delegate to the underlying store with the combined prefix
			let objects_result = self.store.list(&opts).await?;

			// Map the returned objects to strip the prefix
			let mapped_objects = objects_result
				.into_iter()
				.map(|mut meta| {
					// Strip the prefix from keys to maintain proper namespacing
					meta.key = meta.key.strip_prefix(&self.prefix).unwrap_or(meta.key);
					meta
				})
				.collect();

			Ok(mapped_objects)
		})
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::buc::store::memory::MemoryStore;

	/// Regression: two global buckets share one backend, so listing bucket
	/// `data` must not report objects belonging to `data_private`, whose key
	/// prefix extends `data`'s. The prefix has to land on a segment boundary
	/// for backends that match prefixes as raw strings.
	#[tokio::test]
	async fn list_excludes_sibling_bucket_whose_prefix_extends_this_one() {
		let backend = Arc::new(MemoryStore::new()) as Arc<dyn ObjectStore>;

		let private = PrefixedStore::new(Arc::clone(&backend), ObjectKey::new("/1/1/data_private"));
		private.put(&ObjectKey::new("/secret.txt"), Bytes::from_static(b"s")).await.unwrap();

		let data = PrefixedStore::new(Arc::clone(&backend), ObjectKey::new("/1/1/data"));
		data.put(&ObjectKey::new("/own.txt"), Bytes::from_static(b"o")).await.unwrap();

		let listed = data.list(&ListOptions::default()).await.unwrap();
		let keys: Vec<String> = listed.into_iter().map(|meta| meta.key.to_string()).collect();
		assert_eq!(keys, vec!["/own.txt".to_string()]);
	}

	/// A request prefix still matches as a raw string within the bucket, so
	/// `prefix: "/re"` keeps selecting `/report.txt`.
	#[tokio::test]
	async fn list_with_request_prefix_still_matches_partial_keys() {
		let backend = Arc::new(MemoryStore::new()) as Arc<dyn ObjectStore>;
		let data = PrefixedStore::new(backend, ObjectKey::new("/1/1/data"));

		data.put(&ObjectKey::new("/report.txt"), Bytes::from_static(b"r")).await.unwrap();
		data.put(&ObjectKey::new("/other.txt"), Bytes::from_static(b"o")).await.unwrap();

		let opts = ListOptions {
			prefix: Some(ObjectKey::new("/re")),
			..ListOptions::default()
		};
		let listed = data.list(&opts).await.unwrap();
		let keys: Vec<String> = listed.into_iter().map(|meta| meta.key.to_string()).collect();
		assert_eq!(keys, vec!["/report.txt".to_string()]);
	}
}
