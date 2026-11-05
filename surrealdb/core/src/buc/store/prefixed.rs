//! Prefixed object store wrapper.
//!
//! This module provides a wrapper that adds a prefix to all object keys,
//! allowing multiple logical buckets to share a single physical storage backend.

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use futures::stream::BoxStream;

use super::{
	ByteRange, ListEntry, ListOptions, ObjectKey, ObjectMeta, ObjectStore, ObjectStream, PutError,
	PutOptions, PutResult,
};

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

	fn supports_content_type(&self) -> bool {
		self.store.supports_content_type()
	}

	fn put_streamed<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: BoxStream<'static, Result<Bytes, String>>,
		opts: PutOptions,
	) -> Pin<Box<dyn Future<Output = Result<PutResult, PutError>> + Send + 'a>> {
		if let Err(e) = key.check_no_traversal() {
			return Box::pin(async move { Err(PutError::Failed(e)) });
		}
		let full_key = self.prefix.join(key);

		Box::pin(async move { self.store.put_streamed(&full_key, data, opts).await })
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

	fn get_streamed<'a>(
		&'a self,
		key: &'a ObjectKey,
		range: Option<ByteRange>,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectStream>, String>> + Send + 'a>> {
		if let Err(e) = key.check_no_traversal() {
			return Box::pin(async move { Err(e) });
		}
		let full_key = self.prefix.join(key);

		Box::pin(async move {
			// The prefix is stripped back off `meta.key` so the caller sees the
			// key it asked for, as every other method here does.
			let mut res = self.store.get_streamed(&full_key, range).await?;
			if let Some(stream) = res.as_mut() {
				key.clone_into(&mut stream.meta.key);
			}
			Ok(res)
		})
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
	) -> Pin<Box<dyn Future<Output = Result<Vec<ListEntry>, String>> + Send + 'a>> {
		Box::pin(async move {
			// The store's root, without a trailing `/`, so that prepending it
			// to a key (which starts with `/`) never doubles the separator.
			let root = self.prefix.as_str().trim_end_matches('/');

			// Both the prefix and `start` are rebased under the root. The
			// prefix always is, so a listing of this bucket can never reach a
			// sibling whose name merely starts with this one's.
			let opts = ListOptions {
				start: opts.start.as_ref().map(|start| format!("{root}{start}")),
				prefix: Some(format!("{root}{}", opts.prefix())),
				delimiter: opts.delimiter,
				limit: opts.limit,
			};

			let entries = self.store.list(&opts).await?;

			// Strip the root, and drop anything that does not carry it.
			Ok(entries
				.into_iter()
				.filter_map(|entry| match entry {
					ListEntry::Object(mut meta) => {
						meta.key = meta.key.strip_prefix(&self.prefix)?;
						Some(ListEntry::Object(meta))
					}
					ListEntry::Prefix(group) => {
						Some(ListEntry::Prefix(group.strip_prefix(root)?.to_owned()))
					}
				})
				.collect())
		})
	}
}
