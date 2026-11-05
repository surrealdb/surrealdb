//! In-memory object store implementation.
//!
//! This module provides a simple in-memory implementation of the [`ObjectStore`] trait,
//! useful for testing and development environments where persistence is not required.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use url::Url;

use super::{
	ByteRange, ListEntry, ListOptions, Listing, ObjectKey, ObjectMeta, ObjectStore, ObjectStream,
	PutCondition, PutError, PutOptions, PutResult,
};
use crate::val::Datetime;

/// Monotonic across every `MemoryStore` in the process. Only distinctness
/// matters, not ordering between stores.
static WRITE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Internal storage entry containing data and metadata.
#[derive(Clone, Debug)]
pub struct Entry {
	bytes: Bytes,
	updated: Datetime,
	/// Identifies this write. Unique per write, so it is a *strong* validator:
	/// an unchanged tag proves unchanged bytes. Hashing the value instead would
	/// also catch a rewrite of identical content, which saves nothing here
	/// because the bytes are already resident.
	seq: u64,
	content_type: Option<String>,
}

impl Entry {
	fn etag(&self) -> String {
		format!("\"{:x}\"", self.seq)
	}
}

impl From<Bytes> for Entry {
	fn from(bytes: Bytes) -> Self {
		Self {
			bytes,
			updated: Datetime::now(),
			seq: WRITE_SEQ.fetch_add(1, Ordering::Relaxed),
			content_type: None,
		}
	}
}

/// An in-memory implementation of [`ObjectStore`].
///
/// This store keeps all data in memory using a concurrent hash map ([`DashMap`]).
/// Data is not persisted and will be lost when the store is dropped.
///
/// # URL Format
/// - `memory` or `memory://`
///
/// # Use Cases
/// - Testing and development
/// - Temporary storage that doesn't require persistence
/// - Caching layers
#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
	store: DashMap<ObjectKey, Entry>,
}

impl MemoryStore {
	/// Creates a new empty in-memory store.
	pub fn new() -> Self {
		MemoryStore::default()
	}

	/// Checks if the given URL refers to a memory store.
	///
	/// Returns `true` for URLs like `memory` or `memory://`.
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

	fn put_streamed<'a>(
		&'a self,
		key: &'a ObjectKey,
		mut data: BoxStream<'static, Result<Bytes, String>>,
		opts: PutOptions,
	) -> Pin<Box<dyn Future<Output = Result<PutResult, PutError>> + Send + 'a>> {
		Box::pin(async move {
			// This store is the object's final home, so the whole value is held
			// either way. Accumulating and inserting once still keeps the
			// contract: nothing is written at `key` until the stream completes,
			// so an aborted upload leaves any previous object intact.
			let mut buf = BytesMut::new();
			while let Some(chunk) = data.next().await {
				buf.extend_from_slice(&chunk?);
			}

			let size = buf.len() as u64;
			let entry = Entry {
				content_type: opts.content_type,
				..buf.freeze().into()
			};
			let etag = entry.etag();

			match opts.condition {
				PutCondition::Overwrite => {
					self.store.insert(key.clone(), entry);
				}
				// The occupancy test and the insert are one operation, holding
				// the map's entry lock. A second upload racing this one blocks
				// here and then observes the key taken, rather than both
				// finding it free.
				PutCondition::IfNotExists => match self.store.entry(key.clone()) {
					dashmap::mapref::entry::Entry::Occupied(_) => {
						return Err(PutError::AlreadyExists);
					}
					dashmap::mapref::entry::Entry::Vacant(slot) => {
						slot.insert(entry);
					}
				},
			}

			Ok(PutResult {
				size,
				etag: Some(etag),
			})
		})
	}

	fn supports_content_type(&self) -> bool {
		true
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

	fn get_streamed<'a>(
		&'a self,
		key: &'a ObjectKey,
		range: Option<ByteRange>,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectStream>, String>> + Send + 'a>> {
		Box::pin(async move {
			let Some(entry) = self.store.get(key).map(|v| v.clone()) else {
				return Ok(None);
			};

			let size = entry.bytes.len() as u64;
			let range = match range {
				Some(r) => match r.resolve(size) {
					Some(r) => r,
					None => return Ok(None),
				},
				None => 0..size,
			};

			// `Bytes::slice` is a refcounted view, so the whole object is not
			// copied to serve a range. A single chunk is right here: the bytes
			// are already resident, so splitting them would add wakeups without
			// reducing the memory held.
			let chunk = entry.bytes.slice(range.start as usize..range.end as usize);

			Ok(Some(ObjectStream {
				meta: ObjectMeta {
					size,
					updated: entry.updated.0,
					key: key.to_owned(),
					etag: Some(entry.etag()),
					content_type: entry.content_type,
				},
				range,
				stream: stream::once(async move { Ok(chunk) }).boxed(),
			}))
		})
	}

	fn head<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
		Box::pin(async move {
			let data = self.store.get(key).map(|v| ObjectMeta {
				size: v.bytes.len() as u64,
				updated: v.updated.0,
				key: key.to_owned(),
				etag: Some(v.etag()),
				content_type: v.content_type.clone(),
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
					return Err(format!("Source key does not exist: {}", key.as_str()));
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
			let Some((_, data)) = self.store.remove(key) else {
				return Err(format!("Source key does not exist: {}", key.as_str()));
			};

			self.store.insert(target.clone(), data);

			Ok(())
		})
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			// Check if target already exists
			if self.store.contains_key(target) {
				return Ok(());
			}

			// Check if source exists and remove it
			let Some((_, data)) = self.store.remove(key) else {
				return Err(format!("Source key does not exist: {}", key.as_str()));
			};

			self.store.insert(target.clone(), data);

			Ok(())
		})
	}

	fn list<'a>(
		&'a self,
		opts: &'a ListOptions,
	) -> Pin<Box<dyn Future<Output = Result<Vec<ListEntry>, String>> + Send + 'a>> {
		Box::pin(async move {
			// Narrowed to the prefix here only to sort less; the listing
			// applies the prefix, the delimiter, `start` and `limit`.
			let prefix = opts.prefix();
			let mut matching: Vec<_> =
				self.store.iter().filter(|x| x.key().as_str().starts_with(prefix)).collect();
			matching.sort_by(|a, b| a.key().as_str().cmp(b.key().as_str()));

			let mut listing = Listing::new(opts);
			for x in matching {
				let entry = x.value();
				let more = listing.push_object(ObjectMeta {
					key: x.key().clone(),
					size: entry.bytes.len() as u64,
					updated: entry.updated.0,
					etag: Some(entry.etag()),
					content_type: entry.content_type.clone(),
				});
				if !more {
					break;
				}
			}
			Ok(listing.finish())
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn get_streamed_honours_the_contract() {
		super::super::assert_get_streamed_contract(&MemoryStore::new()).await;
	}

	#[tokio::test]
	async fn put_streamed_honours_the_contract() {
		super::super::assert_put_streamed_contract(&MemoryStore::new()).await;
	}

	#[tokio::test]
	async fn a_range_does_not_copy_the_object() {
		// `Bytes::slice` is a refcounted view. A range read of a large object
		// must not allocate a second copy of it, which is the whole reason this
		// store can serve a range without buffering.
		let store = MemoryStore::new();
		let key = ObjectKey::new("/big.bin");
		let body = Bytes::from(vec![7u8; 1 << 20]);
		store.put(&key, body.clone()).await.unwrap();

		let res =
			store.get_streamed(&key, Some(ByteRange::Bounded(16..32))).await.unwrap().unwrap();
		let chunks: Vec<Bytes> = futures::TryStreamExt::try_collect(res.stream).await.unwrap();

		assert_eq!(chunks.len(), 1, "resident bytes are served as one chunk");
		assert_eq!(chunks[0].len(), 16);
	}
}
