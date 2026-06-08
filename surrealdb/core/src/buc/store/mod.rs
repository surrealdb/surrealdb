//! Object store abstractions for bucket storage.
//!
//! This module defines the core traits and types for object storage operations:
//! - [`ObjectStore`] - The main trait that all storage backends implement
//! - [`ObjectKey`] - Normalized path representation for object keys
//! - [`ObjectMeta`] - Metadata about stored objects
//! - [`ListOptions`] - Options for listing objects in a bucket

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::err::Error;
use crate::val::{CoerceError, Datetime, File, Number, Object, Value};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod file;
pub(crate) mod memory;
pub(crate) mod path;
pub(crate) mod prefixed;

// Expose type for external composers
pub use path::ObjectKey;

/// Metadata for a stored object.
///
/// Contains information about an object's size, last modification time, and key.
pub struct ObjectMeta {
	/// Size of the object in bytes
	pub size: u64,
	/// Last modification timestamp
	pub updated: DateTime<Utc>,
	/// The object's key (path)
	pub key: ObjectKey,
}

impl ObjectMeta {
	/// Converts the metadata into a SurrealDB `Value` for query results.
	///
	/// The returned value is an object with `updated`, `size`, and `file` fields.
	pub(crate) fn into_value(self, bucket: String) -> Value {
		Value::from(map! {
			"updated" => Value::from(Datetime(self.updated)),
			"size" => Value::from(self.size),
			"file" => Value::File(File {
				bucket,
				key: self.key.to_string(),
			})
		})
	}
}

/// Options for listing objects in a bucket.
///
/// All fields are optional and can be combined to filter and paginate results.
#[derive(Default)]
pub struct ListOptions {
	/// Start listing after this key (exclusive), used for pagination
	pub start: Option<ObjectKey>,
	/// Only list objects with keys starting with this prefix
	pub prefix: Option<ObjectKey>,
	/// Maximum number of objects to return
	pub limit: Option<usize>,
}

impl TryFrom<Object> for ListOptions {
	type Error = Error;
	fn try_from(mut obj: Object) -> Result<Self, Self::Error> {
		let mut opts = ListOptions::default();

		if let Some(start) = obj.remove("start") {
			opts.start = Some(ObjectKey::new(start.coerce_to::<String>()?));
		}

		if let Some(prefix) = obj.remove("prefix") {
			opts.prefix = Some(ObjectKey::new(prefix.coerce_to::<String>()?));
		}

		if let Some(limit) = obj.remove("limit") {
			let n = limit.coerce_to::<i64>()?;
			opts.limit = Some(usize::try_from(n).map_err(|_| CoerceError::InvalidKind {
				from: Value::Number(Number::Int(n)),
				into: "non-negative int".into(),
			})?);
		}

		Ok(opts)
	}
}

/// Trait for object storage backends.
///
/// This trait defines the core operations that all object storage implementations
/// must provide. Implementations include in-memory storage, local filesystem,
/// and cloud storage backends (S3, GCS, Azure Blob Storage, etc.).
///
/// All methods return boxed futures to allow for async operations and trait object usage.
pub trait ObjectStore: Send + Sync + 'static {
	/// Stores data at the specified key, overwriting any existing data.
	fn put<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Stores data at the specified key only if the key does not already exist.
	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Retrieves data from the specified key.
	///
	/// Returns `Ok(None)` if the key does not exist.
	fn get<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>>;

	/// Retrieves metadata for the specified key without fetching the data.
	///
	/// Returns `Ok(None)` if the key does not exist.
	fn head<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>>;

	/// Deletes the data at the specified key.
	///
	/// This operation is idempotent - deleting a non-existent key is not an error.
	fn delete<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Checks whether data exists at the specified key.
	fn exists<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>>;

	/// Copies data from one key to another, overwriting the target if it exists.
	fn copy<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Copies data from one key to another only if the target does not exist.
	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Moves data from one key to another, overwriting the target if it exists.
	fn rename<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Moves data from one key to another only if the target does not exist.
	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	/// Lists objects matching the specified options.
	///
	/// Results are returned in lexicographical order by key.
	fn list<'a>(
		&'a self,
		prefix: &'a ListOptions,
	) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectMeta>, String>> + Send + 'a>>;
}

impl ObjectStore for Arc<dyn ObjectStore> {
	fn put<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).put(key, data)
	}

	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).put_if_not_exists(key, data)
	}

	fn get<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>> {
		(**self).get(key)
	}

	fn head<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
		(**self).head(key)
	}

	fn delete<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).delete(key)
	}

	fn exists<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>> {
		(**self).exists(key)
	}

	fn copy<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).copy(key, target)
	}

	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).copy_if_not_exists(key, target)
	}

	fn rename<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).rename(key, target)
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).rename_if_not_exists(key, target)
	}

	fn list<'a>(
		&'a self,
		opts: &'a ListOptions,
	) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectMeta>, String>> + Send + 'a>> {
		(**self).list(opts)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use super::{ListOptions, Object, Value};

	fn obj(entries: &[(&'static str, Value)]) -> Object {
		let map: BTreeMap<&str, Value> = entries.iter().cloned().collect();
		Object::from(map)
	}

	#[test]
	fn limit_accepts_zero() {
		let opts = ListOptions::try_from(obj(&[("limit", Value::from(0i64))]))
			.expect("limit=0 should be accepted");
		assert_eq!(opts.limit, Some(0));
	}

	#[test]
	fn limit_accepts_positive() {
		let opts = ListOptions::try_from(obj(&[("limit", Value::from(42i64))]))
			.expect("limit=42 should be accepted");
		assert_eq!(opts.limit, Some(42));
	}

	#[test]
	fn limit_rejects_negative() {
		let err = ListOptions::try_from(obj(&[("limit", Value::from(-1i64))]))
			.err()
			.expect("negative limit should be rejected");
		assert!(err.to_string().contains("non-negative int"), "unexpected error message: {err}");
	}
}
