use crate::sql::{Datetime, Value};
use bytes::Bytes;
use std::{future::Future, pin::Pin, sync::Arc};

pub mod file;
pub mod memory;
pub mod prefixed;
pub mod util;
pub use util::Key;

pub struct ObjectMeta {
	pub size: u64,
	pub updated: Datetime,
	pub key: Key,
}

impl From<ObjectMeta> for Value {
	fn from(val: ObjectMeta) -> Self {
		Value::from(map! {
			"updated" => Value::from(val.updated),
			"key" => val.key.into(),
			"size" => Value::from(val.size),
		})
	}
}

pub trait ObjectStore: Send + Sync + 'static {
	fn prefix(&self) -> Option<Key>;

	fn put<'a>(
		&'a self,
		key: &'a Key,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a Key,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn get<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>>;

	fn head<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>>;

	fn delete<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn exists<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>>;

	fn copy<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn rename<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

impl ObjectStore for Arc<dyn ObjectStore> {
	fn prefix(&self) -> Option<Key> {
		(**self).prefix()
	}

	fn put<'a>(
		&'a self,
		key: &'a Key,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).put(key, data)
	}

	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a Key,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).put_if_not_exists(key, data)
	}

	fn get<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>> {
		(**self).get(key)
	}

	fn head<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
		(**self).head(key)
	}

	fn delete<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).delete(key)
	}

	fn exists<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>> {
		(**self).exists(key)
	}

	fn copy<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).copy(key, target)
	}

	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).copy_if_not_exists(key, target)
	}

	fn rename<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).rename(key, target)
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).rename_if_not_exists(key, target)
	}
}
