use crate::sql::{Datetime, Value};
use bytes::Bytes;
use std::{future::Future, pin::Pin, sync::Arc};

pub mod file;
pub mod memory;
pub mod prefixed;
pub mod util;
pub use util::Path;

pub struct ObjectMeta {
	pub size: u64,
	pub updated: Datetime,
	pub path: Path,
}

impl From<ObjectMeta> for Value {
	fn from(val: ObjectMeta) -> Self {
		Value::from(map! {
			"updated" => Value::from(val.updated),
			"path" => val.path.into(),
			"size" => Value::from(val.size),
		})
	}
}

pub trait ObjectStore: Send + Sync + 'static {
	fn prefix(&self) -> Option<Path>;

	fn put<'a>(
		&'a self,
		path: &'a Path,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn put_if_not_exists<'a>(
		&'a self,
		path: &'a Path,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn get<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>>;

	fn head<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>>;

	fn delete<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn exists<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>>;

	fn copy<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn copy_if_not_exists<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn rename<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn rename_if_not_exists<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

impl ObjectStore for Arc<dyn ObjectStore> {
	fn prefix(&self) -> Option<Path> {
		(**self).prefix()
	}

	fn put<'a>(
		&'a self,
		path: &'a Path,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).put(path, data)
	}

	fn put_if_not_exists<'a>(
		&'a self,
		path: &'a Path,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).put_if_not_exists(path, data)
	}

	fn get<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>> {
		(**self).get(path)
	}

	fn head<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
		(**self).head(path)
	}

	fn delete<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).delete(path)
	}

	fn exists<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>> {
		(**self).exists(path)
	}

	fn copy<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).copy(path, target)
	}

	fn copy_if_not_exists<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).copy_if_not_exists(path, target)
	}

	fn rename<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).rename(path, target)
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		(**self).rename_if_not_exists(path, target)
	}
}
