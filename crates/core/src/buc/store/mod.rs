use crate::{
	err::Error,
	sql::{Datetime, File, Object, Value},
};
use bytes::Bytes;
use std::{future::Future, pin::Pin, sync::Arc};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod file;
pub(crate) mod memory;
pub(crate) mod prefixed;
pub(crate) mod util;
pub(crate) use util::ObjectKey;

pub(crate) struct ObjectMeta {
	pub size: u64,
	pub updated: Datetime,
	pub key: ObjectKey,
}

impl ObjectMeta {
	pub fn to_value(self, bucket: String) -> Value {
		Value::from(map! {
			"updated" => Value::from(self.updated),
			"size" => Value::from(self.size),
			"file" => Value::File(File {
				bucket,
				key: self.key.to_string(),
			})
		})
	}
}

#[derive(Default)]
pub(crate) struct ListOptions {
	pub start: Option<ObjectKey>,
	pub prefix: Option<ObjectKey>,
	pub limit: Option<usize>,
}

impl TryFrom<Object> for ListOptions {
	type Error = Error;
	fn try_from(mut obj: Object) -> Result<Self, Self::Error> {
		let mut opts = ListOptions::default();

		if let Some(start) = obj.remove("start") {
			opts.start = Some(start.coerce_to_string()?.into());
		}

		if let Some(prefix) = obj.remove("prefix") {
			opts.prefix = Some(prefix.coerce_to_string()?.into());
		}

		if let Some(limit) = obj.remove("limit") {
			opts.limit = Some(limit.coerce_to_u64()? as usize);
		}

		Ok(opts)
	}
}

pub(crate) trait ObjectStore: Send + Sync + 'static {
	fn put<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn get<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>>;

	fn head<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>>;

	fn delete<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn exists<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>>;

	fn copy<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn rename<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

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
