use super::{Key, ObjectMeta, ObjectStore};
use bytes::Bytes;
use std::{future::Future, pin::Pin};

#[derive(Clone, Debug)]
pub struct PrefixedStore<T: ObjectStore> {
	prefix: Key,
	store: T,
}

impl<T: ObjectStore> PrefixedStore<T> {
	pub fn new(store: T, prefix: Key) -> Self {
		Self {
			store,
			prefix,
		}
	}
}

impl<T: ObjectStore> ObjectStore for PrefixedStore<T> {
	fn prefix(&self) -> Option<Key> {
		let prefix = if let Some(x) = self.store.prefix() {
			self.prefix.join(&x)
		} else {
			self.prefix.clone()
		};
		Some(prefix)
	}

	fn put<'a>(
		&'a self,
		key: &'a Key,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_key = self.prefix.join(key);

		Box::pin(async move { self.store.put(&full_key, data).await })
	}

	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a Key,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_key = self.prefix.join(key);

		Box::pin(async move { self.store.put_if_not_exists(&full_key, data).await })
	}

	fn get<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>> {
		let full_key = self.prefix.join(key);

		Box::pin(async move { self.store.get(&full_key).await })
	}

	fn head<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
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
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_key = self.prefix.join(key);

		Box::pin(async move { self.store.delete(&full_key).await })
	}

	fn exists<'a>(
		&'a self,
		key: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>> {
		let full_key = self.prefix.join(key);

		Box::pin(async move { self.store.exists(&full_key).await })
	}

	fn copy<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_key = self.prefix.join(key);
		let full_target = self.prefix.join(target);

		Box::pin(async move { self.store.copy(&full_key, &full_target).await })
	}

	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_key = self.prefix.join(key);
		let full_target = self.prefix.join(target);

		Box::pin(async move { self.store.copy_if_not_exists(&full_key, &full_target).await })
	}

	fn rename<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_key = self.prefix.join(key);
		let full_target = self.prefix.join(target);

		Box::pin(async move { self.store.rename(&full_key, &full_target).await })
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a Key,
		target: &'a Key,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_key = self.prefix.join(key);
		let full_target = self.prefix.join(target);

		Box::pin(async move { self.store.rename_if_not_exists(&full_key, &full_target).await })
	}
}
