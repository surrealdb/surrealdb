use super::{ObjectMeta, ObjectStore, Path};
use bytes::Bytes;
use std::{future::Future, pin::Pin};

#[derive(Clone, Debug)]
pub struct PrefixedStore<T: ObjectStore> {
	prefix: Path,
	store: T,
}

impl<T: ObjectStore> PrefixedStore<T> {
	pub fn new(store: T, prefix: Path) -> Self {
		Self {
			store,
			prefix,
		}
	}
}

impl<T: ObjectStore> ObjectStore for PrefixedStore<T> {
	fn prefix(&self) -> Option<Path> {
		let prefix = if let Some(x) = self.store.prefix() {
			self.prefix.join(&x)
		} else {
			self.prefix.clone()
		};
		Some(prefix)
	}

	fn put<'a>(
		&'a self,
		path: &'a Path,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_path = self.prefix.join(path);

		Box::pin(async move { self.store.put(&full_path, data).await })
	}

	fn put_if_not_exists<'a>(
		&'a self,
		path: &'a Path,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_path = self.prefix.join(path);

		Box::pin(async move { self.store.put_if_not_exists(&full_path, data).await })
	}

	fn get<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>> {
		let full_path = self.prefix.join(path);

		Box::pin(async move { self.store.get(&full_path).await })
	}

	fn head<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
		let full_path = self.prefix.join(path);

		Box::pin(async move {
			Ok(self.store.head(&full_path).await?.map(|mut meta| {
				meta.path = meta.path.strip_prefix(&self.prefix).unwrap_or(meta.path);
				meta
			}))
		})
	}

	fn delete<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_path = self.prefix.join(path);

		Box::pin(async move { self.store.delete(&full_path).await })
	}

	fn exists<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>> {
		let full_path = self.prefix.join(path);

		Box::pin(async move { self.store.exists(&full_path).await })
	}

	fn copy<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_path = self.prefix.join(path);
		let full_target = self.prefix.join(target);

		Box::pin(async move { self.store.copy(&full_path, &full_target).await })
	}

	fn copy_if_not_exists<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_path = self.prefix.join(path);
		let full_target = self.prefix.join(target);

		Box::pin(async move { self.store.copy_if_not_exists(&full_path, &full_target).await })
	}

	fn rename<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_path = self.prefix.join(path);
		let full_target = self.prefix.join(target);

		Box::pin(async move { self.store.rename(&full_path, &full_target).await })
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let full_path = self.prefix.join(path);
		let full_target = self.prefix.join(target);

		Box::pin(async move { self.store.rename_if_not_exists(&full_path, &full_target).await })
	}
}
