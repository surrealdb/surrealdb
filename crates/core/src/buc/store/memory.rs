use std::{
	collections::BTreeMap,
	future::Future,
	pin::Pin,
	sync::{Arc, RwLock},
};

use bytes::Bytes;
use url::Url;

use crate::sql::Datetime;

use super::{ObjectMeta, ObjectStore, Path};

#[derive(Clone, Debug, Default)]
pub struct Entry {
	bytes: Bytes,
	updated: Datetime,
}

impl From<Bytes> for Entry {
	fn from(bytes: Bytes) -> Self {
		Self {
			bytes,
			..Default::default()
		}
	}
}

#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
	store: Arc<RwLock<BTreeMap<Path, Entry>>>,
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
	fn prefix(&self) -> Option<Path> {
		None
	}

	fn put<'a>(
		&'a self,
		path: &'a Path,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let path = path.clone();

		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			store.insert(path, data.into());
			Ok(())
		})
	}

	fn put_if_not_exists<'a>(
		&'a self,
		path: &'a Path,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let path = path.clone();

		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			store.entry(path).or_insert_with(|| data.into());

			Ok(())
		})
	}

	fn get<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>> {
		let path = path.clone();

		Box::pin(async move {
			let store = self.store.read().map_err(|_| "Failed to lock object store".to_string())?;
			let data = store.get(&path).map(|v| v.bytes.clone());
			Ok(data)
		})
	}

	fn head<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
		let path = path.clone();

		Box::pin(async move {
			let store = self.store.read().map_err(|_| "Failed to lock object store".to_string())?;

			let data = store.get(&path).map(|v| ObjectMeta {
				size: v.bytes.len() as u64,
				updated: v.updated.clone(),
				path: path.to_owned(),
			});

			Ok(data)
		})
	}

	fn delete<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let path = path.clone();

		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			store.remove(&path);
			Ok(())
		})
	}

	fn exists<'a>(
		&'a self,
		path: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>> {
		let path = path.clone();

		Box::pin(async move {
			let store = self.store.read().map_err(|_| "Failed to lock object store".to_string())?;
			let exists = store.contains_key(&path);
			Ok(exists)
		})
	}

	fn copy<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let path = path.clone();
		let target = target.clone();

		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			if let Some(x) = store.get(&path) {
				let data = x.clone();
				store.insert(target, data);
			}

			Ok(())
		})
	}

	fn copy_if_not_exists<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let path = path.clone();
		let target = target.clone();

		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			if !store.contains_key(&target) {
				if let Some(x) = store.get(&path) {
					let data = x.clone();
					store.insert(target, data);
				}
			}

			Ok(())
		})
	}

	fn rename<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let path = path.clone();
		let target = target.clone();

		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			if let Some(data) = store.remove(&path) {
				store.insert(target, data);
			}

			Ok(())
		})
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		path: &'a Path,
		target: &'a Path,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let path = path.clone();
		let target = target.clone();

		Box::pin(async move {
			let mut store =
				self.store.write().map_err(|_| "Failed to lock object store".to_string())?;

			if !store.contains_key(&target) {
				if let Some(data) = store.remove(&path) {
					store.insert(target, data);
				}
			}

			Ok(())
		})
	}
}
