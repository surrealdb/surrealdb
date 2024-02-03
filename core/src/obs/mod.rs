//! This module defines the operations for object storage using the [object_store](https://docs.rs/object_store/latest/object_store/)
//! crate. This will enable the user to store objects using local file storage, memory, or cloud storage such as S3 or GCS.
use crate::err::Error;
use bytes::Bytes;
use futures::stream::BoxStream;
#[cfg(not(target_arch = "wasm32"))]
use object_store::local::LocalFileSystem;
#[cfg(target_arch = "wasm32")]
use object_store::memory::InMemory;
use object_store::parse_url;
use object_store::path::Path;
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use sha1::{Digest, Sha1};
use std::env;
use std::fs;
use std::sync::Arc;
use url::Url;

static STORE: Lazy<Arc<dyn ObjectStore>> =
	Lazy::new(|| match std::env::var("SURREAL_OBJECT_STORE") {
		Ok(url) => {
			let url = Url::parse(&url).expect("Expected a valid url for SURREAL_OBJECT_STORE");
			let (store, _) =
				parse_url(&url).expect("Expected a valid url for SURREAL_OBJECT_STORE");
			Arc::new(store)
		}
		Err(_) => {
			let path = env::current_dir().unwrap().join("store");
			if !path.exists() || !path.is_dir() {
				fs::create_dir_all(&path)
					.expect("Unable to create directory structure for SURREAL_OBJECT_STORE");
			}
			#[cfg(not(target_arch = "wasm32"))]
			{
				// As long as the provided path is correct, the following should never panic
				Arc::new(LocalFileSystem::new_with_prefix(path).unwrap())
			}
			#[cfg(target_arch = "wasm32")]
			{
				Arc::new(InMemory::new())
			}
		}
	});

static CACHE: Lazy<Arc<dyn ObjectStore>> =
	Lazy::new(|| match std::env::var("SURREAL_OBJECT_CACHE") {
		Ok(url) => {
			let url = Url::parse(&url).expect("Expected a valid url for SURREAL_OBJECT_CACHE");
			let (store, _) =
				parse_url(&url).expect("Expected a valid url for SURREAL_OBJECT_CACHE");
			Arc::new(store)
		}
		Err(_) => {
			let path = env::current_dir().unwrap().join("cache");
			if !path.exists() || !path.is_dir() {
				fs::create_dir_all(&path)
					.expect("Unable to create directory structure for SURREAL_OBJECT_CACHE");
			}
			#[cfg(not(target_arch = "wasm32"))]
			{
				// As long as the provided path is correct, the following should never panic
				Arc::new(LocalFileSystem::new_with_prefix(path).unwrap())
			}
			#[cfg(target_arch = "wasm32")]
			{
				Arc::new(InMemory::new())
			}
		}
	});

/// Streams the file from the local system or memory object storage.
pub async fn stream(
	file: String,
) -> Result<BoxStream<'static, Result<Bytes, object_store::Error>>, Error> {
	match CACHE.get(&Path::from(file.as_str())).await {
		Ok(data) => Ok(data.into_stream()),
		_ => Ok(STORE.get(&Path::from(file.as_str())).await?.into_stream()),
	}
}

/// Gets the file from the local file system or memory object storage.
pub async fn get(file: &str) -> Result<Vec<u8>, Error> {
	match CACHE.get(&Path::from(file)).await {
		Ok(data) => Ok(data.bytes().await?.to_vec()),
		_ => {
			let data = STORE.get(&Path::from(file)).await?;
			CACHE.put(&Path::from(file), data.bytes().await?).await?;
			Ok(CACHE.get(&Path::from(file)).await?.bytes().await?.to_vec())
		}
	}
}

/// Puts the file into the local file system or memory object storage.
pub async fn put(file: &str, data: Vec<u8>) -> Result<(), Error> {
	let _ = STORE.put(&Path::from(file), Bytes::from(data)).await?;
	Ok(())
}

/// Deletes the file from the local file system or memory object storage.
pub async fn del(file: &str) -> Result<(), Error> {
	Ok(STORE.delete(&Path::from(file)).await?)
}

/// Hashes the bytes of a file to a string for the storage of a file.
pub fn hash(data: &Vec<u8>) -> String {
	let mut hasher = Sha1::new();
	hasher.update(data);
	let result = hasher.finalize();
	let mut output = hex::encode(result);
	output.truncate(6);
	output
}
