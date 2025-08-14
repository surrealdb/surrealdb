#![cfg(feature = "ml")]

//! This module defines the operations for object storage using the [object_store](https://docs.rs/object_store/latest/object_store/)
//! crate. This will enable the user to store objects using local file storage,
//! memory, or cloud storage such as S3 or GCS.
use std::sync::{Arc, LazyLock};
use std::{env, fs};

use anyhow::Result;
use bytes::Bytes;
use futures::stream::BoxStream;
#[cfg(not(target_family = "wasm"))]
use object_store::local::LocalFileSystem;
#[cfg(target_family = "wasm")]
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, parse_url};
use sha1::{Digest, Sha1};
use url::Url;

fn initialize_store(env_var: &str, default_dir: &str) -> Arc<dyn ObjectStore> {
	match std::env::var(env_var) {
		Ok(url) => {
			let url =
				Url::parse(&url).unwrap_or_else(|_| panic!("Expected a valid url for {}", env_var));
			let (store, _) =
				parse_url(&url).unwrap_or_else(|_| panic!("Expected a valid url for {}", env_var));
			if url.scheme() == "file" {
				let path_buf = url.to_file_path().unwrap();
				let path = path_buf.to_str().unwrap();
				if !path_buf.as_path().exists() {
					fs::create_dir_all(path_buf.as_path())
						.unwrap_or_else(|_| panic!("Failed to create directory {:?}", path));
				}
				Arc::new(
					LocalFileSystem::new_with_prefix(path)
						.expect("Failed to create LocalFileSystem"),
				)
			} else {
				Arc::new(store)
			}
		}
		Err(_) => {
			info!(
				"No {} environment variable found, using default directory {}",
				env_var, default_dir
			);
			let path = env::current_dir().unwrap().join(default_dir);
			if !path.exists() || !path.is_dir() {
				fs::create_dir_all(&path)
					.unwrap_or_else(|_| panic!("Failed to create directory {:?}", path));
			}
			#[cfg(not(target_family = "wasm"))]
			{
				// As long as the provided path is correct, the following should never panic
				Arc::new(LocalFileSystem::new_with_prefix(path).unwrap())
			}
			#[cfg(target_family = "wasm")]
			{
				Arc::new(InMemory::new())
			}
		}
	}
}

static STORE: LazyLock<Arc<dyn ObjectStore>> =
	LazyLock::new(|| initialize_store("SURREAL_OBJECT_STORE", "store"));

static CACHE: LazyLock<Arc<dyn ObjectStore>> =
	LazyLock::new(|| initialize_store("SURREAL_CACHE_STORE", "cache"));

/// Streams the file from the local system or memory object storage.
pub async fn stream(
	file: String,
) -> Result<BoxStream<'static, Result<Bytes, object_store::Error>>> {
	match CACHE.get(&Path::from(file.as_str())).await {
		Ok(data) => Ok(data.into_stream()),
		_ => Ok(STORE.get(&Path::from(file.as_str())).await?.into_stream()),
	}
}

/// Gets the file from the local file system or memory object storage.
pub async fn get(file: &str) -> Result<Vec<u8>> {
	match CACHE.get(&Path::from(file)).await {
		Ok(data) => Ok(data.bytes().await?.to_vec()),
		_ => {
			let data = STORE.get(&Path::from(file)).await?;
			CACHE.put(&Path::from(file), data.bytes().await?.into()).await?;
			Ok(CACHE.get(&Path::from(file)).await?.bytes().await?.to_vec())
		}
	}
}

/// Puts the file into the local file system or memory object storage.
pub async fn put(file: &str, data: Vec<u8>) -> Result<()> {
	let _ = STORE.put(&Path::from(file), Bytes::from(data).into()).await?;
	Ok(())
}

/// Deletes the file from the local file system or memory object storage.
pub async fn del(file: &str) -> Result<()> {
	Ok(STORE.delete(&Path::from(file)).await?)
}

/// Hashes the bytes of a file to a string for the storage of a file.
pub fn hash(data: &[u8]) -> String {
	let mut hasher = Sha1::new();
	hasher.update(data);
	let result = hasher.finalize();
	let mut output = hex::encode(result);
	output.truncate(6);
	output
}

#[cfg(test)]
mod tests {
	use std::env;

	use super::*;
	#[test]
	fn test_initialize_store_env_var() {
		let url = "file:///tmp/test_store";
		unsafe { env::set_var("SURREAL_OBJECT_STORE", url) };
		let store = initialize_store("SURREAL_OBJECT_STORE", "store");
		// Assert the store is initialized with the correct URL
		assert!(store.to_string().contains("store"));

		unsafe { env::remove_var("SURREAL_OBJECT_STORE") };
		assert!(env::var("SURREAL_OBJECT_STORE").is_err());
		let store = initialize_store("SURREAL_OBJECT_STORE", "store");
		debug!("{store:?}");
		let current_dir = env::current_dir().unwrap();
		assert!(env::current_dir().unwrap().join("store").exists());
		// Remove the dir
		fs::remove_dir_all(current_dir.join("store")).unwrap();
	}
}
