#![cfg(feature = "ml")]

//! This module defines the operations for object storage using the [object_store](https://docs.rs/object_store/latest/object_store/)
//! crate. This will enable the user to store objects using local file storage, memory, or cloud storage such as S3 or GCS.
use crate::err::Error;
#[cfg(not(target_family = "wasm"))]
use object_store::local::LocalFileSystem;
#[cfg(target_family = "wasm")]
use object_store::memory::InMemory;
use object_store::parse_url;
use object_store::path::Path;
use object_store::ObjectStore;
use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::LazyLock;
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

/// Gets the file from the local file system or memory object storage.
pub async fn get(file: &str) -> Result<Vec<u8>, Error> {
	match CACHE.get(&Path::from(file)).await {
		Ok(data) => Ok(data.bytes().await?.to_vec()),
		_ => {
			let data = STORE.get(&Path::from(file)).await?;
			CACHE.put(&Path::from(file), data.bytes().await?.into()).await?;
			Ok(CACHE.get(&Path::from(file)).await?.bytes().await?.to_vec())
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::env;
	#[test]
	fn test_initialize_store_env_var() {
		let url = "file:///tmp/test_store";
		env::set_var("SURREAL_OBJECT_STORE", url);
		let store = initialize_store("SURREAL_OBJECT_STORE", "store");
		// Assert the store is initialized with the correct URL
		assert!(store.to_string().contains("store"));

		env::remove_var("SURREAL_OBJECT_STORE");
		assert!(env::var("SURREAL_OBJECT_STORE").is_err());
		let store = initialize_store("SURREAL_OBJECT_STORE", "store");
		debug!("{store:?}");
		let current_dir = env::current_dir().unwrap();
		assert!(env::current_dir().unwrap().join("store").exists());
		// Remove the dir
		fs::remove_dir_all(current_dir.join("store")).unwrap();
	}
}
