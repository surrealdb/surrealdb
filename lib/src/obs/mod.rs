//! This module defines the operations for object storage using the [object_store](https://docs.rs/object_store/latest/object_store/)
//! crate. This will enable the user to store objects using local file storage, or cloud storage such as S3 or GCS.
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use object_store::local::LocalFileSystem;

pub mod delete;
pub mod get;
pub mod insert;
pub mod update;


/// Creates the localstore directory if it doesn't exist and returns the path.
pub fn get_local_store_path() -> std::io::Result<PathBuf> {
    let cwd = env::current_dir()?;
    let localstore_path: PathBuf = cwd.join("localstore");

    if !localstore_path.exists() {
        fs::create_dir(&localstore_path)?;
    }
    Ok(localstore_path)
}


/// Returns the local file system object storage.
pub fn get_object_storage() -> Arc<LocalFileSystem> {
    let path = get_local_store_path().unwrap();
    let local_file = LocalFileSystem::new_with_prefix(path).unwrap();
    Arc::new(local_file)
}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_get_local_store_path() {
        let localstore_path = get_local_store_path().unwrap();
        println!("localstore_path: {:?}", localstore_path);
    }
}
