//! This module contains the functions for inserting files into the object storage.
use sha1::{Sha1, Digest};
use super::{get_object_storage, get_local_store_path};
use object_store::ObjectStore;
use object_store::path::Path;
use bytes::Bytes;


/// The status of an insert operation.
///
/// # Variants
/// * `Inserted` - The file was inserted into the object storage.
/// * `AlreadyExists` - The file already exists in the object storage.
pub enum InsertStatus {
    Inserted(String),
    AlreadyExists(String),
}


/// Hashes the bytes of a file to a string for the storage of a file.
///
/// # Notes
/// Right now we are just dumping the bytes of the file into a hash function. However, it must be
/// noted that the hasher can be updated so will be implementing a function that enables
/// chunking in the furue
///
/// It must be noted that the SHA1 hash function is not cryptographically secure and should not be
/// used for security purposes. We are just using it here to generate a unique hash for a file.
/// to avoid the same file being stored multiple times.
///
/// # Arguments
/// * `data` - The bytes of the file
///
/// # Returns
/// * `String` - The hash of the file
pub fn hash_file(data: &Vec<u8>) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let result = hasher.finalize();
    hex::encode(result)
}


/// Inserts a file into the local file object storage using the hash as a file path.
/// If the file is already in the object storage, it will not be inserted again but
/// the reference to the file will be returned.
///
/// # Notes
/// We need to support streaming in the future
///
/// The hash is of the entire file so we do not have duplicate files in the object storage.
/// It is advised that you have an indentifiable key in the key value storage that stores
/// the hash of the file and the file name.
///
/// # Arguments
/// * `file_data` - The bytes of the file to be inserted.
///
/// # Returns
/// * The status of the insert operation which contains the hash of the file
pub async fn insert_local_file(file_data: Vec<u8>) -> Result<InsertStatus, String> {
    let hash = hash_file(&file_data);
    let local_file = get_object_storage();
    let local_path = get_local_store_path().map_err(
        |e| format!("Error getting local store path: {}", e)
    )?;
    let file_path = local_path.join(&hash);

    if !file_path.exists() {
        let object_path: Path = hash.clone().try_into().map_err(
            |e| format!("Error converting path to object store path: {}", e)
        )?;
        local_file.put(&object_path, Bytes::from(file_data)).await.map_err(
            |e| format!("Error inserting file into object storage: {}", e)
        )?;
        Ok(InsertStatus::Inserted(hash))
    } else {
        Ok(InsertStatus::AlreadyExists(hash))
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_file() {
        let data = b"hello world";
        let expected_hash = "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed";

        let calculated_hash = hash_file(&data.to_vec());

        assert_eq!(calculated_hash, expected_hash);
    }
}
