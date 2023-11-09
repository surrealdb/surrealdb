//! Defines the get for object storage. Right now the only supported object storage is local file
//! system so we are keeping it to a simple function call but will add more in the future.
use super::get_object_storage;
use object_store::ObjectStore;
use object_store::path::Path;
use object_store::GetResultPayload;
use std::io::Read;


/// Gets the file from the local file system object storage.
/// 
/// # Arguments
/// * `file_hash` - The hash of the file to be retrieved.
/// 
/// # Notes
/// We need to support streaming in the future
/// 
/// # Returns
/// * `Vec<u8>` - The bytes of the file.
pub async fn get_local_file(file_hash: String) -> Result<Vec<u8>, String> {
    let local_file = get_object_storage();
    let object_path: Path = file_hash.try_into().map_err(
        |e| format!("Error converting path to object store path: {}", e) 
    )?;
    let file_data = local_file.get(&object_path).await.map_err(
        |e| format!("Error getting file from local file system: {}", e)
    )?.payload;
    match file_data {
        GetResultPayload::File(mut file, _) => {
            let mut buffer = vec![];
            file.read_to_end(&mut buffer).map_err(
                |e| format!("Error reading file from local file system: {}", e)
            )?;
            return Ok(buffer)
        },
        GetResultPayload::Stream(_) => {
            return Err("Stream not supported yet".to_string())
        
        },
    };
}


#[cfg(test)]
mod tests {

    use super::*;
    use surrealml_core::storage::surml_file::SurMlFile;


    #[test]
    fn test_get_local_file() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let file_hash = "0244f3ab411ecceb2bf1c20aa73758523f34a274".to_string();

        let file = rt.block_on(get_local_file(file_hash)).unwrap();
        let _ = SurMlFile::from_bytes(file).unwrap();
    }
}