use core_dockpack::{
    docker_commands,
    cache,
    unpacking
};
use std::path::PathBuf;


/// Unpacks the files from a Docker image into a directory.
/// 
/// # Arguments
/// * `image` - The name of the Docker image to unpack.
/// * `directory` - The directory to unpack the Docker image into.
/// 
/// # Returns
/// The path to the directory where the Docker image files are stored.
pub fn unpack_files_from_image(image: &str, directory: &str) -> Result<String, String> {
    let image_file = cache::process_image_name(&image.to_string());

    let main_path = PathBuf::from(directory);

    cache::wipe_and_create_cache(&main_path);

    let tar_dir = main_path.join("tar");
    let tar_dir = tar_dir.to_str().unwrap();
    let main_tar_path = docker_commands::save_docker_image(
        image,
        tar_dir,
    )?;
    let unpack_path  = main_path.join(image_file);
    let final_path = unpacking::extract_layers(
        main_tar_path.as_str(),
        // unwrap is safe here because we are using a hardcoded path
        unpack_path.to_str().unwrap(),
    )?;
    Ok(final_path)
}



fn main() {
	if cfg!(target_arch = "wasm32") {
		println!("cargo:rustc-cfg=wasm");
		println!("cargo::rustc-check-cfg=cfg(wasm)");
	}
	if cfg!(any(
		feature = "kv-mem",
		feature = "kv-fdb",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-surrealkv",
		feature = "kv-surrealcs",
	)) {
		println!("cargo:rustc-cfg=storage");
		println!("cargo::rustc-check-cfg=cfg(storage)");

		if cfg!(feature = "kv-surrealcs") {
			let surrealcs_path = "../surrealcs/";

			// check to see if the directory exists
			if !std::path::Path::new(surrealcs_path).exists() {
				unpack_files_from_image(
					"surrealdb/surrealcs-client:latest", 
					surrealcs_path
				).unwrap();
			}
		}
	}
}
