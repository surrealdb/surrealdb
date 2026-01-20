//! Utilities for loading model weights into a `VarBuilder` from `.safetensors` files.
use std::path::PathBuf;

use candle_core::{DType, Device};
use candle_transformers::models::mimi::candle_nn::VarBuilder;

use crate::utils::error::{SurrealError, SurrealErrorStatus};

/// Load model weights from a list of `.safetensors` files into a `VarBuilder`.
/// Note, in the future we may pass the device type as an argument (see note below).
///
/// # Arguments
/// * `paths` — A vector of `PathBuf` pointing to each `.safetensors` (or `.bin`) file on disk.
/// * `dtype` — The data type of the model weights (e.g., `DType::F16`).
///
/// # Returns
/// * `Ok(VarBuilder)` containing all loaded variables, ready for model instantiation.
/// * `Err(SurrealError)` with any errors.
pub fn load_model_vars(paths: &[PathBuf], dtype: DType) -> Result<VarBuilder<'_>, SurrealError> {
	// TO DO - For now we hardcode Device::Cpu, because elsewhere in the config we haven't supported
	// CUDA yet. If we ever support CUDA, we can pass the device into the method.
	let device = Device::Cpu;

	for path in paths {
		if !path.exists() {
			return Err(SurrealError::new(
				format!("Tensor file not found: {:?}", path),
				SurrealErrorStatus::NotFound,
			));
		}
	}

	// This is marked unsafe because `from_mmaped_safetensors` uses memory-mapping under the hood.
	let vb = unsafe {
		VarBuilder::from_mmaped_safetensors(paths, dtype, &device).map_err(|e| {
			SurrealError::new(
				format!("Failed to load weights via VarBuilder: {}", e),
				SurrealErrorStatus::Unknown,
			)
		})?
	};

	Ok(vb)
}

#[cfg(test)]
mod tests {
	use std::fs;

	use candle_core::DType;
	use tempfile::tempdir;
	#[cfg(feature = "local-gemma-test")]
	use {
		crate::{interface::load_model::load_model, models::model_spec::models::gemma::Gemma},
		std::io::Write,
		std::path::PathBuf,
	};

	use super::*;
	use crate::models::model_spec::model_spec_trait::ModelSpec;
	use crate::models::model_spec::models::mistral::Mistral;

	#[test]
	fn test_load_model_vars_missing_file_on_disk() {
		// Use the V7B variant, which expects 2 .safetensors files
		let model_variant = Mistral::V7bV0_1;
		let dir = tempdir().unwrap();

		// Generate the two expected filenames
		let filenames = model_variant.return_tensor_filenames();
		let p1 = dir.path().join(&filenames[0]);
		let p2 = dir.path().join(&filenames[1]);

		// Create only the first file; leave the second missing
		fs::File::create(&p1).unwrap();

		let paths = [p1.clone(), p2.clone()];
		let err = match load_model_vars(&paths, DType::F16) {
			Err(e) => e,
			Ok(_) => panic!("expected an error but got Ok(...)"),
		};

		assert!(
			err.message.contains("Tensor file not found:"),
			"unexpected error message: {}",
			err.message
		);
		assert_eq!(err.status, SurrealErrorStatus::NotFound);
	}

	#[test]
	fn test_load_model_vars_invalid_safetensors_format() {
		// Use the V7B variant again (2 .safetensors files)
		let model_variant = Mistral::V7bV0_1;
		let dir = tempdir().unwrap();

		let filenames = model_variant.return_tensor_filenames();
		let p1 = dir.path().join(&filenames[0]);
		let p2 = dir.path().join(&filenames[1]);

		// Create two empty files (invalid as real safetensors)
		fs::File::create(&p1).unwrap();
		fs::File::create(&p2).unwrap();

		let paths = [p1.clone(), p2.clone()];
		let err = match load_model_vars(&paths, DType::F16) {
			Err(e) => e,
			Ok(_) => panic!("expected an error but got Ok(...)"),
		};

		assert!(
			err.message.contains("Failed to load weights via VarBuilder"),
			"unexpected error message: {}",
			err.message
		);
		assert_eq!(err.status, SurrealErrorStatus::Unknown);
	}

	// This only runs if the `local-gemma-test` feature is enabled.
	// For it to work you must have the Gemma-7B files already cached in Hugging Face’s default
	// location.
	#[cfg(feature = "local-gemma-test")]
	#[test]
	fn test_load_model_vars_success() {
		// Use the Gemma7B variant, which expects 4 `.safetensors` files.
		let model_variant = Gemma;

		// Determine the user’s home directory at runtime.
		let home = std::env::var("HOME").expect("HOME environment variable not set");
		let cache_base = std::path::PathBuf::from(home)
			.join(".cache")
			.join("huggingface")
			.join("hub")
			.join("models--google--gemma-7b")
			.join("snapshots");

		// Pick the first snapshot subdirectory we find.
		let snapshot_dir = std::fs::read_dir(&cache_base)
			.expect("Failed to read Hugging Face cache directory")
			.next()
			.expect("No snapshot directory found under models--google--gemma-7b")
			.expect("Failed to access snapshot directory entry")
			.path();

		// Generate the four expected filenames:
		let filenames = model_variant.return_tensor_filenames();
		let p1 = snapshot_dir.join(&filenames[0]);
		let p2 = snapshot_dir.join(&filenames[1]);
		let p3 = snapshot_dir.join(&filenames[2]);
		let p4 = snapshot_dir.join(&filenames[3]);

		// Ensure all four files exist on disk:
		for p in [&p1, &p2, &p3, &p4] {
			assert!(p.exists(), "Expected tensor file to exist: {:?}", p);
		}

		let paths = [p1, p2, p3, p4];
		// Now attempt to load them. This should succeed since they are real files.
		let result = load_model_vars(&paths, DType::F16);

		assert!(
			result.is_ok(),
			"expected Ok(...) when Gemma-7B files exist in cache, got Err({:?})",
			result.err()
		);
	}
}
