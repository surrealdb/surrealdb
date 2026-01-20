//! Utilities for loading a checkpoint into a `ModelWrapper`, either from
//! local `.safetensors` files or (optionally) by fetching them over HTTP.
use crate::models::model_wrapper::ModelWrapper;
use crate::tensors::fetch_tensors::fetch_safetensors;
use crate::tensors::tensor_utils::load_model_vars;
use crate::utils::error::SurrealError;
use candle_core::DType;
use std::path::PathBuf;
use std::str::FromStr;

/// Load and initialise a model given its Hugging-Face ID or local tensor files.
///
/// Attempts to parse `model_id` into a [`ModelWrapper`] (via
/// [`FromStr`]).  Then:
/// 1. If `paths` is `Some(vec![...])`, loads weights from those files.
/// 2. Otherwise, if the `"http-access"` feature is enabled, fetches them
///    from the Hub using `model_id` and an optional token.
/// 3. If neither applies, returns an error.
///
/// # Arguments
///
/// * `model_id` — e.g. `"google/gemma-7b"`, `"tiiuae/falcon-7b"`.
/// * `dtype` — the numeric type of the model weights (`DType::F32`, `DType::F16`, etc.).
/// * `paths` — local filesystem paths to the `.safetensors` shards (in order),
///   if already downloaded.
/// * `optional_hf_token` — an OAuth token for private repos; ignored if
///   `paths` is `Some`.
///
/// # Returns
///
/// On success, returns a **fully loaded** [`ModelWrapper`].  
/// On failure, returns a [`SurrealError`]:
/// * `BadRequest` if `model_id` is unrecognised.
/// * `NotFound` if any local path is missing.
/// * `Unknown` for other I/O, fetch, or loading errors.
pub fn load_model(
    model_id: &str,
    dtype: DType,
    optional_paths: Option<Vec<PathBuf>>,
    optional_hf_token: Option<&str>,
) -> Result<ModelWrapper, SurrealError> {
    let mut model = ModelWrapper::from_str(model_id)?;

    match optional_paths {
        Some(paths) => {
            let vb = load_model_vars(&paths, dtype)?;
            model.load(vb)?;
        }

        None => {
            let filenames = model.tensor_filenames();
            let paths = fetch_safetensors(model_id, &filenames, optional_hf_token)?;
            let vb = load_model_vars(&paths, dtype)?;
            model.load(vb)?;
        }
    }

    Ok(model)
}

#[cfg(test)]
mod tests {
    //! Tests for `load_model` covering:
    //! 1) local-gemma-test + local paths,
    //! 2) http-access + local-gemma-test,
    //! 3) error cases.

    use super::*;
    use crate::utils::error::SurrealErrorStatus;
    use candle_core::DType;
    use tempfile::tempdir;

    #[cfg(feature = "local-gemma-test")]
    use crate::models::model_spec::model_spec_trait::ModelSpec;
    #[cfg(feature = "local-gemma-test")]
    use crate::models::model_spec::models::gemma::Gemma;
    #[cfg(feature = "local-gemma-test")]
    use std::path::PathBuf;

    /// Local paths + `local-gemma-test` → success
    #[cfg(feature = "local-gemma-test")]
    #[test]
    fn load_model_from_local_paths() {
        // Locate first snapshot under ~/.cache/huggingface/hub/models--google--gemma-7b/snapshots
        let home = std::env::var("HOME").unwrap();
        let base =
            PathBuf::from(home).join(".cache/huggingface/hub/models--google--gemma-7b/snapshots");
        let snapshot = std::fs::read_dir(&base)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();

        let names = Gemma.return_tensor_filenames();
        let paths: Vec<PathBuf> = names.into_iter().map(|f| snapshot.join(f)).collect();
        let wrapper = load_model("google/gemma-7b", DType::F16, Some(paths), None)
            .expect("should load from local cache");
        assert!(matches!(wrapper, ModelWrapper::Gemma(_)));
    }

    /// HTTP fallback + `http-access` + `local-gemma-test` → success
    #[cfg(feature = "http-access")]
    #[test]
    fn load_model_via_http() {
        // Rely on fetch_safetensors to pull the same cached files
        let wrapper = load_model("google/gemma-7b", DType::F16, None, Some("TOKEN"))
            .expect("should fetch & load via HTTP");
        assert!(matches!(wrapper, ModelWrapper::Gemma(_)));
    }

    /// Dummy local paths → NotFound error
    #[test]
    fn load_model_fails_on_missing_local_files() {
        let dir = tempdir().unwrap();
        let fake = vec![dir.path().join("does_not_exist.safetensors")];
        let err = match load_model("google/gemma-7b", DType::F16, Some(fake), None) {
            Err(e) => e,
            Ok(_) => panic!("expected error when files are missing"),
        };
        assert_eq!(err.status, SurrealErrorStatus::NotFound);
    }

    /// Unknown model identifier → BadRequest
    #[test]
    fn load_model_rejects_bad_model_id() {
        let err = match load_model("not/a/model", DType::F32, None, None) {
            Err(e) => e,
            Ok(_) => panic!("expected BadRequest for unknown model id"),
        };
        assert_eq!(err.status, SurrealErrorStatus::BadRequest);
    }
}
