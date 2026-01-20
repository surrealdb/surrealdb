//! Utilities for **remote** safetensor access.
use crate::utils::error::{SurrealError, SurrealErrorStatus};
use hf_hub::api::sync::ApiBuilder;
use std::path::PathBuf;

/// Download all `.safetensors` files for a given model from the Hugging Face Hub.
///
/// # Arguments
/// * `model_id` — Fully-qualified model identifier, e.g. `"bert-base-uncased"`.
/// * `filenames` — The list of tensor filenames to download.
/// * `hf_token` — Optional user access token. Pass `None` for public models or when the token is already
///   set via the `HF_TOKEN` environment variable.
///
/// # Returns
/// * `Ok(Vec<PathBuf>)` containing local paths to each downloaded `.safetensors` file.
/// * `Err(SurrealError)` if the Hub API cannot be initialised or any file is missing in the repository.
pub fn fetch_safetensors(
    model_id: &str,
    filenames: &Vec<String>,
    hf_token: Option<&str>,
) -> Result<Vec<PathBuf>, SurrealError> {
    let token: Option<String> = match hf_token {
        Some(tok) if !tok.is_empty() => Some(tok.to_string()),
        _ => None,
    };

    // TODO: hoist the builder into a lazy_static and reuse between calls.
    // TODO: allow setting a custom cache path.
    let api = ApiBuilder::new()
        .with_token(token)
        .with_progress(true)
        .build()
        .map_err(|_| {
            SurrealError::new(
                "Failed to initialise Hugging Face API".to_string(),
                SurrealErrorStatus::NotFound,
            )
        })?;

    let repo = api.model(model_id.to_string());
    let mut paths = Vec::new();

    for filename in filenames {
        let path = repo.get(&filename).map_err(|_| {
            SurrealError::new(
                format!("{} not found in repository", filename),
                SurrealErrorStatus::NotFound,
            )
        })?;
        paths.push(path);
    }

    Ok(paths)
}
