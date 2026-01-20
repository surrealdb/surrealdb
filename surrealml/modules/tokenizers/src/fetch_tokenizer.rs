//! Utilities for **remote** tokenizer access.
use crate::error::{SurrealError, SurrealErrorStatus};
use hf_hub::api::sync::ApiBuilder;
use std::path::PathBuf;
use tokenizers::Tokenizer;

/// Download `tokenizer.json` for a given model from the Hugging Face Hub.
///
/// # Arguments
/// * `model_id` — Fully‑qualified model identifier, e.g. `"bert-base-uncased"`.
/// * `hf_token` — Optional user access token.  Pass `None` for public models
///   or when the token is already set via the `HF_TOKEN` environment variable.
///
/// # Returns
/// * `Ok(PathBuf)` containing the local path to *tokenizer.json*.
/// * `Err(SurrealError)` if the Hub API cannot be initialised or the file is
///   missing in the repository.
pub fn fetch_tokenizer(model_id: &str, hf_token: Option<&str>) -> Result<PathBuf, SurrealError> {
    let token: Option<String> = match hf_token {
        Some(token) if !token.is_empty() => Some(token.to_string()),
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
    let tokenizer_path = repo.get("tokenizer.json").map_err(|_| {
        SurrealError::new(
            "tokenizer.json not found in repository".to_string(),
            SurrealErrorStatus::NotFound,
        )
    })?;

    Ok(tokenizer_path)
}

/// Load and deserialize a tokenizer from a local *tokenizer.json* file.
///
/// # Arguments
/// * `path` — Path to *tokenizer.json* on disk, usually obtained via
///   [`fetch_tokenizer`].
///
/// # Returns
/// * `Ok(Tokenizer)` ready to encode and decode strings.
/// * `Err(SurrealError)` if the file cannot be read or parsed.
pub fn load_tokenizer_from_file(path: &PathBuf) -> Result<Tokenizer, SurrealError> {
    Tokenizer::from_file(path).map_err(|e| {
        SurrealError::new(
            format!("Failed to load tokenizer from {:?}: {}", path, e),
            SurrealErrorStatus::BadRequest,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real network integration test guarded by a Cargo feature.
    mod integration {
        use super::*;

        #[test]
        fn download_and_parse_gpt2() {
            // GPT-2 is public so no token needed.
            let path = fetch_tokenizer("gpt2", None).unwrap();
            println!("Downloaded tokenizer to: {:?}", path);
            let tok = load_tokenizer_from_file(&path).unwrap();

            // Check encoding
            let enc = tok.encode("hello", true).unwrap();
            assert!(!enc.get_ids().is_empty());
        }
    }
}
