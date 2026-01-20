//! The interface for running a model.
use crate::models::model_wrapper::ModelWrapper;
use crate::utils::error::{SurrealError, SurrealErrorStatus};
use surrealml_tokenizers::{encode, load_local_tokenizer};

/// Runs a model that has been loaded.
///
/// # Notes
/// If the maximum number of tokens is too big for the response then the
/// LLM will repeat itself until the maximum number of tokens is produced.
///
/// # Arguments
/// - `model`: The loaded LLM model to be executed.
/// - `input_string`: The input to be fed intot he LLM model.
/// - `max_steps`: The number of tokens that the LLM can produce
///
/// # Returns
/// The string that the LLM produced
pub fn run_model(
    model: &mut ModelWrapper,
    input_string: String,
    max_steps: usize,
) -> Result<String, SurrealError> {
    // Load the corresponding tokenizer for the model. For now, we assume http-access
    // isn't enabled, so we aren't passing in the `hf_token` parameter.
    let tokenizer = load_local_tokenizer(model.to_string()).map_err(|e| {
        SurrealError::new(
            format!(
                "Failed to load tokenizer for model '{}': {}",
                model.to_string(),
                e
            ),
            SurrealErrorStatus::NotFound,
        )
    })?;

    let input_ids = encode(&tokenizer, &input_string).map_err(|e| {
        SurrealError::new(
            format!("Failed to encode input '{}': {}", input_string, e),
            SurrealErrorStatus::BadRequest,
        )
    })?;

    let model_result = model.run_model(&input_ids, max_steps, &tokenizer)?;
    Ok(model_result)
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::str::FromStr;

    #[cfg(feature = "local-gemma-test")]
    use {
        crate::{
            interface::load_model::load_model,
            models::model_spec::{model_spec_trait::ModelSpec, models::gemma::Gemma},
        },
        candle_core::DType,
        std::path::PathBuf,
        tempfile::tempdir,
    };

    /// Local Gemma → `run_model` should succeed and return some text
    #[cfg(feature = "local-gemma-test")]
    #[test]
    fn run_model_from_local_paths() {
        // Re-use the same cache logic as the load_model test
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

        let mut wrapper = load_model("google/gemma-7b", DType::F16, Some(paths), None)
            .expect("Gemma should load from local cache");

        // Small prompt + very small `max_steps` keeps the test fast
        let prompt = "Hello, Gemma!".to_string();
        let result = run_model(&mut wrapper, prompt, 5).expect("run_model should succeed");

        println!("Model output: {}", result);
        assert!(!result.trim().is_empty(), "Model produced no text");
    }

    /// Calling `run_model` on a wrapper that hasn’t been fully loaded
    /// should return `BadRequest` with an informative message.
    #[test]
    fn run_model_requires_loaded_state() {
        // Create an *un-initialised* wrapper
        let mut wrapper =
            ModelWrapper::from_str("google/gemma-7b").expect("Wrapper creation should succeed");

        let err = run_model(&mut wrapper, "Hi".into(), 1)
            .expect_err("run_model should fail on unloaded wrapper");

        assert_eq!(err.status, SurrealErrorStatus::BadRequest);
        assert!(
            err.message.contains("Model not yet loaded"),
            "unexpected message: {}",
            err.message
        );
    }

    /// Even if the wrapper type is valid, `run_model` currently supports
    /// only Gemma; other variants should error with `BadRequest`.
    #[test]
    fn run_model_fails_on_unsupported_model() {
        // Use Falcon as an example of an unsupported variant.
        let mut wrapper =
            ModelWrapper::from_str("tiiuae/falcon-7b").expect("Wrapper creation should succeed");

        let err = run_model(&mut wrapper, "Hi".into(), 1)
            .expect_err("run_model should fail for unsupported model");

        assert_eq!(err.status, SurrealErrorStatus::BadRequest);
        assert!(
            err.message.contains("not implemented for this model"),
            "unexpected message: {}",
            err.message
        );
    }
}
