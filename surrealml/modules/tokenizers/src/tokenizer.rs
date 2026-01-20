//! High‑level helpers for **tokenizer** loading & (de‑)coding.
use crate::error::{SurrealError, SurrealErrorStatus};
use crate::preset_tokenizers::PresetTokenizers;
use std::str::FromStr;
use tokenizers::Tokenizer;

#[cfg(feature = "http-access")]
use crate::fetch_tokenizer::{fetch_tokenizer, load_tokenizer_from_file};

/// Load a [`Tokenizer`] by **model name**.
///
/// # Arguments
/// * `model` — Canonical model identifier (e.g. `"gpt2"`).
/// * `hf_token` — Optional Hugging Face access token.
///
/// # Returns
/// * `Ok(Tokenizer)` on success.
/// * `Err(SurrealError)` when either retrieval or deserialization fails.
#[cfg(feature = "http-access")]
pub fn load_tokenizer_with_http(
    model: String,
    hf_token: Option<String>,
) -> Result<Tokenizer, SurrealError> {
    if let Ok(preset) = PresetTokenizers::from_str(&model) {
        return preset.retrieve_tokenizer();
    }
    let tokenizer_path = fetch_tokenizer(&model, hf_token.as_deref())?;
    load_tokenizer_from_file(&tokenizer_path)
}

/// Load a [`Tokenizer`] by **model name**.
///
/// # Arguments
/// * `model` — Canonical model identifier (e.g. `"gpt2"`).
///
/// # Returns
/// * `Ok(Tokenizer)` on success.
/// * `Err(SurrealError)` when either retrieval or deserialization fails.
pub fn load_local_tokenizer(model: String) -> Result<Tokenizer, SurrealError> {
    if let Ok(preset) = PresetTokenizers::from_str(&model) {
        return preset.retrieve_tokenizer();
    }
    Err(SurrealError::new(
        "Tokenizer not found locally, and remote access is disabled. \
            Please enable the 'http-access' feature to fetch tokenizers from \
            Hugging Face."
            .to_string(),
        SurrealErrorStatus::NotFound,
    ))
}

/// Encode `text` into a vector of token‑IDs.
///
/// # Arguments
/// * `tokenizer` — A ready tokenizer instance.
/// * `text` — The input string to encode.
///
/// # Returns
/// * `Ok(Vec<u32>)` containing token IDs.
/// * `Err(SurrealError)` if the encoding process fails.
pub fn encode(tokenizer: &Tokenizer, text: &str) -> Result<Vec<u32>, SurrealError> {
    tokenizer
        .encode(text, false)
        .map_err(|e| {
            SurrealError::new(
                format!("Failed to encode text '{}': {}", text, e),
                SurrealErrorStatus::BadRequest,
            )
        })
        .map(|encoding| encoding.get_ids().to_vec())
}

/// Decode a slice of token‑IDs back to a UTF‑8 `String`.
///
/// # Arguments
/// * `tokenizer` — A ready tokenizer instance.
/// * `ids` — Slice of token IDs to decode.
///
/// # Returns
/// * `Ok(String)` with the decoded text.
/// * `Err(SurrealError)` if decoding fails.
pub fn decode(tokenizer: &Tokenizer, ids: &[u32]) -> Result<String, SurrealError> {
    tokenizer.decode(ids, true).map_err(|e| {
        SurrealError::new(
            format!("Failed to decode ids '{:?}': {}", ids, e),
            SurrealErrorStatus::BadRequest,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preset_tokenizers::PresetTokenizers;

    // Returns a tokenizer we can reuse in multiple test cases.
    fn mixtral_tok() -> tokenizers::Tokenizer {
        PresetTokenizers::Mixtral8x7Bv01
            .retrieve_tokenizer()
            .expect("embedded Mixtral8x7Bv01 tokenizer should load")
    }

    #[test]
    fn load_tokenizer_with_preset_name_succeeds() {
        // "gpt2" is one of the PresetTokenizers; adjust if you add/change names.
        let tokenizer = load_local_tokenizer("mistralai/Mixtral-8x7B-v0.1".to_owned()).unwrap();

        // Quick sanity check - encoding a non-empty string yields at least one id.
        let ids = encode(&tokenizer, "Hello from a preset!").expect("encode failed");
        assert!(
            !ids.is_empty(),
            "expected non-empty token id vector from encode()"
        );
    }

    #[cfg(not(feature = "http-access"))]
    #[test]
    fn load_tokenizer_without_http_access_returns_not_found() {
        let err = super::load_local_tokenizer("not_a_real_model".to_owned()).unwrap_err();
        assert_eq!(err.status, SurrealErrorStatus::NotFound);
    }

    // Success path tests
    #[test]
    fn encode_returns_non_empty_ids() {
        let tok = mixtral_tok();
        let ids = encode(&tok, "hello again").expect("encode failed");
        assert!(
            !ids.is_empty(),
            "expected at least one token id for a non-empty string"
        );
    }

    #[test]
    fn encode_then_decode_round_trip() {
        let tok = mixtral_tok();
        let original = "Hello world!";
        let ids = encode(&tok, original).expect("encode failed");
        let decoded = decode(&tok, &ids).expect("decode failed");

        // GPT-style tokenizers may insert leading spaces or change case.  Trim
        // so small formatting differences don't cause a false negative.
        assert_eq!(original.trim(), decoded.trim());
    }
}
