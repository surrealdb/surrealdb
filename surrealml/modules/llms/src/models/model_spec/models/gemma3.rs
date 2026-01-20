//! Utilities for working with **preset Gemma-3 model configurations**.

use crate::models::model_spec::model_spec_trait::ModelSpec;
use crate::utils::error::{SurrealError, SurrealErrorStatus};
use candle_nn::activation::Activation;
use candle_transformers::models::gemma3::Config as Gemma3Config;
use candle_transformers::models::gemma3::Model as Gemma3Model;
use candle_transformers::models::mimi::candle_nn::VarBuilder;
use surrealml_tokenizers::Tokenizer;

/// Marker type for the **Gemma-3** family.
///
/// Implements [`ModelSpec`] so callers can ask generically for
/// `M::Cfg` where `M = Gemma3`.
pub struct Gemma3;

impl ModelSpec for Gemma3 {
    type Cfg = Gemma3Config;
    type LoadedModel = Gemma3Model;

    /// Return the `Gemma3Config` for the Gemma-3 checkpoint.
    ///
    /// The values below come from the upstream `config.json` distributed
    /// alongside the model on the Hugging Face Hub:
    /// <https://huggingface.co/google/gemma-3-4b-it/blob/main/config.json>
    ///
    /// # Returns
    /// * `Gemma3Config` with the hard-coded hyperparameters for Gemma-3.
    fn config(&self) -> Self::Cfg {
        Gemma3Config {
            attention_bias: false,
            head_dim: 256, // hidden_size / num_attention_heads
            hidden_activation: Activation::Gelu,
            hidden_size: 2_560,
            intermediate_size: 10_240,
            num_attention_heads: 10, // 2 560 / 256
            num_hidden_layers: 34,
            num_key_value_heads: 10, // not in JSON → mirror attention heads
            rms_norm_eps: 1e-6,
            rope_theta: 10_000.0,
            rope_local_base_freq: 1.0, // JSON omits it; keep default
            vocab_size: 256_000,       // top-level JSON field
            final_logit_softcapping: None,
            attn_logit_softcapping: None,
            query_pre_attn_scalar: 1,
            sliding_window: 1_024,
            sliding_window_pattern: 256, // JSON rope_scaling.factor → 8×256 = 2 048 window span
            max_position_embeddings: 8_192, // JSON field
        }
    }

    /// Returns a list of 2 `.safetensors` tensor filenames for Gemma-3-4B-It.
    ///
    /// Filenames follow the pattern `model-<index>-of-00002.safetensors`.
    ///
    /// # Returns
    /// A `Vec<String>` containing 2 filenames, from
    /// `"model-00001-of-00002.safetensors"` through
    /// `"model-00002-of-00002.safetensors"`.
    fn return_tensor_filenames(&self) -> Vec<String> {
        let tensor_count: u8 = 2;
        let total_str = format!("{:05}", tensor_count);
        let mut filenames = Vec::with_capacity(2);
        for i in 1..=tensor_count {
            let idx_str = format!("{:05}", i);
            filenames.push(format!("model-{}-of-{}.safetensors", idx_str, total_str));
        }
        filenames
    }

    /// Returns a loaded model for Gemma3. Takes in the VarBuilder object
    /// for the model. Note we hardcode use_flash_attn to `false` since
    /// we're not yet supporting CUDA.
    ///
    /// # Returns
    /// A `LoadedModel` object containing the loaded model.
    fn return_loaded_model(&self, vb: VarBuilder) -> Result<Self::LoadedModel, SurrealError> {
        let config = self.config();
        let model = Gemma3Model::new(false, &config, vb).map_err(|e| {
            SurrealError::new(
                format!("Failed to load Gemma3 model: {}", e),
                SurrealErrorStatus::Unknown,
            )
        })?;
        Ok(model)
    }

    /// This is a dummy stub that does nothing and always returns an empty string.
    fn run_model(
        &self,
        _model: &mut Self::LoadedModel,
        _input_ids: &[u32],
        _max_steps: usize,
        _tokenizer: &Tokenizer,
    ) -> Result<String, SurrealError> {
        Ok(String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candle_transformers::models::gemma3::Config as Upstream;

    /// Config must equal the manually constructed `Gemma3Config` values.
    #[test]
    fn matches_expected_fields() {
        let ours: Upstream = Gemma3.config();
        // Check individual fields
        assert_eq!(ours.attention_bias, false);
        assert_eq!(ours.head_dim, 256);
        assert_eq!(ours.hidden_size, 2_560);
    }

    #[test]
    fn test_gemma3_4bit_filenames() {
        let m = Gemma3;
        let filenames = m.return_tensor_filenames();
        assert_eq!(filenames.len(), 2);
        assert_eq!(filenames[0], "model-00001-of-00002.safetensors");
        assert_eq!(filenames[1], "model-00002-of-00002.safetensors");
    }
}
