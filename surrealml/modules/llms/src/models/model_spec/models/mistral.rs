//! Utilities for working with **preset Mistral model configurations**.

use crate::models::model_spec::model_spec_trait::ModelSpec;
use crate::utils::error::{SurrealError, SurrealErrorStatus};
use candle_transformers::models::mimi::candle_nn::VarBuilder;
use candle_transformers::models::mistral::Config as MistralConfig;
use candle_transformers::models::mistral::Model as MistralModel;
use surrealml_tokenizers::Tokenizer;

/// All the Mistral checkpoints we support, by name.
///
/// * `V7bV0_1` — Mistral-7B-v0.1  
///   <https://huggingface.co/mistralai/Mistral-7B-v0.1/blob/main/config.json>
///
/// * `AmazonLite` — amazon/MistralLite  
///   <https://huggingface.co/amazon/MistralLite/blob/main/config.json>
pub enum Mistral {
    V7bV0_1,
    AmazonLite,
}

impl ModelSpec for Mistral {
    type Cfg = MistralConfig;
    type LoadedModel = MistralModel;

    /// Return the `MistralConfig` for this Mistral variant.
    ///
    /// The `use_flash_attn` parameter is ignored, as we're not compiling
    /// candle-transformers with CUDA/Flash-Attn support. We may change this
    /// in the future.
    ///
    /// # Returns
    /// * `MistralConfig` matching the chosen preset, with `use_flash_attn = false`.
    fn config(&self) -> Self::Cfg {
        match self {
            Mistral::V7bV0_1 => MistralConfig::config_7b_v0_1(false),
            Mistral::AmazonLite => MistralConfig::config_amazon_mistral_lite(false),
        }
    }

    /// Returns a list of filenames for tensor files corresponding to this model.
    ///
    /// Each filename is formatted either as:
    /// * `"model-<index>-of-<total>.safetensors"` (for `V7bV0_1`)
    /// * `"pytorch_model-<index>-of-<total>.bin"` (for `AmazonLite`)
    ///
    /// - `<index>` runs from `00001` up to `<total>`, zero-padded to 5 digits.
    /// - `<total>` is the total number of tensor files for this variant, zero-padded to 5 digits.
    ///
    /// # Returns
    /// A `Vec<String>` containing either 2 `.safetensors` filenames
    /// (`V7bV0_1`) or 2 `.bin` filenames (`AmazonLite`).
    fn return_tensor_filenames(&self) -> Vec<String> {
        // We batch 2 files together for both variants:
        let tensor_count: u8 = 2;
        let total_str = format!("{:05}", tensor_count);
        let mut filenames = Vec::with_capacity(tensor_count as usize);

        for i in 1..=tensor_count {
            let idx_str = format!("{:05}", i);
            match self {
                // AmazonLite uses .bin with "pytorch_model" prefix
                Mistral::AmazonLite => {
                    filenames.push(format!("pytorch_model-{}-of-{}.bin", idx_str, total_str))
                }
                // V7bV0_1 uses .safetensors
                Mistral::V7bV0_1 => {
                    filenames.push(format!("model-{}-of-{}.safetensors", idx_str, total_str))
                }
            }
        }

        filenames
    }

    /// Returns a loaded model for Mistral. Takes in the VarBuilder object
    /// for the model.
    ///
    /// # Returns
    /// A `LoadedModel` object containing the loaded model.
    fn return_loaded_model(&self, vb: VarBuilder) -> Result<Self::LoadedModel, SurrealError> {
        let config = self.config();
        let model = MistralModel::new(&config, vb).map_err(|e| {
            SurrealError::new(
                format!("Failed to load Mistral model: {}", e),
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
    use candle_nn::Activation;
    use candle_transformers::models::mistral::Config as Upstream;

    /// Enum-based config must equal the canonical upstream `config_7b_v0_1(false)`
    /// and have the expected field values.
    #[test]
    fn matches_upstream_v7b_v0_1() {
        let expected: Upstream = Upstream::config_7b_v0_1(false);
        let ours: Upstream = Mistral::V7bV0_1.config();
        assert_eq!(expected, ours);

        assert_eq!(ours.vocab_size, 32_000);
        assert_eq!(ours.hidden_size, 4096);
        assert_eq!(ours.intermediate_size, 14_336);
        assert_eq!(ours.num_hidden_layers, 32);
        assert_eq!(ours.num_attention_heads, 32);
        assert_eq!(ours.head_dim, None);
        assert_eq!(ours.num_key_value_heads, 8);
        assert_eq!(ours.hidden_act, Activation::Silu);
        assert_eq!(ours.max_position_embeddings, 32_768);
        assert_eq!(ours.rms_norm_eps, 1e-5);
        assert_eq!(ours.rope_theta, 10_000.);
        assert_eq!(ours.sliding_window, Some(4096));
        assert_eq!(ours.use_flash_attn, false);
    }

    /// Enum-based config must equal the canonical upstream `config_amazon_mistral_lite(false)`
    /// and have the expected field values.
    #[test]
    fn matches_upstream_amazon_lite() {
        let expected: Upstream = Upstream::config_amazon_mistral_lite(false);
        let ours: Upstream = Mistral::AmazonLite.config();
        assert_eq!(expected, ours);

        assert_eq!(ours.vocab_size, 32_003);
        assert_eq!(ours.hidden_size, 4096);
        assert_eq!(ours.intermediate_size, 14_336);
        assert_eq!(ours.num_hidden_layers, 32);
        assert_eq!(ours.num_attention_heads, 32);
        assert_eq!(ours.head_dim, None);
        assert_eq!(ours.num_key_value_heads, 8);
        assert_eq!(ours.hidden_act, Activation::Silu);
        assert_eq!(ours.max_position_embeddings, 32_768);
        assert_eq!(ours.rms_norm_eps, 1e-5);
        assert_eq!(ours.rope_theta, 10_000.);
        assert_eq!(ours.sliding_window, Some(4096));
        assert_eq!(ours.use_flash_attn, false);
    }

    /// Returns 2 `.safetensors` filenames for V7bV0_1.
    #[test]
    fn test_v7b_v0_1_filenames() {
        let m = Mistral::V7bV0_1;
        let filenames = m.return_tensor_filenames();
        assert_eq!(filenames.len(), 2);
        assert_eq!(filenames[0], "model-00001-of-00002.safetensors");
        assert_eq!(filenames[1], "model-00002-of-00002.safetensors");
    }

    /// Returns 2 `.bin` filenames for AmazonLite.
    #[test]
    fn test_amazon_lite_filenames() {
        let m = Mistral::AmazonLite;
        let filenames = m.return_tensor_filenames();
        assert_eq!(filenames.len(), 2);
        assert_eq!(filenames[0], "pytorch_model-00001-of-00002.bin");
        assert_eq!(filenames[1], "pytorch_model-00002-of-00002.bin");
    }
}
