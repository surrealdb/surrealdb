//! Utilities for working with **preset Falcon model configurations**.

use crate::models::model_spec::model_spec_trait::ModelSpec;
use crate::utils::error::{SurrealError, SurrealErrorStatus};
use candle_transformers::models::falcon::Config as FalconConfig;
use candle_transformers::models::falcon::Falcon as FalconModel;
use candle_transformers::models::mimi::candle_nn::VarBuilder;
use surrealml_tokenizers::Tokenizer;

/// All the Falcon checkpoints we support, by name.
///
/// * `Falcon7B` — Falcon-7B  
///   <https://huggingface.co/tiiuae/falcon-7b/blob/main/config.json>
pub enum Falcon {
    Falcon7B,
}

impl ModelSpec for Falcon {
    type Cfg = FalconConfig;
    type LoadedModel = FalconModel;

    /// Return the `FalconConfig` for this Falcon variant.
    ///
    /// # Returns
    /// * `FalconConfig` matching the chosen preset.
    fn config(&self) -> Self::Cfg {
        match self {
            Falcon::Falcon7B => FalconConfig::falcon7b(),
        }
    }

    /// Returns a list of 2 `.safetensors` tensor filenames for Falcon-7B.
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

    /// Returns a loaded model for Falcon-7B. Takes in the VarBuilder object
    /// for the model.
    ///
    /// # Returns
    /// A `LoadedModel` object containing the loaded model.
    fn return_loaded_model(&self, vb: VarBuilder) -> Result<Self::LoadedModel, SurrealError> {
        let config = self.config();
        let model = FalconModel::load(vb, config).map_err(|e| {
            SurrealError::new(
                format!("Failed to load Falcon model: {}", e),
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
    use candle_transformers::models::falcon::Config as Upstream;

    #[test]
    fn matches_upstream_falcon7b_fields() {
        let ours: Upstream = Falcon::Falcon7B.config();
        // If these lines compile, `Upstream`'s fields are public.
        assert_eq!(ours.vocab_size, 65024);
        assert_eq!(ours.hidden_size, 4544);
        assert_eq!(ours.num_hidden_layers, 32);
        assert_eq!(ours.num_attention_heads, 71);
        assert_eq!(ours.layer_norm_epsilon, 1e-5);
        assert_eq!(ours.initializer_range, 0.02);
        assert_eq!(ours.use_cache, true);
        assert_eq!(ours.bos_token_id, 11);
        assert_eq!(ours.eos_token_id, 11);
        assert_eq!(ours.hidden_dropout, 0.0);
        assert_eq!(ours.attention_dropout, 0.0);
        assert_eq!(ours.n_head_kv, None);
        assert_eq!(ours.alibi, false);
        assert_eq!(ours.new_decoder_architecture, false);
        assert_eq!(ours.multi_query, true);
        assert_eq!(ours.parallel_attn, true);
        assert_eq!(ours.bias, false);
    }

    #[test]
    fn test_falcon7b_filenames() {
        let m = Falcon::Falcon7B;
        let filenames = m.return_tensor_filenames();
        assert_eq!(filenames.len(), 2);
        assert_eq!(filenames[0], "model-00001-of-00002.safetensors");
        assert_eq!(filenames[1], "model-00002-of-00002.safetensors");
    }
}
