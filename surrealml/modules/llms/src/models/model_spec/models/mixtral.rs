//! Utilities for working with **preset Mixtral model configurations**.

use crate::models::model_spec::model_spec_trait::ModelSpec;
use crate::utils::error::{SurrealError, SurrealErrorStatus};
use candle_transformers::models::mimi::candle_nn::VarBuilder;
use candle_transformers::models::mixtral::Config as MixtralConfig;
use candle_transformers::models::mixtral::Model as MixtralModel;
use surrealml_tokenizers::Tokenizer;

/// All the Mixtral checkpoints we support, by name.
///
/// * `V0_1_8x7b` — Mixtral-8×7B-v0.1
pub enum Mixtral {
    V0_1_8x7b,
}

impl ModelSpec for Mixtral {
    type Cfg = MixtralConfig;
    type LoadedModel = MixtralModel;

    /// Return the `MixtralConfig` for this Mixtral variant.
    ///
    /// The values come straight from the upstream `config.json` distributed
    /// alongside the checkpoint  
    /// <https://huggingface.co/mistralai/Mixtral-8x7B-v0.1/blob/main/config.json>.
    ///
    /// The `use_flash_attn` parameter is ignored, as we're not compiling the
    /// candle-transformers crate with the cuda or flash-attention support. We may
    /// change this in the future.
    ///
    /// # Returns
    /// * `MixtralConfig` for the requested Mixtral model (always v0.1 8×7B).
    fn config(&self) -> Self::Cfg {
        match self {
            Mixtral::V0_1_8x7b => MixtralConfig::v0_1_8x7b(false),
        }
    }

    /// Returns a list of 19 `.safetensors` tensor filenames for Mixtral-8×7B-v0.1.
    ///
    /// # Returns
    /// A `Vec<String>` containing 19 filenames, from
    fn return_tensor_filenames(&self) -> Vec<String> {
        let tensor_count: u8 = 19;
        let total_str = format!("{:05}", tensor_count);
        let mut filenames = Vec::with_capacity(tensor_count as usize);
        for i in 1..=tensor_count {
            let idx_str = format!("{:05}", i);
            filenames.push(format!("model-{}-of-{}.safetensors", idx_str, total_str));
        }
        filenames
    }

    /// Returns a loaded model for Mixtral. Takes in the VarBuilder object
    /// for the model.
    ///
    /// # Returns
    /// A `LoadedModel` object containing the loaded model.
    fn return_loaded_model(&self, vb: VarBuilder) -> Result<Self::LoadedModel, SurrealError> {
        let config = self.config();
        let model = MixtralModel::new(&config, vb).map_err(|e| {
            SurrealError::new(
                format!("Failed to load Mixtral model: {}", e),
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
    use candle_transformers::models::mixtral::Config as Upstream;

    /// Enum-based config must equal the canonical upstream config.
    #[test]
    fn matches_upstream_v0_1_8x7b() {
        let expected: Upstream = Upstream::v0_1_8x7b(false);
        let ours: Upstream = Mixtral::V0_1_8x7b.config();
        assert_eq!(expected, ours);
    }

    #[test]
    fn test_mixtral8x7bv01_filenames() {
        let m = Mixtral::V0_1_8x7b;
        let filenames = m.return_tensor_filenames();
        assert_eq!(filenames.len(), 19);
        assert_eq!(filenames[0], "model-00001-of-00019.safetensors");
        assert_eq!(filenames[18], "model-00019-of-00019.safetensors");
    }
}
