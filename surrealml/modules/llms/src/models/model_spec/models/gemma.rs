//! Utilities for working with **preset Gemma model configurations**.
use crate::models::model_spec::model_spec_trait::ModelSpec;
use crate::utils::error::{SurrealError, SurrealErrorStatus};
use candle_core::{DType, Device, Tensor};
use candle_nn::activation::Activation;
use candle_transformers::models::gemma::Config as GemmaConfig;
use candle_transformers::models::gemma::Model as GemmaModel;
use candle_transformers::models::mimi::candle_nn::VarBuilder;
use surrealml_tokenizers::Tokenizer;

/// Marker type for the **Gemma** family.
///
/// Implements [`ModelSpec`] so callers can ask generically for
/// `M::Cfg` where `M = Gemma`.
pub struct Gemma;

impl ModelSpec for Gemma {
    type Cfg = GemmaConfig;
    type LoadedModel = GemmaModel;

    /// Return the `GemmaConfig` for the Gemma-7B checkpoint.
    ///
    /// The values below come from the upstream `config.json` distributed
    /// alongside the model on the Hugging Face Hub:
    /// <https://huggingface.co/google/gemma-7b/blob/main/config.json>
    ///
    /// The `use_flash_attn` parameter does not apply to Gemma; we always
    /// initialize with the standard configuration. If in the future Candle
    /// adds CUDA/Flash-Attn support for Gemma, this method may be updated.
    ///
    /// # Returns
    /// * `GemmaConfig` with the hard-coded hyperparameters for Gemma-7B.
    fn config(&self) -> Self::Cfg {
        GemmaConfig {
            attention_bias: false,
            head_dim: 256,
            hidden_act: Some(Activation::Gelu),
            hidden_activation: None,
            hidden_size: 3_072,
            intermediate_size: 24_576,
            num_attention_heads: 16,
            num_hidden_layers: 28,
            num_key_value_heads: 16,
            rms_norm_eps: 1e-6,
            rope_theta: 10_000.0,
            vocab_size: 256_000,
            max_position_embeddings: 8_192,
        }
    }
    /// Returns a list of 4 `.safetensors` tensor filenames for Gemma-7B.
    ///
    /// Filenames follow the pattern `model-<index>-of-00004.safetensors`.
    ///
    /// # Returns
    /// A `Vec<String>` containing 4 filenames, from
    /// `"model-00001-of-00004.safetensors"` through
    /// `"model-00004-of-00004.safetensors"`.
    fn return_tensor_filenames(&self) -> Vec<String> {
        let tensor_count: u8 = 4;
        let total_str = format!("{:05}", tensor_count);
        let mut filenames = Vec::with_capacity(4);
        for i in 1..=tensor_count {
            let idx_str = format!("{:05}", i);
            filenames.push(format!("model-{}-of-{}.safetensors", idx_str, total_str));
        }
        filenames
    }

    /// Returns a loaded model for Gemma. Takes in the VarBuilder object
    /// for the model. Note we hardcode use_flash_attn to `false` since
    /// we're not yet supporting CUDA.
    ///
    /// # Returns
    /// A `LoadedModel` object containing the loaded model.
    fn return_loaded_model(&self, vb: VarBuilder) -> Result<Self::LoadedModel, SurrealError> {
        let config = self.config();
        let model = GemmaModel::new(false, &config, vb).map_err(|e| {
            SurrealError::new(
                format!("Failed to load Gemma model: {}", e),
                SurrealErrorStatus::Unknown,
            )
        })?;
        Ok(model)
    }

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
    fn run_model(
        &self,
        model: &mut <Self as ModelSpec>::LoadedModel,
        input_ids: &[u32],
        max_steps: usize,
        tokenizer: &Tokenizer,
    ) -> Result<String, SurrealError> {
        // For now preset device to CPU.
        let device = Device::Cpu;

        // Prepend BOS if absent
        let mut ids = input_ids.to_vec();
        if input_ids.first() != Some(&2) {
            // 2 is the BOS token for Gemma
            ids.insert(0, 2);
        }

        // Turn the prompt into a [1, seq_len] tensor
        let prompt_tensor = Tensor::new(&ids[..], &device)
            .map_err(|e| {
                SurrealError::new(
                    format!("Failed to build input tensor: {}", e),
                    SurrealErrorStatus::BadRequest,
                )
            })?
            .unsqueeze(0)
            .map_err(|e| {
                SurrealError::new(
                    format!("Failed to unsqueeze input tensor: {}", e),
                    SurrealErrorStatus::BadRequest,
                )
            })?;

        // Warm up Gemma on the entire prompt
        Self::warmup(model, &prompt_tensor)?;

        // Autoregressive generation
        let generated = Self::generate(model, &ids, device, max_steps, tokenizer)?;
        Ok(generated)
    }
}

impl Gemma {
    /// Do the first forward‐pass over the whole prompt (seqlen_offset = 0).
    fn warmup(
        model: &mut <Self as ModelSpec>::LoadedModel,
        prompt: &Tensor,
    ) -> Result<(), SurrealError> {
        model.forward(prompt, 0).map(|_| ()).map_err(|e| {
            SurrealError::new(
                format!("Gemma warmup failed: {}", e),
                SurrealErrorStatus::Unknown,
            )
        })
    }

    /// Autoregressively generate up to `max_steps` new tokens.
    ///
    /// # Arguments
    /// - `model`: The already-loaded neural-network weights plus any internal state (e.g., past-key memory). It’s mutable because inference updates the model’s cached KV tensors for each new step.
    /// - `input_ids`: The token IDs of the user-supplied prompt. The last element starts the autoregressive loop; the full slice establishes the offset so the model knows how far into the sequence it is.
    /// - `device`: Tells the tensor library which backend to place new tensors on (CPU, CUDA GPU, Metal, etc.). Every temporary tensor (token_t, scores_t) is created on this device so that operations stay on the same accelerator.
    /// - `max_steps`: Hard ceiling on how many new tokens the function will try to generate. The loop breaks early on EOS (token ID 2) but never exceeds this count, preventing runaway inference.
    /// - `tokenizer`: Used twice: (1) to turn the winning prev_id into human-readable text that gets appended to output; (2) to handle byte-pair/word-piece quirks such as merging spaces (decode(&[prev_id], true)).
    ///
    /// # Returns
    /// The response string from the
    fn generate(
        model: &mut <Self as ModelSpec>::LoadedModel,
        input_ids: &[u32],
        device: Device,
        max_steps: usize,
        tokenizer: &Tokenizer,
    ) -> Result<String, SurrealError> {
        let mut output = String::new();
        let mut prev_id = *input_ids.last().ok_or_else(|| {
            SurrealError::new(
                "No tokens in prompt".to_string(),
                SurrealErrorStatus::BadRequest,
            )
        })?;
        let prompt_len = input_ids.len();

        for step in 0..max_steps {
            // make [1,1] tensor for the last token
            let token_t = Tensor::new(&[prev_id], &device)
                .map_err(|e| {
                    SurrealError::new(
                        format!("Failed to build step tensor: {}", e),
                        SurrealErrorStatus::BadRequest,
                    )
                })?
                .unsqueeze(0)
                .map_err(|e| {
                    SurrealError::new(
                        format!("Failed to unsqueeze step tensor: {}", e),
                        SurrealErrorStatus::BadRequest,
                    )
                })?;

            // forward with growing offset
            let offset = prompt_len + step;
            let logits = model.forward(&token_t, offset).map_err(|e| {
                SurrealError::new(
                    format!("Gemma forward failed at step {}: {}", step, e),
                    SurrealErrorStatus::Unknown,
                )
            })?;

            // [1,1,V] → [V]
            let scores_t = logits.squeeze(0).and_then(|t| t.squeeze(0)).map_err(|e| {
                SurrealError::new(
                    format!("Failed to squeeze logits: {}", e),
                    SurrealErrorStatus::Unknown,
                )
            })?;

            // 👉 Ensure dtype = F32 so we can safely call to_vec1::<f32>()
            let scores_t = if scores_t.dtype() != DType::F32 {
                scores_t.to_dtype(DType::F32).map_err(|e| {
                    SurrealError::new(
                        format!("Failed to cast logits to F32: {}", e),
                        SurrealErrorStatus::Unknown,
                    )
                })?
            } else {
                scores_t
            };

            let scores = scores_t.to_vec1::<f32>().map_err(|e| {
                SurrealError::new(
                    format!("Failed to extract logits to Vec: {}", e),
                    SurrealErrorStatus::Unknown,
                )
            })?;

            // greedy pick
            let (tok, _) = scores
                .iter()
                .enumerate()
                .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
                .unwrap();
            prev_id = tok as u32;

            // EOS check (Gemma’s EOS might differ—adjust as needed)
            if prev_id == 2 {
                break;
            }

            // decode & append
            let text = tokenizer.decode(&[prev_id], true).map_err(|e| {
                SurrealError::new(
                    format!("Token decode error: {}", e),
                    SurrealErrorStatus::Unknown,
                )
            })?;
            output.push_str(&text);
            output.push(' ');
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candle_transformers::models::gemma::Config as Upstream;

    /// Config must equal the manually constructed `GemmaConfig` values.
    #[test]
    fn matches_expected_fields() {
        let ours: Upstream = Gemma.config();
        // Check individual fields
        assert_eq!(ours.attention_bias, false);
        assert_eq!(ours.head_dim, 256);
        assert_eq!(ours.hidden_size, 3_072);
    }

    #[test]
    fn test_gemma7b_filenames() {
        let m = Gemma;
        let filenames = m.return_tensor_filenames();
        assert_eq!(filenames.len(), 4);
        assert_eq!(filenames[0], "model-00001-of-00004.safetensors");
        assert_eq!(filenames[3], "model-00004-of-00004.safetensors");
    }

    // This runs only when the real Gemma-7B weights are present in the user’s
    // Hugging Face cache **and** the crate is built with
    // `--features local-gemma-test`.
    #[cfg(feature = "local-gemma-test")]
    #[test]
    fn test_return_loaded_model_success() {
        use crate::tensors::tensor_utils::load_model_vars;
        use candle_core::DType;
        use std::path::PathBuf;

        let home = std::env::var("HOME").expect("HOME env var not set");
        let snapshot_base = PathBuf::from(home)
            .join(".cache")
            .join("huggingface")
            .join("hub")
            .join("models--google--gemma-7b")
            .join("snapshots");

        let snapshot_dir = std::fs::read_dir(&snapshot_base)
            .expect("cannot read Hugging Face cache")
            .next()
            .expect("no Gemma-7B snapshot found")
            .expect("failed to access snapshot entry")
            .path();

        let filenames = Gemma.return_tensor_filenames();
        let paths: Vec<PathBuf> = filenames.iter().map(|f| snapshot_dir.join(f)).collect();

        for p in &paths {
            assert!(p.exists(), "expected tensor file to exist: {:?}", p);
        }

        let vb = load_model_vars(&paths, DType::F16)
            .expect("load_model_vars must succeed with real Gemma-7B weights");

        let loaded = Gemma
            .return_loaded_model(vb)
            .expect("Gemma model should load");

        // Type guard: `loaded` is the concrete GemmaModel.
        let _: GemmaModel = loaded;
    }
}
