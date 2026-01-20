//! Bert Pooler module for Candle.
//!
//! This module extracts the `[CLS]` token from the final hidden states of a
//! BERT-like model and applies a linear layer followed by a `tanh` activation.
//! The implementation reflects the behaviour described in Appendix A of the
//! original BERT paper (Devlin *et al.*, 2018).
//!
//! # Example
//! ```no_run
//! use candle_nn::VarBuilder;
//! use candle_transformers::models::bert::Config;
//! use crate::bert_pooler::BertPooler;
//!
//! let vb = VarBuilder::from_tensors("weights.npz")?;
//! let config = Config { hidden_size: 768, ..Default::default() };
//! let pooler = BertPooler::load(vb, &config)?;
//! let pooled = pooler.forward(&hidden_states)?;
//! ```

use candle_core::{Result as CandleResult, Tensor};
use candle_nn::{Linear, Module, VarBuilder, linear};
use candle_transformers::models::bert::Config;

/// Pooling layer that converts token-level representations into a single
/// vector per sequence by processing the `[CLS]` token.
#[derive(Debug, Clone)]
pub struct BertPooler {
    /// Dense linear layer applied to the `[CLS]` embedding.
    ///
    /// * **Input shape:** `(batch, hidden_size)`
    /// * **Output shape:** `(batch, hidden_size)`
    ///
    /// The weights are initialised and stored under the *"dense"* prefix in the
    /// model checkpoint.
    dense: Linear,
}

impl BertPooler {
    /// Construct a new [`BertPooler`].
    ///
    /// # Arguments
    /// * `vb` – Variable builder providing access to the model's parameters.
    /// * `config` – BERT model configuration; only `hidden_size` is used.
    ///
    /// The internal linear layer maps from `hidden_size` back to
    /// `hidden_size`, as required by the original architecture.
    pub fn load(vb: VarBuilder, config: &Config) -> CandleResult<Self> {
        let dense = linear(config.hidden_size, config.hidden_size, vb.pp("dense"))?;
        Ok(Self { dense })
    }

    /// Forward pass.
    ///
    /// # Arguments
    /// * `hidden_states` – The final hidden states output by the last
    ///   transformer layer.
    ///   * **Shape:** `(batch, seq_len, hidden_size)`
    ///   * The first token along `seq_len` is expected to be the `[CLS]` token,
    ///     whose embedding is pooled by this layer.
    ///
    /// # Returns
    /// A tensor of shape **(batch, hidden_size)** representing the pooled
    /// sequence embeddings.
    ///
    /// # Steps
    /// 1. Select the embedding of the `[CLS]` token (`hidden_states[:, 0, :]`).
    /// 2. Apply the linear transformation.
    /// 3. Apply a `tanh` activation.
    pub fn forward(&self, hidden_states: &Tensor) -> CandleResult<Tensor> {
        // Extract `[CLS]` token (first token in the sequence dimension).
        let cls_embedding = hidden_states
            .narrow(1, 0, 1)? // keep the first token only
            .squeeze(1)?; // remove the singleton dimension

        // Apply dense layer and activation.
        self.dense.forward(&cls_embedding)?.tanh()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candle_core::{DType, Device, Tensor};

    /// Helper that constructs a `BertPooler` with zero‑initialised weights on
    /// the CPU. Suitable for lightweight shape/property tests.
    fn build_pooler(hidden_size: usize) -> BertPooler {
        let device = &Device::Cpu;
        let vb = VarBuilder::zeros(DType::F32, device);
        let config = Config {
            hidden_size,
            ..Default::default()
        };
        BertPooler::load(vb, &config).expect("failed to build pooler")
    }

    #[test]
    fn forward_produces_expected_shape() {
        let (batch, seq_len, hidden) = (2_usize, 5_usize, 16_usize);
        let pooler = build_pooler(hidden);
        let input = Tensor::ones((batch, seq_len, hidden), DType::F32, &Device::Cpu).unwrap();
        let output = pooler.forward(&input).unwrap();
        assert_eq!(output.dims(), &[batch, hidden]);
    }

    #[test]
    fn works_with_various_sequence_lengths() {
        let hidden = 8_usize;
        let pooler = build_pooler(hidden);
        for &seq_len in &[1_usize, 8_usize, 16_usize] {
            let input = Tensor::zeros((3, seq_len, hidden), DType::F32, &Device::Cpu).unwrap();
            let output = pooler.forward(&input).unwrap();
            assert_eq!(output.dims(), &[3, hidden]);
        }
    }
}
