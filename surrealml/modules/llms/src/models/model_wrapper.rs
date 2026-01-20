//! High-level **runtime selector**.

use crate::models::model_spec::model_spec_trait::ModelSpec;
use crate::models::model_spec::models::{
    falcon::Falcon, gemma::Gemma, gemma2::Gemma2, gemma3::Gemma3, mistral::Mistral,
    mixtral::Mixtral,
};
use crate::models::model_wrapper_state::State;
use crate::utils::error::{SurrealError, SurrealErrorStatus};
use candle_transformers::models::mimi::candle_nn::VarBuilder;
use std::str::FromStr;
use surrealml_tokenizers::Tokenizer;

/// Runtime wrapper – one **variant per supported family**, each holding a
/// `State<S>` whose `spec` is pre-selected.
///
/// The `loaded` field inside each state starts as `None`; call
/// [`ModelWrapper::load`] to fill it.
pub enum ModelWrapper {
    Falcon(State<Falcon>),
    Gemma(State<Gemma>),
    Gemma2(State<Gemma2>),
    Gemma3(State<Gemma3>),
    Mistral(State<Mistral>),
    Mixtral(State<Mixtral>),
}

impl FromStr for ModelWrapper {
    type Err = SurrealError;

    /// Parse a Hugging-Face-style identifier into the correct variant.
    ///
    /// # Errors
    /// Returns `SurrealErrorStatus::BadRequest` if the string does not match
    /// any supported checkpoint.
    fn from_str(model: &str) -> Result<Self, Self::Err> {
        match model {
            "tiiuae/falcon-7b" => Ok(Self::Falcon(State::new(Falcon::Falcon7B))),
            "google/gemma-7b" => Ok(Self::Gemma(State::new(Gemma))),
            "google/gemma-2b" => Ok(Self::Gemma2(State::new(Gemma2))),
            "google/gemma-3-4b-it" => Ok(Self::Gemma3(State::new(Gemma3))),
            "mistralai/Mistral-7B-v0.1" => Ok(Self::Mistral(State::new(Mistral::V7bV0_1))),
            "amazon/MistralLite" => Ok(Self::Mistral(State::new(Mistral::AmazonLite))),
            "mistralai/Mixtral-8x7B-v0.1" => Ok(Self::Mixtral(State::new(Mixtral::V0_1_8x7b))),
            other => Err(SurrealError::new(
                format!("Invalid model identifier: {other}"),
                SurrealErrorStatus::BadRequest,
            )),
        }
    }
}

impl ModelWrapper {
    pub fn to_string(&self) -> String {
        match self {
            ModelWrapper::Falcon(_) => "tiiuae/falcon-7b".to_string(),
            ModelWrapper::Gemma(_) => "google/gemma-7b".to_string(),
            ModelWrapper::Gemma2(_) => "google/gemma-2b".to_string(),
            ModelWrapper::Gemma3(_) => "google/gemma-3-4b-it".to_string(),
            ModelWrapper::Mistral(s) => match s.spec {
                crate::models::model_spec::models::mistral::Mistral::V7bV0_1 => {
                    "mistralai/Mistral-7B-v0.1".to_string()
                }
                crate::models::model_spec::models::mistral::Mistral::AmazonLite => {
                    "amazon/MistralLite".to_string()
                }
            },
            ModelWrapper::Mixtral(s) => match s.spec {
                crate::models::model_spec::models::mixtral::Mixtral::V0_1_8x7b => {
                    "mistralai/Mixtral-8x7B-v0.1".to_string()
                }
            },
        }
    }

    /// Initialize the underlying model by forwarding to the inner [`State::load`].
    ///
    /// # Errors
    /// * `SurrealErrorStatus::Unknown` if the model was already loaded.
    /// * Whatever error your specific model’s `return_loaded_model` may produce.
    pub fn load(&mut self, vb: VarBuilder) -> Result<(), SurrealError> {
        match self {
            ModelWrapper::Falcon(s) => s.load(vb),
            ModelWrapper::Gemma(s) => s.load(vb),
            ModelWrapper::Gemma2(s) => s.load(vb),
            ModelWrapper::Gemma3(s) => s.load(vb),
            ModelWrapper::Mistral(s) => s.load(vb),
            ModelWrapper::Mixtral(s) => s.load(vb),
        }
    }

    /// Return the list of tensor filenames by forwarding to each model’s spec.
    ///
    /// This saves you from having to `match` on each variant yourself.
    pub fn tensor_filenames(&self) -> Vec<String> {
        match self {
            ModelWrapper::Falcon(s) => s.spec.return_tensor_filenames(),
            ModelWrapper::Gemma(s) => s.spec.return_tensor_filenames(),
            ModelWrapper::Gemma2(s) => s.spec.return_tensor_filenames(),
            ModelWrapper::Gemma3(s) => s.spec.return_tensor_filenames(),
            ModelWrapper::Mistral(s) => s.spec.return_tensor_filenames(),
            ModelWrapper::Mixtral(s) => s.spec.return_tensor_filenames(),
        }
    }

    /// Return the list of tensor filenames by forwarding to each model’s spec.
    ///
    /// This saves you from having to `match` on each variant yourself.
    pub fn run_model(
        &mut self,
        input_ids: &[u32],
        max_steps: usize,
        tokenizer: &Tokenizer,
    ) -> Result<String, SurrealError> {
        // For now, we only implement this for Gemma.
        let model_result = match self {
            ModelWrapper::Gemma(s) => s.run_model(input_ids, max_steps, tokenizer)?,
            _ => {
                return Err(SurrealError::new(
                    "ModelWrapper::run_model is not implemented for this model".to_string(),
                    SurrealErrorStatus::BadRequest,
                ));
            }
        };

        Ok(model_result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candle_core::{DType, Device};
    use candle_transformers::models::mimi::candle_nn::VarBuilder;
    use std::str::FromStr;

    #[test]
    fn parses_known_identifiers() {
        assert!(matches!(
            ModelWrapper::from_str("tiiuae/falcon-7b"),
            Ok(ModelWrapper::Falcon(_))
        ));

        assert!(matches!(
            ModelWrapper::from_str("google/gemma-7b"),
            Ok(ModelWrapper::Gemma(_))
        ));

        assert!(matches!(
            ModelWrapper::from_str("mistralai/Mixtral-8x7B-v0.1"),
            Ok(ModelWrapper::Mixtral(_))
        ));
    }

    #[test]
    fn to_string_roundtrip() {
        let ids = [
            "tiiuae/falcon-7b",
            "google/gemma-7b",
            "google/gemma-2b",
            "google/gemma-3-4b-it",
            "mistralai/Mistral-7B-v0.1",
            "amazon/MistralLite",
            "mistralai/Mixtral-8x7B-v0.1",
        ];
        for &id in &ids {
            let wrapper = ModelWrapper::from_str(id).unwrap();
            assert_eq!(wrapper.to_string(), id);
        }
    }

    #[test]
    fn unknown_identifier_yields_error() {
        assert!(ModelWrapper::from_str("unknown/model").is_err());
    }

    fn dummy_vb() -> VarBuilder<'static> {
        VarBuilder::zeros(DType::F32, &Device::Cpu)
    }

    #[test]
    fn tensor_filenames_matches_expected_count() {
        let w = ModelWrapper::from_str("google/gemma-7b").unwrap();
        let files = w.tensor_filenames();
        assert_eq!(files.len(), 4, "Gemma-7B should list 4 shards");
        assert_eq!(&files[0], "model-00001-of-00004.safetensors");
    }

    #[test]
    fn load_once_then_error_via_wrapper() {
        let mut w = ModelWrapper::from_str("google/gemma-7b").unwrap();
        let vb1 = dummy_vb();
        assert!(w.load(vb1.clone()).is_ok(), "first load should succeed");
        let err = w.load(vb1).unwrap_err();
        assert_eq!(err.status, SurrealErrorStatus::Unknown);
        assert!(
            err.message.contains("already loaded"),
            "unexpected message: {}",
            err.message
        );
    }
}
