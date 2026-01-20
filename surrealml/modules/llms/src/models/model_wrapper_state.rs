//! Utilities for **wrapping a `ModelSpec` preset together with its
//! (eventually-)loaded Candle model**.

use crate::models::model_spec::model_spec_trait::ModelSpec;
use crate::utils::error::{SurrealError, SurrealErrorStatus};
use candle_transformers::models::mimi::candle_nn::VarBuilder;
use surrealml_tokenizers::Tokenizer;

/// Container that tracks both the *spec* (compile-time preset) **and**
/// the loaded Candle model.
///
/// * `spec`   – any type that implements [`ModelSpec`].  
/// * `loaded` – `None` until you call [`State::load`]; afterwards
///              `Some(S::LoadedModel)`.
#[derive(Debug, Clone, PartialEq)]
pub struct State<S: ModelSpec> {
    pub spec: S,
    pub loaded: Option<S::LoadedModel>,
}

impl<S: ModelSpec> State<S> {
    /// Create a fresh state whose model is **not yet loaded**.
    pub fn new(spec: S) -> Self {
        Self { spec, loaded: None }
    }

    /// Consume a `VarBuilder` and initialize the runtime model **once**.
    ///
    /// # Errors
    /// * `SurrealErrorStatus::Unknown` if the model is already loaded.
    /// * Whatever error `S::return_loaded_model` propagates while
    ///   constructing the concrete Candle model.
    pub fn load(&mut self, vb: VarBuilder) -> Result<(), SurrealError> {
        if self.loaded.is_some() {
            return Err(SurrealError::new(
                "Model already loaded".into(),
                SurrealErrorStatus::Unknown,
            ));
        }

        self.loaded = Some(self.spec.return_loaded_model(vb)?);
        Ok(())
    }

    pub fn run_model(
        &mut self,
        input_ids: &[u32],
        max_steps: usize,
        tokenizer: &Tokenizer,
    ) -> Result<String, SurrealError> {
        if self.loaded.is_none() {
            return Err(SurrealError::new(
                "Model not yet loaded".into(),
                SurrealErrorStatus::BadRequest,
            ));
        };

        let model = self.loaded.as_mut().ok_or_else(|| {
            SurrealError::new("Error loading model".into(), SurrealErrorStatus::BadRequest)
        })?;

        let model_result = self
            .spec
            .run_model(model, input_ids, max_steps, tokenizer)?;
        Ok(model_result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candle_core::{DType, Device};
    use candle_transformers::models::mimi::candle_nn::VarBuilder;

    struct DummySpec;
    struct DummyModel;

    impl ModelSpec for DummySpec {
        type Cfg = ();
        type LoadedModel = DummyModel;

        fn config(&self) -> Self::Cfg {
            ()
        }
        fn return_tensor_filenames(&self) -> Vec<String> {
            Vec::new()
        }
        fn return_loaded_model(&self, _vb: VarBuilder) -> Result<Self::LoadedModel, SurrealError> {
            Ok(DummyModel)
        }
        fn run_model(
            &self,
            _model: &mut Self::LoadedModel,
            _input_ids: &[u32],
            _max_steps: usize,
            _tokenizer: &Tokenizer,
        ) -> Result<String, SurrealError> {
            Ok("dummy output".into())
        }
    }

    fn dummy_vb() -> VarBuilder<'static> {
        VarBuilder::zeros(DType::F32, &Device::Cpu)
    }

    #[test]
    fn new_state_is_unloaded() {
        let state: State<DummySpec> = State::new(DummySpec);
        assert!(state.loaded.is_none());
    }

    #[test]
    fn load_populates_loaded_field() {
        let mut state = State::new(DummySpec);
        assert!(state.load(dummy_vb()).is_ok());
        assert!(state.loaded.is_some());
    }

    #[test]
    fn second_load_returns_error() {
        let mut state = State::new(DummySpec);
        state.load(dummy_vb()).unwrap();
        let err = state.load(dummy_vb()).unwrap_err();
        assert_eq!(err.status, SurrealErrorStatus::Unknown);
    }
}
