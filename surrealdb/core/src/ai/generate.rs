//! Core text generation logic: provider routing and result conversion.
use anyhow::Result;

use super::config::{
	AiConfigOverlay, google_credentials, huggingface_credentials, openai_credentials,
	voyage_credentials,
};
use super::provider::{GenerationConfig, GenerationProvider};
use super::providers::google::GoogleProvider;
use super::providers::huggingface::HuggingFaceProvider;
use super::providers::openai::OpenAiProvider;
use super::providers::voyage::VoyageProvider;
use crate::err::Error;

/// Parse a model identifier into `(provider, model_name)`.
///
/// Model identifiers use the format `provider:model_name`, e.g.:
/// - `openai:gpt-4-turbo`
/// - `huggingface:mistralai/Mistral-7B-Instruct-v0.3`
fn parse_model_id(model_id: &str) -> Result<(&str, &str)> {
	model_id.split_once(':').ok_or_else(|| {
		anyhow::Error::new(Error::InvalidFunctionArguments {
			name: "ai::generate".to_owned(),
			message: format!(
				"Model ID must be in 'provider:model' format \
				 (e.g. 'openai:gpt-4-turbo'), got: '{model_id}'"
			),
		})
	})
}

/// Generate text from a prompt using the specified model.
///
/// Routes to the appropriate provider based on the model identifier prefix.
/// When `ai_config` is `Some`, provider credentials and base URLs are taken
/// from the overlay first, then fall back to environment variables.
pub async fn generate(
	model_id: &str,
	prompt: &str,
	config: &GenerationConfig,
	ai_config: Option<&AiConfigOverlay>,
) -> Result<String> {
	let (provider_name, model_name) = parse_model_id(model_id)?;

	match provider_name {
		"openai" => {
			let (api_key, base_url) = openai_credentials(ai_config)?;
			let provider = OpenAiProvider::new(api_key, base_url);
			provider.generate(model_name, prompt, config).await
		}
		"huggingface" => {
			let (api_key, base_url, generation_base_url) = huggingface_credentials(ai_config)?;
			let provider =
				HuggingFaceProvider::new_with_urls(api_key, base_url, generation_base_url);
			provider.generate(model_name, prompt, config).await
		}
		"voyage" | "claude" | "anthropic" => {
			let (api_key, base_url) = voyage_credentials(ai_config)?;
			let provider = VoyageProvider::new(api_key, base_url);
			provider.generate(model_name, prompt, config).await
		}
		"google" | "gemini" => {
			let (api_key, base_url) = google_credentials(ai_config)?;
			let provider = GoogleProvider::new(api_key, base_url);
			provider.generate(model_name, prompt, config).await
		}
		other => Err(anyhow::Error::new(Error::InvalidFunctionArguments {
			name: "ai::generate".to_owned(),
			message: format!(
				"Unknown provider '{other}'. Supported providers: \
				 'openai', 'huggingface', 'voyage' (or 'claude'/'anthropic'), 'google' (or 'gemini')"
			),
		})),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_model_id_valid_openai() {
		let (provider, model) = parse_model_id("openai:gpt-4-turbo").unwrap();
		assert_eq!(provider, "openai");
		assert_eq!(model, "gpt-4-turbo");
	}

	#[test]
	fn parse_model_id_valid_huggingface() {
		let (provider, model) =
			parse_model_id("huggingface:mistralai/Mistral-7B-Instruct-v0.3").unwrap();
		assert_eq!(provider, "huggingface");
		assert_eq!(model, "mistralai/Mistral-7B-Instruct-v0.3");
	}

	#[test]
	fn parse_model_id_missing_prefix() {
		let result = parse_model_id("gpt-4-turbo");
		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("provider:model"), "Expected format hint in error: {err_msg}");
	}

	#[test]
	fn parse_model_id_empty_string() {
		let result = parse_model_id("");
		assert!(result.is_err());
	}

	#[tokio::test]
	async fn generate_unknown_provider_returns_error() {
		let config = GenerationConfig::default();
		let result = generate("badprovider:some-model", "hello", &config, None).await;
		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(
			err_msg.contains("Unknown provider"),
			"Expected 'Unknown provider' in error: {err_msg}"
		);
	}

	#[tokio::test]
	async fn generate_missing_prefix_returns_error() {
		let config = GenerationConfig::default();
		let result = generate("just-a-model-name", "hello", &config, None).await;
		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("provider:model"), "Expected format hint in error: {err_msg}");
	}
}
