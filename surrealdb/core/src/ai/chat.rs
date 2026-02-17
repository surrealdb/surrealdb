//! Core chat completion logic: provider routing and result conversion.
use anyhow::Result;

use super::config::{
	AiConfigOverlay, anthropic_credentials, google_credentials, huggingface_credentials,
	openai_credentials, voyage_credentials,
};
use super::provider::{ChatMessage, ChatProvider, GenerationConfig};
use super::providers::anthropic::AnthropicProvider;
use super::providers::google::GoogleProvider;
use super::providers::huggingface::HuggingFaceProvider;
use super::providers::openai::OpenAiProvider;
use super::providers::voyage::VoyageProvider;
use crate::err::Error;

/// Parse a model identifier into `(provider, model_name)`.
///
/// Model identifiers use the format `provider:model_name`, e.g.:
/// - `openai:gpt-4-turbo`
/// - `google:gemini-2.0-flash`
pub fn parse_model_id(model_id: &str) -> Result<(&str, &str)> {
	model_id.split_once(':').ok_or_else(|| {
		anyhow::Error::new(Error::InvalidFunctionArguments {
			name: "ai::chat".to_owned(),
			message: format!(
				"Model ID must be in 'provider:model' format \
				 (e.g. 'openai:gpt-4-turbo'), got: '{model_id}'"
			),
		})
	})
}

/// Get a chat provider for the given provider name.
///
/// When `config` is `Some`, provider credentials and base URLs are taken from
/// the overlay first, then fall back to environment variables.
pub fn get_provider(
	provider_name: &str,
	config: Option<&AiConfigOverlay>,
) -> Result<Box<dyn ChatProvider>> {
	match provider_name {
		"openai" => {
			let (api_key, base_url) = openai_credentials(config)?;
			Ok(Box::new(OpenAiProvider::new(api_key, base_url)))
		}
		"huggingface" => {
			let (api_key, base_url, generation_base_url) = huggingface_credentials(config)?;
			Ok(Box::new(HuggingFaceProvider::new_with_urls(api_key, base_url, generation_base_url)))
		}
		"anthropic" | "claude" => {
			let (api_key, base_url) = anthropic_credentials(config)?;
			Ok(Box::new(AnthropicProvider::new(api_key, base_url)))
		}
		"voyage" => {
			let (api_key, base_url) = voyage_credentials(config)?;
			Ok(Box::new(VoyageProvider::new(api_key, base_url)))
		}
		"google" | "gemini" => {
			let (api_key, base_url) = google_credentials(config)?;
			Ok(Box::new(GoogleProvider::new(api_key, base_url)))
		}
		other => Err(anyhow::Error::new(Error::InvalidFunctionArguments {
			name: "ai::chat".to_owned(),
			message: format!(
				"Unknown provider '{other}'. Supported providers: \
				 'openai', 'anthropic' (or 'claude'), 'google' (or 'gemini'), 'voyage', 'huggingface'"
			),
		})),
	}
}

/// Conduct a multi-turn chat conversation using the specified model.
///
/// Routes to the appropriate provider based on the model identifier prefix.
/// When `ai_config` is `Some`, provider credentials and base URLs are taken
/// from the overlay first, then fall back to environment variables.
pub async fn chat(
	model_id: &str,
	messages: &[ChatMessage],
	config: &GenerationConfig,
	ai_config: Option<&AiConfigOverlay>,
) -> Result<String> {
	let (provider_name, model_name) = parse_model_id(model_id)?;
	let provider = get_provider(provider_name, ai_config)?;
	provider.chat(model_name, messages, config).await
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
	fn parse_model_id_valid_google() {
		let (provider, model) = parse_model_id("google:gemini-2.0-flash").unwrap();
		assert_eq!(provider, "google");
		assert_eq!(model, "gemini-2.0-flash");
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
	async fn chat_unknown_provider_returns_error() {
		let config = GenerationConfig::default();
		let messages = vec![ChatMessage::text("user", "hello")];
		let result = chat("badprovider:some-model", &messages, &config, None).await;
		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(
			err_msg.contains("Unknown provider"),
			"Expected 'Unknown provider' in error: {err_msg}"
		);
	}

	#[tokio::test]
	async fn chat_missing_prefix_returns_error() {
		let config = GenerationConfig::default();
		let messages = vec![ChatMessage::text("user", "hello")];
		let result = chat("just-a-model-name", &messages, &config, None).await;
		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("provider:model"), "Expected format hint in error: {err_msg}");
	}
}
