//! Core chat completion logic: provider routing and result conversion.
use anyhow::Result;

use super::provider::{ChatMessage, ChatProvider, GenerationConfig};
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
pub fn get_provider(provider_name: &str) -> Result<Box<dyn ChatProvider>> {
	match provider_name {
		"openai" => Ok(Box::new(OpenAiProvider::from_env()?)),
		"huggingface" => Ok(Box::new(HuggingFaceProvider::new())),
		"voyage" | "claude" | "anthropic" => Ok(Box::new(VoyageProvider::from_env()?)),
		"google" | "gemini" => Ok(Box::new(GoogleProvider::from_env()?)),
		other => Err(anyhow::Error::new(Error::InvalidFunctionArguments {
			name: "ai::chat".to_owned(),
			message: format!(
				"Unknown provider '{other}'. Supported providers: \
				 'openai', 'huggingface', 'voyage' (or 'claude'/'anthropic'), 'google' (or 'gemini')"
			),
		})),
	}
}

/// Conduct a multi-turn chat conversation using the specified model.
///
/// Routes to the appropriate provider based on the model identifier prefix.
pub async fn chat(
	model_id: &str,
	messages: &[ChatMessage],
	config: &GenerationConfig,
) -> Result<String> {
	let (provider_name, model_name) = parse_model_id(model_id)?;

	match provider_name {
		"openai" => {
			let provider = OpenAiProvider::from_env()?;
			provider.chat(model_name, messages, config).await
		}
		"huggingface" => {
			let provider = HuggingFaceProvider::new();
			provider.chat(model_name, messages, config).await
		}
		"voyage" | "claude" | "anthropic" => {
			let provider = VoyageProvider::from_env()?;
			provider.chat(model_name, messages, config).await
		}
		"google" | "gemini" => {
			let provider = GoogleProvider::from_env()?;
			provider.chat(model_name, messages, config).await
		}
		other => Err(anyhow::Error::new(Error::InvalidFunctionArguments {
			name: "ai::chat".to_owned(),
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
		let result = chat("badprovider:some-model", &messages, &config).await;
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
		let result = chat("just-a-model-name", &messages, &config).await;
		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("provider:model"), "Expected format hint in error: {err_msg}");
	}

	#[tokio::test]
	async fn chat_openai_via_mock() {
		use wiremock::matchers::{method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"id": "chatcmpl-abc123",
			"object": "chat.completion",
			"choices": [{
				"index": 0,
				"message": {
					"role": "assistant",
					"content": "SurrealDB is a multi-model database."
				},
				"finish_reason": "stop"
			}],
			"usage": {
				"prompt_tokens": 20,
				"completion_tokens": 7,
				"total_tokens": 27
			}
		});

		Mock::given(method("POST"))
			.and(path("/chat/completions"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		unsafe {
			std::env::set_var("SURREAL_AI_OPENAI_API_KEY", "test-key");
			std::env::set_var("SURREAL_AI_OPENAI_BASE_URL", server.uri());
		}

		let config = GenerationConfig::default();
		let messages = vec![
			ChatMessage::text("system", "You are a helpful assistant."),
			ChatMessage::text("user", "What is SurrealDB?"),
		];
		let result = chat("openai:gpt-4-turbo", &messages, &config).await;

		unsafe {
			std::env::remove_var("SURREAL_AI_OPENAI_API_KEY");
			std::env::remove_var("SURREAL_AI_OPENAI_BASE_URL");
		}

		let text = result.expect("chat should succeed via mock");
		assert_eq!(text, "SurrealDB is a multi-model database.");

		server.verify().await;
	}
}
