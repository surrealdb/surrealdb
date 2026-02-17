//! Core text generation logic: provider routing and result conversion.
use anyhow::Result;

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
pub async fn generate(model_id: &str, prompt: &str, config: &GenerationConfig) -> Result<String> {
	let (provider_name, model_name) = parse_model_id(model_id)?;

	match provider_name {
		"openai" => {
			let provider = OpenAiProvider::from_env()?;
			provider.generate(model_name, prompt, config).await
		}
		"huggingface" => {
			let provider = HuggingFaceProvider::new();
			provider.generate(model_name, prompt, config).await
		}
		"voyage" | "claude" | "anthropic" => {
			let provider = VoyageProvider::from_env()?;
			provider.generate(model_name, prompt, config).await
		}
		"google" | "gemini" => {
			let provider = GoogleProvider::from_env()?;
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
		let result = generate("badprovider:some-model", "hello", &config).await;
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
		let result = generate("just-a-model-name", "hello", &config).await;
		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("provider:model"), "Expected format hint in error: {err_msg}");
	}

	#[tokio::test]
	async fn generate_openai_via_mock() {
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
					"content": "Hello! How can I help you?"
				},
				"finish_reason": "stop"
			}],
			"usage": {
				"prompt_tokens": 10,
				"completion_tokens": 7,
				"total_tokens": 17
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
		let result = generate("openai:gpt-4-turbo", "Hello", &config).await;

		unsafe {
			std::env::remove_var("SURREAL_AI_OPENAI_API_KEY");
			std::env::remove_var("SURREAL_AI_OPENAI_BASE_URL");
		}

		let text = result.expect("generate should succeed via mock");
		assert_eq!(text, "Hello! How can I help you?");

		server.verify().await;
	}
}
