//! Voyage AI embedding and generation provider.
//!
//! Voyage uses an OpenAI-compatible API at `https://api.voyageai.com/v1`, so
//! this provider is a thin wrapper around [`OpenAiProvider`] with Voyage-specific
//! defaults and environment variable names.
//!
//! The `voyage:` prefix is the canonical name. `claude:` and `anthropic:` are
//! accepted as aliases since Voyage is Anthropic's recommended embedding provider.
//!
//! # Configuration
//!
//! - `SURREAL_AI_VOYAGE_API_KEY` — Required. A Voyage AI API key.
//! - `SURREAL_AI_VOYAGE_BASE_URL` — Optional. Defaults to `https://api.voyageai.com/v1`.
//!
//! # Usage
//!
//! ```sql
//! ai::embed('voyage:voyage-3.5', 'hello world')
//! ai::embed('claude:voyage-3.5', 'hello world')   -- alias
//! ai::embed('anthropic:voyage-3.5', 'hello world') -- alias
//! ```
use anyhow::Result;

use super::openai::OpenAiProvider;
use crate::ai::provider::{
	ChatMessage, ChatProvider, EmbeddingProvider, GenerationConfig, GenerationProvider,
};

const DEFAULT_BASE_URL: &str = "https://api.voyageai.com/v1";

/// An AI provider that delegates to the Voyage API (OpenAI-compatible).
pub struct VoyageProvider(OpenAiProvider);

impl VoyageProvider {
	/// Create a new provider with explicit configuration.
	#[cfg(test)]
	pub fn new(api_key: String, base_url: String) -> Self {
		Self(OpenAiProvider::new(api_key, base_url))
	}

	/// Create a new provider from environment variables.
	///
	/// Reads `SURREAL_AI_VOYAGE_API_KEY` (required) and
	/// `SURREAL_AI_VOYAGE_BASE_URL` (optional, defaults to Voyage).
	pub fn from_env() -> Result<Self> {
		let api_key = std::env::var("SURREAL_AI_VOYAGE_API_KEY").map_err(|_| {
			anyhow::anyhow!(
				"SURREAL_AI_VOYAGE_API_KEY environment variable is not set. \
				 Set it to your Voyage AI API key to use 'voyage:', 'claude:', or 'anthropic:' models."
			)
		})?;

		let base_url = std::env::var("SURREAL_AI_VOYAGE_BASE_URL")
			.unwrap_or_else(|_| DEFAULT_BASE_URL.to_owned());

		Ok(Self(OpenAiProvider::new(api_key, base_url)))
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl EmbeddingProvider for VoyageProvider {
	async fn embed(&self, model: &str, input: &str) -> Result<Vec<f64>> {
		self.0.embed(model, input).await
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl GenerationProvider for VoyageProvider {
	async fn generate(
		&self,
		model: &str,
		prompt: &str,
		config: &GenerationConfig,
	) -> Result<String> {
		self.0.generate(model, prompt, config).await
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl ChatProvider for VoyageProvider {
	async fn chat(
		&self,
		model: &str,
		messages: &[ChatMessage],
		config: &GenerationConfig,
	) -> Result<String> {
		self.0.chat(model, messages, config).await
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn embed_returns_vector_from_mock_api() {
		use wiremock::matchers::{header, method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"object": "list",
			"data": [{
				"object": "embedding",
				"embedding": [0.1, 0.2, 0.3, 0.4],
				"index": 0
			}],
			"model": "voyage-3.5",
			"usage": { "total_tokens": 2 }
		});

		Mock::given(method("POST"))
			.and(path("/embeddings"))
			.and(header("Authorization", "Bearer voyage-test-key"))
			.and(header("Content-Type", "application/json"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		let provider = VoyageProvider::new("voyage-test-key".into(), server.uri());
		let result = provider.embed("voyage-3.5", "hello world").await;

		let embedding = result.expect("embed should succeed with mock Voyage server");
		assert_eq!(embedding, vec![0.1, 0.2, 0.3, 0.4]);

		server.verify().await;
	}

	#[tokio::test]
	async fn embed_returns_error_on_401() {
		use wiremock::matchers::method;
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		Mock::given(method("POST"))
			.respond_with(
				ResponseTemplate::new(401).set_body_string(r#"{"detail":"Invalid API key"}"#),
			)
			.expect(1)
			.mount(&server)
			.await;

		let provider = VoyageProvider::new("bad-key".into(), server.uri());
		let result = provider.embed("voyage-3.5", "hello").await;

		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("401"), "Expected 401 in error: {err_msg}");

		server.verify().await;
	}

	#[tokio::test]
	async fn generate_returns_text_from_mock_api() {
		use wiremock::matchers::{header, method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"id": "chatcmpl-abc",
			"object": "chat.completion",
			"choices": [{
				"index": 0,
				"message": { "role": "assistant", "content": "Hello from Voyage!" },
				"finish_reason": "stop"
			}],
			"usage": { "prompt_tokens": 5, "completion_tokens": 4, "total_tokens": 9 }
		});

		Mock::given(method("POST"))
			.and(path("/chat/completions"))
			.and(header("Authorization", "Bearer voyage-test-key"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		let provider = VoyageProvider::new("voyage-test-key".into(), server.uri());
		let config = GenerationConfig::default();
		let result = provider.generate("voyage-chat", "Hello", &config).await;

		let text = result.expect("generate should succeed with mock Voyage server");
		assert_eq!(text, "Hello from Voyage!");

		server.verify().await;
	}
}
