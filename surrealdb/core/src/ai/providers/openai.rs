//! OpenAI-compatible AI provider.
//!
//! Calls `POST /v1/embeddings` and `POST /v1/chat/completions` on an
//! OpenAI-compatible endpoint. Works with OpenAI, Azure OpenAI, Ollama,
//! and any other service that implements the same API contract.
//!
//! # Configuration
//!
//! - `SURREAL_AI_OPENAI_API_KEY` — Required. The API key for authentication.
//! - `SURREAL_AI_OPENAI_BASE_URL` — Optional. Defaults to `https://api.openai.com/v1`.
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::ai::provider::{EmbeddingProvider, GenerationConfig, GenerationProvider};

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

/// An embedding provider that calls an OpenAI-compatible embeddings API.
pub struct OpenAiProvider {
	api_key: String,
	base_url: String,
}

impl OpenAiProvider {
	/// Create a new provider with explicit configuration.
	pub(crate) fn new(api_key: String, base_url: String) -> Self {
		Self {
			api_key,
			base_url,
		}
	}

	/// Create a new provider from environment variables.
	///
	/// Reads `SURREAL_AI_OPENAI_API_KEY` (required) and
	/// `SURREAL_AI_OPENAI_BASE_URL` (optional, defaults to OpenAI).
	pub fn from_env() -> Result<Self> {
		let api_key = std::env::var("SURREAL_AI_OPENAI_API_KEY").map_err(|_| {
			anyhow::anyhow!(
				"SURREAL_AI_OPENAI_API_KEY environment variable is not set. \
				 Set it to your OpenAI API key to use 'openai:' models."
			)
		})?;

		let base_url = std::env::var("SURREAL_AI_OPENAI_BASE_URL")
			.unwrap_or_else(|_| DEFAULT_BASE_URL.to_owned());

		Ok(Self {
			api_key,
			base_url,
		})
	}

	/// Build the full URL for the embeddings endpoint.
	fn embeddings_url(&self) -> String {
		let base = self.base_url.trim_end_matches('/');
		format!("{base}/embeddings")
	}

	/// Build the full URL for the chat completions endpoint.
	fn chat_completions_url(&self) -> String {
		let base = self.base_url.trim_end_matches('/');
		format!("{base}/chat/completions")
	}
}

/// Request body for the OpenAI embeddings API.
#[derive(Serialize)]
struct EmbeddingRequest<'a> {
	model: &'a str,
	input: &'a str,
}

/// Top-level response from the OpenAI embeddings API.
#[derive(Deserialize)]
struct EmbeddingResponse {
	data: Vec<EmbeddingData>,
}

/// A single embedding result within the response.
#[derive(Deserialize)]
struct EmbeddingData {
	embedding: Vec<f64>,
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl EmbeddingProvider for OpenAiProvider {
	async fn embed(&self, model: &str, input: &str) -> Result<Vec<f64>> {
		let url = self.embeddings_url();

		let body = EmbeddingRequest {
			model,
			input,
		};

		let client = reqwest::Client::new();
		let response = client
			.post(&url)
			.header("Authorization", format!("Bearer {}", self.api_key))
			.header("Content-Type", "application/json")
			.json(&body)
			.send()
			.await
			.map_err(|e| anyhow::anyhow!("Failed to call OpenAI embeddings API: {e}"))?;

		if !response.status().is_success() {
			let status = response.status();
			let body = response.text().await.unwrap_or_default();
			bail!("OpenAI embeddings API returned {status}: {body}");
		}

		let result: EmbeddingResponse = response
			.json()
			.await
			.map_err(|e| anyhow::anyhow!("Failed to parse OpenAI embeddings response: {e}"))?;

		let embedding = result
			.data
			.into_iter()
			.next()
			.ok_or_else(|| anyhow::anyhow!("OpenAI embeddings response contained no data"))?;

		Ok(embedding.embedding)
	}
}

/// A single message in the OpenAI chat completions format.
#[derive(Serialize)]
struct ChatMessage<'a> {
	role: &'a str,
	content: &'a str,
}

/// Request body for the OpenAI chat completions API.
#[derive(Serialize)]
struct ChatCompletionRequest<'a> {
	model: &'a str,
	messages: Vec<ChatMessage<'a>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	temperature: Option<f64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	max_tokens: Option<u64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	top_p: Option<f64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	stop: Option<Vec<String>>,
}

/// Top-level response from the OpenAI chat completions API.
#[derive(Deserialize)]
struct ChatCompletionResponse {
	choices: Vec<ChatChoice>,
}

/// A single choice within the chat completion response.
#[derive(Deserialize)]
struct ChatChoice {
	message: ChatChoiceMessage,
}

/// The message content within a chat completion choice.
#[derive(Deserialize)]
struct ChatChoiceMessage {
	content: Option<String>,
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl GenerationProvider for OpenAiProvider {
	async fn generate(
		&self,
		model: &str,
		prompt: &str,
		config: &GenerationConfig,
	) -> Result<String> {
		let url = self.chat_completions_url();

		let body = ChatCompletionRequest {
			model,
			messages: vec![ChatMessage {
				role: "user",
				content: prompt,
			}],
			temperature: config.temperature,
			max_tokens: config.max_tokens,
			top_p: config.top_p,
			stop: config.stop.clone(),
		};

		let client = reqwest::Client::new();
		let response = client
			.post(&url)
			.header("Authorization", format!("Bearer {}", self.api_key))
			.header("Content-Type", "application/json")
			.json(&body)
			.send()
			.await
			.map_err(|e| anyhow::anyhow!("Failed to call OpenAI chat completions API: {e}"))?;

		if !response.status().is_success() {
			let status = response.status();
			let body = response.text().await.unwrap_or_default();
			bail!("OpenAI chat completions API returned {status}: {body}");
		}

		let result: ChatCompletionResponse = response
			.json()
			.await
			.map_err(|e| {
				anyhow::anyhow!("Failed to parse OpenAI chat completions response: {e}")
			})?;

		let choice = result
			.choices
			.into_iter()
			.next()
			.ok_or_else(|| anyhow::anyhow!("OpenAI chat completions response contained no choices"))?;

		choice
			.message
			.content
			.ok_or_else(|| anyhow::anyhow!("OpenAI chat completions response contained no content"))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn embeddings_url_default() {
		let provider = OpenAiProvider::new("test".into(), DEFAULT_BASE_URL.into());
		assert_eq!(provider.embeddings_url(), "https://api.openai.com/v1/embeddings");
	}

	#[test]
	fn embeddings_url_custom_with_trailing_slash() {
		let provider =
			OpenAiProvider::new("test".into(), "https://custom.example.com/v1/".into());
		assert_eq!(provider.embeddings_url(), "https://custom.example.com/v1/embeddings");
	}

	#[test]
	fn embeddings_url_custom_no_trailing_slash() {
		let provider = OpenAiProvider::new("test".into(), "https://custom.example.com/v1".into());
		assert_eq!(provider.embeddings_url(), "https://custom.example.com/v1/embeddings");
	}

	#[test]
	fn deserialize_embedding_response() {
		let json = r#"{
			"object": "list",
			"data": [
				{
					"object": "embedding",
					"embedding": [0.1, 0.2, 0.3],
					"index": 0
				}
			],
			"model": "text-embedding-3-small",
			"usage": {
				"prompt_tokens": 5,
				"total_tokens": 5
			}
		}"#;

		let response: EmbeddingResponse = serde_json::from_str(json).unwrap();
		assert_eq!(response.data.len(), 1);
		assert_eq!(response.data[0].embedding, vec![0.1, 0.2, 0.3]);
	}

	#[test]
	fn deserialize_empty_response() {
		let json = r#"{"object": "list", "data": []}"#;
		let response: EmbeddingResponse = serde_json::from_str(json).unwrap();
		assert!(response.data.is_empty());
	}

	#[tokio::test]
	async fn embed_returns_vector_from_mock_api() {
		use wiremock::matchers::{header, method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"object": "list",
			"data": [{
				"object": "embedding",
				"embedding": [0.1, 0.2, 0.3, 0.4, 0.5],
				"index": 0
			}],
			"model": "text-embedding-3-small",
			"usage": { "prompt_tokens": 2, "total_tokens": 2 }
		});

		Mock::given(method("POST"))
			.and(path("/embeddings"))
			.and(header("Authorization", "Bearer test-key"))
			.and(header("Content-Type", "application/json"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		let provider = OpenAiProvider::new("test-key".into(), server.uri());
		let result = provider.embed("text-embedding-3-small", "hello world").await;

		let embedding = result.expect("embed should succeed with mock server");
		assert_eq!(embedding, vec![0.1, 0.2, 0.3, 0.4, 0.5]);

		server.verify().await;
	}

	#[tokio::test]
	async fn embed_returns_error_on_401() {
		use wiremock::matchers::method;
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		Mock::given(method("POST"))
			.respond_with(
				ResponseTemplate::new(401).set_body_string(r#"{"error":"invalid api key"}"#),
			)
			.expect(1)
			.mount(&server)
			.await;

		let provider = OpenAiProvider::new("bad-key".into(), server.uri());
		let result = provider.embed("text-embedding-3-small", "hello").await;

		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("401"), "Expected 401 in error: {err_msg}");

		server.verify().await;
	}

	#[tokio::test]
	async fn embed_returns_error_on_empty_data() {
		use wiremock::matchers::method;
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"object": "list",
			"data": [],
			"model": "text-embedding-3-small"
		});

		Mock::given(method("POST"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		let provider = OpenAiProvider::new("test-key".into(), server.uri());
		let result = provider.embed("text-embedding-3-small", "hello").await;

		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("no data"), "Expected 'no data' in error: {err_msg}");

		server.verify().await;
	}

	#[test]
	fn chat_completions_url_default() {
		let provider = OpenAiProvider::new("test".into(), DEFAULT_BASE_URL.into());
		assert_eq!(
			provider.chat_completions_url(),
			"https://api.openai.com/v1/chat/completions"
		);
	}

	#[test]
	fn chat_completions_url_custom_with_trailing_slash() {
		let provider =
			OpenAiProvider::new("test".into(), "https://custom.example.com/v1/".into());
		assert_eq!(
			provider.chat_completions_url(),
			"https://custom.example.com/v1/chat/completions"
		);
	}

	#[test]
	fn deserialize_chat_completion_response() {
		let json = r#"{
			"id": "chatcmpl-abc123",
			"object": "chat.completion",
			"choices": [
				{
					"index": 0,
					"message": {
						"role": "assistant",
						"content": "Hello there!"
					},
					"finish_reason": "stop"
				}
			],
			"usage": {
				"prompt_tokens": 5,
				"completion_tokens": 3,
				"total_tokens": 8
			}
		}"#;

		let response: ChatCompletionResponse = serde_json::from_str(json).unwrap();
		assert_eq!(response.choices.len(), 1);
		assert_eq!(response.choices[0].message.content.as_deref(), Some("Hello there!"));
	}

	#[test]
	fn deserialize_chat_completion_empty_choices() {
		let json = r#"{"id": "chatcmpl-abc", "object": "chat.completion", "choices": []}"#;
		let response: ChatCompletionResponse = serde_json::from_str(json).unwrap();
		assert!(response.choices.is_empty());
	}

	#[tokio::test]
	async fn generate_returns_text_from_mock_api() {
		use wiremock::matchers::{header, method, path};
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
				"prompt_tokens": 10,
				"completion_tokens": 7,
				"total_tokens": 17
			}
		});

		Mock::given(method("POST"))
			.and(path("/chat/completions"))
			.and(header("Authorization", "Bearer test-key"))
			.and(header("Content-Type", "application/json"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		let provider = OpenAiProvider::new("test-key".into(), server.uri());
		let config = GenerationConfig::default();
		let result = provider.generate("gpt-4-turbo", "What is SurrealDB?", &config).await;

		let text = result.expect("generate should succeed with mock server");
		assert_eq!(text, "SurrealDB is a multi-model database.");

		server.verify().await;
	}

	#[tokio::test]
	async fn generate_returns_error_on_401() {
		use wiremock::matchers::method;
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		Mock::given(method("POST"))
			.respond_with(
				ResponseTemplate::new(401).set_body_string(r#"{"error":"invalid api key"}"#),
			)
			.expect(1)
			.mount(&server)
			.await;

		let provider = OpenAiProvider::new("bad-key".into(), server.uri());
		let config = GenerationConfig::default();
		let result = provider.generate("gpt-4-turbo", "Hello", &config).await;

		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("401"), "Expected 401 in error: {err_msg}");

		server.verify().await;
	}

	#[tokio::test]
	async fn generate_returns_error_on_empty_choices() {
		use wiremock::matchers::method;
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"id": "chatcmpl-abc",
			"object": "chat.completion",
			"choices": []
		});

		Mock::given(method("POST"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		let provider = OpenAiProvider::new("test-key".into(), server.uri());
		let config = GenerationConfig::default();
		let result = provider.generate("gpt-4-turbo", "Hello", &config).await;

		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(
			err_msg.contains("no choices"),
			"Expected 'no choices' in error: {err_msg}"
		);

		server.verify().await;
	}
}
