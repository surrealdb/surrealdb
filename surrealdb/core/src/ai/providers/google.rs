//! Google Gemini AI embedding and generation provider.
//!
//! Calls the Google Generative Language API which uses a different format
//! from OpenAI-compatible providers.
//!
//! The `google:` prefix is the canonical name. `gemini:` is accepted as an alias.
//!
//! # Configuration
//!
//! - `SURREAL_AI_GOOGLE_API_KEY` — Required. A Google AI API key.
//! - `SURREAL_AI_GOOGLE_BASE_URL` — Optional. Defaults to
//!   `https://generativelanguage.googleapis.com/v1beta`.
//!
//! # Usage
//!
//! ```sql
//! ai::embed('google:text-embedding-005', 'hello world')
//! ai::embed('gemini:gemini-embedding-001', 'hello world') -- alias
//! ai::generate('google:gemini-2.0-flash', 'What is SurrealDB?')
//! ```
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::ai::provider::{EmbeddingProvider, GenerationConfig, GenerationProvider};

const DEFAULT_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";

/// An AI provider that calls the Google Generative Language API.
pub struct GoogleProvider {
	api_key: String,
	base_url: String,
}

impl GoogleProvider {
	/// Create a new provider with explicit configuration.
	#[cfg(test)]
	pub fn new(api_key: String, base_url: String) -> Self {
		Self {
			api_key,
			base_url,
		}
	}

	/// Create a new provider from environment variables.
	///
	/// Reads `SURREAL_AI_GOOGLE_API_KEY` (required) and
	/// `SURREAL_AI_GOOGLE_BASE_URL` (optional, defaults to Google).
	pub fn from_env() -> Result<Self> {
		let api_key = std::env::var("SURREAL_AI_GOOGLE_API_KEY").map_err(|_| {
			anyhow::anyhow!(
				"SURREAL_AI_GOOGLE_API_KEY environment variable is not set. \
				 Set it to your Google AI API key to use 'google:' or 'gemini:' models."
			)
		})?;

		let base_url = std::env::var("SURREAL_AI_GOOGLE_BASE_URL")
			.unwrap_or_else(|_| DEFAULT_BASE_URL.to_owned());

		Ok(Self {
			api_key,
			base_url,
		})
	}

	/// Build the URL for the embedContent endpoint.
	fn embed_url(&self, model: &str) -> String {
		let base = self.base_url.trim_end_matches('/');
		format!("{base}/models/{model}:embedContent?key={}", self.api_key)
	}

	/// Build the URL for the generateContent endpoint.
	fn generate_url(&self, model: &str) -> String {
		let base = self.base_url.trim_end_matches('/');
		format!("{base}/models/{model}:generateContent?key={}", self.api_key)
	}
}

// =========================================================================
// Embedding types
// =========================================================================

/// Request body for Google embedContent API.
#[derive(Serialize)]
struct EmbedContentRequest<'a> {
	content: ContentBody<'a>,
}

/// Content wrapper used by both embedding and generation requests.
#[derive(Serialize)]
struct ContentBody<'a> {
	parts: Vec<PartBody<'a>>,
}

/// A single part within a content body.
#[derive(Serialize)]
struct PartBody<'a> {
	text: &'a str,
}

/// Response from the Google embedContent API.
#[derive(Deserialize)]
struct EmbedContentResponse {
	embedding: EmbeddingValues,
}

/// The embedding values within the response.
#[derive(Deserialize)]
struct EmbeddingValues {
	values: Vec<f64>,
}

// =========================================================================
// Generation types
// =========================================================================

/// Request body for Google generateContent API.
#[derive(Serialize)]
struct GenerateContentRequest<'a> {
	contents: Vec<GenerateContentBody<'a>>,
	#[serde(skip_serializing_if = "Option::is_none", rename = "generationConfig")]
	generation_config: Option<GoogleGenerationConfig>,
}

/// A single content entry in the generation request.
#[derive(Serialize)]
struct GenerateContentBody<'a> {
	parts: Vec<PartBody<'a>>,
}

/// Google-specific generation configuration.
#[derive(Serialize)]
struct GoogleGenerationConfig {
	#[serde(skip_serializing_if = "Option::is_none")]
	temperature: Option<f64>,
	#[serde(skip_serializing_if = "Option::is_none", rename = "maxOutputTokens")]
	max_output_tokens: Option<u64>,
	#[serde(skip_serializing_if = "Option::is_none", rename = "topP")]
	top_p: Option<f64>,
	#[serde(skip_serializing_if = "Option::is_none", rename = "stopSequences")]
	stop_sequences: Option<Vec<String>>,
}

/// Response from the Google generateContent API.
#[derive(Deserialize)]
struct GenerateContentResponse {
	candidates: Vec<Candidate>,
}

/// A single candidate in the generation response.
#[derive(Deserialize)]
struct Candidate {
	content: CandidateContent,
}

/// Content within a candidate.
#[derive(Deserialize)]
struct CandidateContent {
	parts: Vec<CandidatePart>,
}

/// A single part within candidate content.
#[derive(Deserialize)]
struct CandidatePart {
	text: Option<String>,
}

// =========================================================================
// Trait implementations
// =========================================================================

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl EmbeddingProvider for GoogleProvider {
	async fn embed(&self, model: &str, input: &str) -> Result<Vec<f64>> {
		let url = self.embed_url(model);

		let body = EmbedContentRequest {
			content: ContentBody {
				parts: vec![PartBody {
					text: input,
				}],
			},
		};

		let client = reqwest::Client::new();
		let response = client
			.post(&url)
			.header("Content-Type", "application/json")
			.json(&body)
			.send()
			.await
			.map_err(|e| anyhow::anyhow!("Failed to call Google embedContent API: {e}"))?;

		if !response.status().is_success() {
			let status = response.status();
			let body = response.text().await.unwrap_or_default();
			bail!("Google embedContent API returned {status}: {body}");
		}

		let result: EmbedContentResponse = response
			.json()
			.await
			.map_err(|e| anyhow::anyhow!("Failed to parse Google embedContent response: {e}"))?;

		Ok(result.embedding.values)
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl GenerationProvider for GoogleProvider {
	async fn generate(
		&self,
		model: &str,
		prompt: &str,
		config: &GenerationConfig,
	) -> Result<String> {
		let url = self.generate_url(model);

		let generation_config =
			if config.temperature.is_some()
				|| config.max_tokens.is_some()
				|| config.top_p.is_some()
				|| config.stop.is_some()
			{
				Some(GoogleGenerationConfig {
					temperature: config.temperature,
					max_output_tokens: config.max_tokens,
					top_p: config.top_p,
					stop_sequences: config.stop.clone(),
				})
			} else {
				None
			};

		let body = GenerateContentRequest {
			contents: vec![GenerateContentBody {
				parts: vec![PartBody {
					text: prompt,
				}],
			}],
			generation_config,
		};

		let client = reqwest::Client::new();
		let response = client
			.post(&url)
			.header("Content-Type", "application/json")
			.json(&body)
			.send()
			.await
			.map_err(|e| anyhow::anyhow!("Failed to call Google generateContent API: {e}"))?;

		if !response.status().is_success() {
			let status = response.status();
			let body = response.text().await.unwrap_or_default();
			bail!("Google generateContent API returned {status}: {body}");
		}

		let result: GenerateContentResponse = response.json().await.map_err(|e| {
			anyhow::anyhow!("Failed to parse Google generateContent response: {e}")
		})?;

		let candidate = result
			.candidates
			.into_iter()
			.next()
			.ok_or_else(|| anyhow::anyhow!("Google generateContent response contained no candidates"))?;

		let text = candidate
			.content
			.parts
			.into_iter()
			.filter_map(|p| p.text)
			.collect::<Vec<_>>()
			.join("");

		if text.is_empty() {
			bail!("Google generateContent response contained no text");
		}

		Ok(text)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn embed_url_default() {
		let provider = GoogleProvider::new("test-key".into(), DEFAULT_BASE_URL.into());
		assert_eq!(
			provider.embed_url("text-embedding-005"),
			"https://generativelanguage.googleapis.com/v1beta/models/text-embedding-005:embedContent?key=test-key"
		);
	}

	#[test]
	fn embed_url_custom_with_trailing_slash() {
		let provider =
			GoogleProvider::new("test-key".into(), "https://custom.example.com/v1/".into());
		assert_eq!(
			provider.embed_url("my-model"),
			"https://custom.example.com/v1/models/my-model:embedContent?key=test-key"
		);
	}

	#[test]
	fn generate_url_default() {
		let provider = GoogleProvider::new("test-key".into(), DEFAULT_BASE_URL.into());
		assert_eq!(
			provider.generate_url("gemini-2.0-flash"),
			"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=test-key"
		);
	}

	#[test]
	fn deserialize_embed_response() {
		let json = r#"{"embedding": {"values": [0.1, 0.2, 0.3]}}"#;
		let response: EmbedContentResponse = serde_json::from_str(json).unwrap();
		assert_eq!(response.embedding.values, vec![0.1, 0.2, 0.3]);
	}

	#[test]
	fn deserialize_generate_response() {
		let json = r#"{
			"candidates": [{
				"content": {
					"parts": [{"text": "Hello from Gemini!"}],
					"role": "model"
				},
				"finishReason": "STOP"
			}]
		}"#;
		let response: GenerateContentResponse = serde_json::from_str(json).unwrap();
		assert_eq!(response.candidates.len(), 1);
		assert_eq!(
			response.candidates[0].content.parts[0].text.as_deref(),
			Some("Hello from Gemini!")
		);
	}

	#[tokio::test]
	async fn embed_returns_vector_from_mock_api() {
		use wiremock::matchers::{method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"embedding": {
				"values": [0.1, 0.2, 0.3, 0.4, 0.5]
			}
		});

		Mock::given(method("POST"))
			.and(path("/models/text-embedding-005:embedContent"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		let provider = GoogleProvider::new("test-key".into(), server.uri());
		let result = provider.embed("text-embedding-005", "hello world").await;

		let embedding = result.expect("embed should succeed with mock Google server");
		assert_eq!(embedding, vec![0.1, 0.2, 0.3, 0.4, 0.5]);

		server.verify().await;
	}

	#[tokio::test]
	async fn embed_returns_error_on_400() {
		use wiremock::matchers::method;
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		Mock::given(method("POST"))
			.respond_with(
				ResponseTemplate::new(400)
					.set_body_string(r#"{"error":{"message":"API key not valid"}}"#),
			)
			.expect(1)
			.mount(&server)
			.await;

		let provider = GoogleProvider::new("bad-key".into(), server.uri());
		let result = provider.embed("text-embedding-005", "hello").await;

		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("400"), "Expected 400 in error: {err_msg}");

		server.verify().await;
	}

	#[tokio::test]
	async fn generate_returns_text_from_mock_api() {
		use wiremock::matchers::{method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"candidates": [{
				"content": {
					"parts": [{"text": "SurrealDB is a multi-model database."}],
					"role": "model"
				},
				"finishReason": "STOP"
			}]
		});

		Mock::given(method("POST"))
			.and(path("/models/gemini-2.0-flash:generateContent"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		let provider = GoogleProvider::new("test-key".into(), server.uri());
		let config = GenerationConfig::default();
		let result = provider.generate("gemini-2.0-flash", "What is SurrealDB?", &config).await;

		let text = result.expect("generate should succeed with mock Google server");
		assert_eq!(text, "SurrealDB is a multi-model database.");

		server.verify().await;
	}

	#[tokio::test]
	async fn generate_returns_error_on_empty_candidates() {
		use wiremock::matchers::method;
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"candidates": []
		});

		Mock::given(method("POST"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		let provider = GoogleProvider::new("test-key".into(), server.uri());
		let config = GenerationConfig::default();
		let result = provider.generate("gemini-2.0-flash", "Hello", &config).await;

		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(
			err_msg.contains("no candidates"),
			"Expected 'no candidates' in error: {err_msg}"
		);

		server.verify().await;
	}

	#[tokio::test]
	async fn generate_returns_error_on_403() {
		use wiremock::matchers::method;
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		Mock::given(method("POST"))
			.respond_with(
				ResponseTemplate::new(403)
					.set_body_string(r#"{"error":{"message":"Permission denied"}}"#),
			)
			.expect(1)
			.mount(&server)
			.await;

		let provider = GoogleProvider::new("bad-key".into(), server.uri());
		let config = GenerationConfig::default();
		let result = provider.generate("gemini-2.0-flash", "Hello", &config).await;

		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("403"), "Expected 403 in error: {err_msg}");

		server.verify().await;
	}
}
