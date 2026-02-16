//! HuggingFace embedding provider.
//!
//! Calls the HuggingFace Inference API to generate embeddings using any
//! sentence-transformer or embedding model hosted on the Hub.
//!
//! # Configuration
//!
//! - `SURREAL_AI_HUGGINGFACE_API_KEY` — Required. A HuggingFace API token.
//! - `SURREAL_AI_HUGGINGFACE_BASE_URL` — Optional. Defaults to
//!   `https://api-inference.huggingface.co/pipeline/feature-extraction`.
//!
//! # Usage
//!
//! ```sql
//! ai::embed('huggingface:BAAI/bge-small-en-v1.5', 'hello world')
//! ai::embed('huggingface:sentence-transformers/all-MiniLM-L6-v2', 'hello world')
//! ```
use anyhow::{Result, bail};
use serde::Serialize;

use crate::ai::provider::EmbeddingProvider;

const DEFAULT_BASE_URL: &str = "https://api-inference.huggingface.co/pipeline/feature-extraction";

/// An embedding provider that calls the HuggingFace Inference API.
pub struct HuggingFaceProvider {
	api_key: String,
	base_url: String,
}

impl HuggingFaceProvider {
	/// Create a new provider with explicit configuration.
	#[cfg(test)]
	pub fn with_config(api_key: String, base_url: String) -> Self {
		Self {
			api_key,
			base_url,
		}
	}

	/// Create a new provider, reading credentials from environment variables.
	pub fn new() -> Self {
		let api_key =
			std::env::var("SURREAL_AI_HUGGINGFACE_API_KEY").unwrap_or_else(|_| String::new());

		let base_url = std::env::var("SURREAL_AI_HUGGINGFACE_BASE_URL")
			.unwrap_or_else(|_| DEFAULT_BASE_URL.to_owned());

		Self {
			api_key,
			base_url,
		}
	}

	/// Build the full URL for a model's feature-extraction endpoint.
	fn model_url(&self, model: &str) -> String {
		let base = self.base_url.trim_end_matches('/');
		format!("{base}/{model}")
	}
}

/// Request body for the HuggingFace Inference API.
#[derive(Serialize)]
struct InferenceRequest<'a> {
	inputs: &'a str,
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl EmbeddingProvider for HuggingFaceProvider {
	async fn embed(&self, model: &str, input: &str) -> Result<Vec<f64>> {
		let url = self.model_url(model);

		let body = InferenceRequest {
			inputs: input,
		};

		let mut request = reqwest::Client::new()
			.post(&url)
			.header("Content-Type", "application/json")
			.json(&body);

		if !self.api_key.is_empty() {
			request = request.header("Authorization", format!("Bearer {}", self.api_key));
		}

		let response = request
			.send()
			.await
			.map_err(|e| anyhow::anyhow!("Failed to call HuggingFace Inference API: {e}"))?;

		if !response.status().is_success() {
			let status = response.status();
			let body = response.text().await.unwrap_or_default();
			bail!("HuggingFace Inference API returned {status}: {body}");
		}

		// The feature-extraction pipeline returns a nested array.
		// For a single input string, the response is typically [[f64, f64, ...]]
		// (one embedding per token) or [f64, f64, ...] (mean-pooled).
		// We try to handle both formats.
		let text = response.text().await.map_err(|e| {
			anyhow::anyhow!("Failed to read HuggingFace Inference API response: {e}")
		})?;

		// Try parsing as a flat vector first (mean-pooled output)
		if let Ok(embedding) = serde_json::from_str::<Vec<f64>>(&text) {
			return Ok(embedding);
		}

		// Try parsing as a nested array (token-level embeddings)
		// Take the first token's embedding or mean-pool across tokens
		if let Ok(embeddings) = serde_json::from_str::<Vec<Vec<f64>>>(&text) {
			if embeddings.is_empty() {
				bail!("HuggingFace Inference API returned empty embeddings");
			}
			// Mean-pool across token embeddings
			let dim = embeddings[0].len();
			let n = embeddings.len() as f64;
			let mut pooled = vec![0.0f64; dim];
			for token_embedding in &embeddings {
				for (i, &val) in token_embedding.iter().enumerate() {
					if i < dim {
						pooled[i] += val / n;
					}
				}
			}
			return Ok(pooled);
		}

		bail!(
			"Failed to parse HuggingFace Inference API response as embeddings. \
			 Expected a JSON array of numbers."
		)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn model_url_default() {
		let provider = HuggingFaceProvider::with_config(String::new(), DEFAULT_BASE_URL.into());
		assert_eq!(
			provider.model_url("BAAI/bge-small-en-v1.5"),
			"https://api-inference.huggingface.co/pipeline/feature-extraction/BAAI/bge-small-en-v1.5"
		);
	}

	#[test]
	fn model_url_custom() {
		let provider =
			HuggingFaceProvider::with_config(String::new(), "https://custom.example.com/v1/".into());
		assert_eq!(
			provider.model_url("my-model"),
			"https://custom.example.com/v1/my-model"
		);
	}

	#[test]
	fn parse_flat_embedding() {
		let json = "[0.1, 0.2, 0.3, 0.4]";
		let result: Vec<f64> = serde_json::from_str(json).unwrap();
		assert_eq!(result, vec![0.1, 0.2, 0.3, 0.4]);
	}

	#[test]
	fn parse_nested_embedding() {
		let json = "[[0.1, 0.2], [0.3, 0.4]]";
		let result: Vec<Vec<f64>> = serde_json::from_str(json).unwrap();
		assert_eq!(result.len(), 2);
		assert_eq!(result[0], vec![0.1, 0.2]);
	}

	#[tokio::test]
	async fn embed_flat_response() {
		use wiremock::matchers::{method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		Mock::given(method("POST"))
			.and(path("/my-model"))
			.respond_with(
				ResponseTemplate::new(200).set_body_string("[0.1, 0.2, 0.3, 0.4, 0.5]"),
			)
			.expect(1)
			.mount(&server)
			.await;

		let provider = HuggingFaceProvider::with_config("test-token".into(), server.uri());
		let result = provider.embed("my-model", "hello world").await;

		let embedding = result.expect("embed should succeed with flat response");
		assert_eq!(embedding, vec![0.1, 0.2, 0.3, 0.4, 0.5]);

		server.verify().await;
	}

	#[tokio::test]
	async fn embed_nested_response_mean_pools() {
		use wiremock::matchers::method;
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		// Simulate token-level embeddings: 2 tokens, 3 dimensions
		Mock::given(method("POST"))
			.respond_with(
				ResponseTemplate::new(200).set_body_string("[[1.0, 2.0, 3.0], [3.0, 4.0, 5.0]]"),
			)
			.expect(1)
			.mount(&server)
			.await;

		let provider = HuggingFaceProvider::with_config("test-token".into(), server.uri());
		let result = provider.embed("test-model", "hello").await;

		let embedding = result.expect("embed should succeed with nested response");
		// Mean pool: [(1+3)/2, (2+4)/2, (3+5)/2] = [2.0, 3.0, 4.0]
		assert_eq!(embedding, vec![2.0, 3.0, 4.0]);

		server.verify().await;
	}

	#[tokio::test]
	async fn embed_returns_error_on_500() {
		use wiremock::matchers::method;
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		Mock::given(method("POST"))
			.respond_with(
				ResponseTemplate::new(500).set_body_string(r#"{"error":"model is loading"}"#),
			)
			.expect(1)
			.mount(&server)
			.await;

		let provider = HuggingFaceProvider::with_config("test-token".into(), server.uri());
		let result = provider.embed("test-model", "hello").await;

		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(err_msg.contains("500"), "Expected 500 in error: {err_msg}");

		server.verify().await;
	}

	#[tokio::test]
	async fn embed_returns_error_on_invalid_json() {
		use wiremock::matchers::method;
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		Mock::given(method("POST"))
			.respond_with(ResponseTemplate::new(200).set_body_string(r#"{"not": "an array"}"#))
			.expect(1)
			.mount(&server)
			.await;

		let provider = HuggingFaceProvider::with_config("test-token".into(), server.uri());
		let result = provider.embed("test-model", "hello").await;

		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(
			err_msg.contains("Failed to parse"),
			"Expected parse error in error: {err_msg}"
		);

		server.verify().await;
	}
}
