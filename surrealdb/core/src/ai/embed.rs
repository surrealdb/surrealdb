//! Core embedding logic: provider prefix parsing, routing, and result conversion.
use anyhow::Result;

use crate::err::Error;

use super::provider::EmbeddingProvider;
use super::providers::huggingface::HuggingFaceProvider;
use super::providers::openai::OpenAiProvider;

/// Parse a model identifier into `(provider, model_name)`.
///
/// Model identifiers use the format `provider:model_name`, e.g.:
/// - `openai:text-embedding-3-small`
/// - `huggingface:BAAI/bge-small-en-v1.5`
fn parse_model_id(model_id: &str) -> Result<(&str, &str)> {
	model_id.split_once(':').ok_or_else(|| {
		anyhow::Error::new(Error::InvalidFunctionArguments {
			name: "ai::embed".to_owned(),
			message: format!(
				"Model ID must be in 'provider:model' format \
				 (e.g. 'openai:text-embedding-3-small'), got: '{model_id}'"
			),
		})
	})
}

/// Generate an embedding for the given text using the specified model.
///
/// Routes to the appropriate provider based on the model identifier prefix.
pub async fn embed(model_id: &str, input: &str) -> Result<Vec<f64>> {
	let (provider_name, model_name) = parse_model_id(model_id)?;

	match provider_name {
		"openai" => {
			let provider = OpenAiProvider::from_env()?;
			provider.embed(model_name, input).await
		}
		"huggingface" => {
			let provider = HuggingFaceProvider::new();
			provider.embed(model_name, input).await
		}
		other => Err(anyhow::Error::new(Error::InvalidFunctionArguments {
			name: "ai::embed".to_owned(),
			message: format!(
				"Unknown provider '{other}'. Supported providers: 'openai', 'huggingface'"
			),
		})),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_model_id_valid_openai() {
		let (provider, model) = parse_model_id("openai:text-embedding-3-small").unwrap();
		assert_eq!(provider, "openai");
		assert_eq!(model, "text-embedding-3-small");
	}

	#[test]
	fn parse_model_id_valid_huggingface() {
		let (provider, model) = parse_model_id("huggingface:BAAI/bge-small-en-v1.5").unwrap();
		assert_eq!(provider, "huggingface");
		assert_eq!(model, "BAAI/bge-small-en-v1.5");
	}

	#[test]
	fn parse_model_id_missing_prefix() {
		let result = parse_model_id("text-embedding-3-small");
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
	async fn embed_unknown_provider_returns_error() {
		let result = embed("badprovider:some-model", "hello").await;
		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(
			err_msg.contains("Unknown provider"),
			"Expected 'Unknown provider' in error: {err_msg}"
		);
	}

	#[tokio::test]
	async fn embed_missing_prefix_returns_error() {
		let result = embed("just-a-model-name", "hello").await;
		assert!(result.is_err());
		let err_msg = result.unwrap_err().to_string();
		assert!(
			err_msg.contains("provider:model"),
			"Expected format hint in error: {err_msg}"
		);
	}

	#[tokio::test]
	async fn embed_openai_via_mock() {
		use wiremock::matchers::{method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

		let server = MockServer::start().await;

		let response_body = serde_json::json!({
			"object": "list",
			"data": [{
				"object": "embedding",
				"embedding": [0.1, 0.2, 0.3],
				"index": 0
			}],
			"model": "text-embedding-3-small",
			"usage": { "prompt_tokens": 2, "total_tokens": 2 }
		});

		Mock::given(method("POST"))
			.and(path("/embeddings"))
			.respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
			.expect(1)
			.mount(&server)
			.await;

		// Point the OpenAI provider at the mock server via env vars
		std::env::set_var("SURREAL_AI_OPENAI_API_KEY", "test-key");
		std::env::set_var("SURREAL_AI_OPENAI_BASE_URL", &server.uri());

		let result = embed("openai:text-embedding-3-small", "hello world").await;

		// Clean up env vars
		std::env::remove_var("SURREAL_AI_OPENAI_API_KEY");
		std::env::remove_var("SURREAL_AI_OPENAI_BASE_URL");

		let embedding = result.expect("embed should succeed via mock");
		assert_eq!(embedding, vec![0.1, 0.2, 0.3]);

		server.verify().await;
	}
}
