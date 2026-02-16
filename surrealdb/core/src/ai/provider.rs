//! Defines the provider traits that all AI backends implement.
use anyhow::Result;

/// A provider that can generate vector embeddings from text input.
///
/// Implementations must be thread-safe (`Send + Sync`) since they may be
/// shared across concurrent SurrealQL queries.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub trait EmbeddingProvider: Send + Sync {
	/// Generate an embedding vector for a single text input.
	///
	/// # Arguments
	/// * `model` — The model name (without the provider prefix).
	/// * `input` — The text to embed.
	///
	/// # Returns
	/// A vector of `f64` values representing the embedding.
	async fn embed(&self, model: &str, input: &str) -> Result<Vec<f64>>;
}

/// Configuration for text generation requests.
#[derive(Debug, Clone, Default)]
pub struct GenerationConfig {
	/// Sampling temperature (0.0 = deterministic, higher = more random).
	pub temperature: Option<f64>,
	/// Maximum number of tokens to generate.
	pub max_tokens: Option<u64>,
	/// Nucleus sampling: only consider tokens with cumulative probability >= top_p.
	pub top_p: Option<f64>,
	/// Stop sequences: generation stops when any of these strings is produced.
	pub stop: Option<Vec<String>>,
}

/// A provider that can generate text from a prompt using an LLM.
///
/// Implementations must be thread-safe (`Send + Sync`) since they may be
/// shared across concurrent SurrealQL queries.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub trait GenerationProvider: Send + Sync {
	/// Generate text from a prompt using the specified model.
	///
	/// # Arguments
	/// * `model` — The model name (without the provider prefix).
	/// * `prompt` — The user prompt to send to the model.
	/// * `config` — Optional generation parameters.
	///
	/// # Returns
	/// The generated text as a `String`.
	async fn generate(
		&self,
		model: &str,
		prompt: &str,
		config: &GenerationConfig,
	) -> Result<String>;
}
