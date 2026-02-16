//! Defines the `EmbeddingProvider` trait that all embedding backends implement.
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
