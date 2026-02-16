//! SurrealQL `ai::*` function implementations.
use anyhow::Result;

use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::val::Value;

/// Generate an embedding vector for a text input using a provider-prefixed model.
///
/// # SurrealQL
///
/// ```surql
/// ai::embed('openai:text-embedding-3-small', 'hello world')
/// ai::embed('huggingface:BAAI/bge-small-en-v1.5', 'hello world')
/// ```
///
/// Returns an `array<float>` containing the embedding vector.
#[cfg(not(feature = "ai"))]
pub async fn embed(
	_: &FrozenContext,
	(_model_id, _input): (String, String),
) -> Result<Value> {
	anyhow::bail!(Error::AiDisabled)
}

/// Generate an embedding vector for a text input using a provider-prefixed model.
#[cfg(feature = "ai")]
pub async fn embed(
	_ctx: &FrozenContext,
	(model_id, input): (String, String),
) -> Result<Value> {
	let embedding = crate::ai::embed::embed(&model_id, &input).await?;
	let array: Vec<Value> = embedding.into_iter().map(Value::from).collect();
	Ok(Value::Array(array.into()))
}
