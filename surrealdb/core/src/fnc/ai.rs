//! SurrealQL `ai::*` function implementations.
use anyhow::Result;

use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::fnc::args::Optional;
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
pub async fn embed(_: &FrozenContext, (_model_id, _input): (String, String)) -> Result<Value> {
	anyhow::bail!(Error::AiDisabled)
}

/// Generate an embedding vector for a text input using a provider-prefixed model.
#[cfg(feature = "ai")]
pub async fn embed(_ctx: &FrozenContext, (model_id, input): (String, String)) -> Result<Value> {
	let embedding = crate::ai::embed::embed(&model_id, &input).await?;
	let array: Vec<Value> = embedding.into_iter().map(Value::from).collect();
	Ok(Value::Array(array.into()))
}

/// Generate text from a prompt using a provider-prefixed model.
///
/// # SurrealQL
///
/// ```surql
/// ai::generate('openai:gpt-4-turbo', 'What is SurrealDB?')
/// ai::generate('openai:gpt-4-turbo', 'Summarize this', { temperature: 0.7, max_tokens: 500 })
/// ```
///
/// Returns a `string` containing the generated text.
#[cfg(not(feature = "ai"))]
pub async fn generate(
	_: &FrozenContext,
	(_model_id, _prompt, _config): (String, String, Optional<Value>),
) -> Result<Value> {
	anyhow::bail!(Error::AiDisabled)
}

/// Generate text from a prompt using a provider-prefixed model.
#[cfg(feature = "ai")]
pub async fn generate(
	_ctx: &FrozenContext,
	(model_id, prompt, config): (String, String, Optional<Value>),
) -> Result<Value> {
	let config = parse_generation_config("ai::generate", config.0)?;
	let text = crate::ai::generate::generate(&model_id, &prompt, &config).await?;
	Ok(Value::String(text))
}

/// Conduct a multi-turn chat conversation using a provider-prefixed model.
///
/// # SurrealQL
///
/// ```surql
/// ai::chat('openai:gpt-4-turbo', [
///     { role: 'system', content: 'You are a helpful assistant.' },
///     { role: 'user', content: 'What is SurrealDB?' }
/// ])
/// ai::chat('openai:gpt-4-turbo', [
///     { role: 'user', content: 'Hello' }
/// ], { temperature: 0.7, max_tokens: 500 })
/// ```
///
/// Returns a `string` containing the assistant's response.
#[cfg(not(feature = "ai"))]
pub async fn chat(
	_: &FrozenContext,
	(_model_id, _messages, _config): (String, Value, Optional<Value>),
) -> Result<Value> {
	anyhow::bail!(Error::AiDisabled)
}

/// Conduct a multi-turn chat conversation using a provider-prefixed model.
#[cfg(feature = "ai")]
pub async fn chat(
	_ctx: &FrozenContext,
	(model_id, messages, config): (String, Value, Optional<Value>),
) -> Result<Value> {
	let messages = parse_chat_messages(&messages)?;
	let config = parse_generation_config("ai::chat", config.0)?;
	let text = crate::ai::chat::chat(&model_id, &messages, &config).await?;
	Ok(Value::String(text))
}

/// Parse a SurrealQL array value into a `Vec<ChatMessage>`.
///
/// Each element must be an object with `role` (string) and `content` (string) fields.
#[cfg(feature = "ai")]
fn parse_chat_messages(value: &Value) -> Result<Vec<crate::ai::provider::ChatMessage>> {
	use crate::ai::provider::ChatMessage;

	let arr = match value {
		Value::Array(arr) => arr,
		v => {
			anyhow::bail!(Error::InvalidFunctionArguments {
				name: "ai::chat".to_owned(),
				message: format!(
					"The messages argument must be an array of objects, got: {}",
					v.kind_of()
				),
			})
		}
	};

	if arr.is_empty() {
		anyhow::bail!(Error::InvalidFunctionArguments {
			name: "ai::chat".to_owned(),
			message: "The messages array must not be empty".to_owned(),
		});
	}

	let mut messages = Vec::with_capacity(arr.len());
	for (i, item) in arr.iter().enumerate() {
		let obj = match item {
			Value::Object(obj) => obj,
			v => {
				anyhow::bail!(Error::InvalidFunctionArguments {
					name: "ai::chat".to_owned(),
					message: format!(
						"Message at index {i} must be an object, got: {}",
						v.kind_of()
					),
				})
			}
		};

		let role = match obj.get("role") {
			Some(Value::String(s)) => s.clone(),
			Some(v) => {
				anyhow::bail!(Error::InvalidFunctionArguments {
					name: "ai::chat".to_owned(),
					message: format!(
						"Message at index {i}: 'role' must be a string, got: {}",
						v.kind_of()
					),
				})
			}
			None => {
				anyhow::bail!(Error::InvalidFunctionArguments {
					name: "ai::chat".to_owned(),
					message: format!("Message at index {i} is missing the 'role' field"),
				})
			}
		};

		let content = match obj.get("content") {
			Some(Value::String(s)) => s.clone(),
			Some(v) => {
				anyhow::bail!(Error::InvalidFunctionArguments {
					name: "ai::chat".to_owned(),
					message: format!(
						"Message at index {i}: 'content' must be a string, got: {}",
						v.kind_of()
					),
				})
			}
			None => {
				anyhow::bail!(Error::InvalidFunctionArguments {
					name: "ai::chat".to_owned(),
					message: format!("Message at index {i} is missing the 'content' field"),
				})
			}
		};

		messages.push(ChatMessage::text(role, content));
	}

	Ok(messages)
}

/// Parse an optional SurrealQL object value into a `GenerationConfig`.
#[cfg(feature = "ai")]
fn parse_generation_config(
	fn_name: &str,
	value: Option<Value>,
) -> Result<crate::ai::provider::GenerationConfig> {
	use crate::ai::provider::GenerationConfig;

	match value {
		None => Ok(GenerationConfig::default()),
		Some(Value::Object(obj)) => {
			let temperature = match obj.get("temperature") {
				Some(Value::Number(n)) => Some(n.as_float()),
				Some(_) => {
					anyhow::bail!(Error::InvalidFunctionArguments {
						name: fn_name.to_owned(),
						message: "The 'temperature' config field must be a number".to_owned(),
					})
				}
				None => None,
			};

			let max_tokens = match obj.get("max_tokens") {
				Some(Value::Number(n)) => Some(n.as_int() as u64),
				Some(_) => {
					anyhow::bail!(Error::InvalidFunctionArguments {
						name: fn_name.to_owned(),
						message: "The 'max_tokens' config field must be a number".to_owned(),
					})
				}
				None => None,
			};

			let top_p = match obj.get("top_p") {
				Some(Value::Number(n)) => Some(n.as_float()),
				Some(_) => {
					anyhow::bail!(Error::InvalidFunctionArguments {
						name: fn_name.to_owned(),
						message: "The 'top_p' config field must be a number".to_owned(),
					})
				}
				None => None,
			};

			let stop = match obj.get("stop") {
				Some(Value::Array(arr)) => {
					let mut stops = Vec::new();
					for v in arr.iter() {
						match v {
							Value::String(s) => stops.push(s.clone()),
							_ => {
								anyhow::bail!(Error::InvalidFunctionArguments {
									name: fn_name.to_owned(),
									message: "The 'stop' config field must be an array of strings"
										.to_owned(),
								})
							}
						}
					}
					Some(stops)
				}
				Some(_) => {
					anyhow::bail!(Error::InvalidFunctionArguments {
						name: fn_name.to_owned(),
						message: "The 'stop' config field must be an array of strings".to_owned(),
					})
				}
				None => None,
			};

			Ok(GenerationConfig {
				temperature,
				max_tokens,
				top_p,
				stop,
			})
		}
		Some(v) => {
			anyhow::bail!(Error::InvalidFunctionArguments {
				name: fn_name.to_owned(),
				message: format!("The config argument must be an object, got: {}", v.kind_of()),
			})
		}
	}
}
