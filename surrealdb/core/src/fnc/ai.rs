//! SurrealQL `ai::*` function implementations.
use anyhow::Result;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::err::Error;
use crate::fnc::args::Optional;
use crate::val::Value;

/// Load the AI config overlay from the database context, if available.
///
/// When a database has `DEFINE CONFIG AI ON DATABASE`, the stored credentials
/// override environment variables. Returns `None` when no config exists or
/// when the context has no database.
#[cfg(feature = "ai")]
async fn ai_config_overlay(
	ctx: &FrozenContext,
	opt: &Options,
) -> Option<crate::ai::config::AiConfigOverlay> {
	use crate::catalog::providers::DatabaseProvider;

	let (ns, db) = ctx.try_ns_db_ids(opt).await.ok().flatten()?;
	let txn = ctx.tx();
	let config = txn.get_db_config(ns, db, "ai").await.ok().flatten()?;
	let catalog_ai = config.try_as_ai().ok()?;
	Some(crate::ai::config::AiConfigOverlay {
		openai_api_key: catalog_ai.openai_api_key.clone(),
		openai_base_url: catalog_ai.openai_base_url.clone(),
		anthropic_api_key: catalog_ai.anthropic_api_key.clone(),
		anthropic_base_url: catalog_ai.anthropic_base_url.clone(),
		google_api_key: catalog_ai.google_api_key.clone(),
		google_base_url: catalog_ai.google_base_url.clone(),
		voyage_api_key: catalog_ai.voyage_api_key.clone(),
		voyage_base_url: catalog_ai.voyage_base_url.clone(),
		huggingface_api_key: catalog_ai.huggingface_api_key.clone(),
		huggingface_base_url: catalog_ai.huggingface_base_url.clone(),
	})
}

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
	_: (&FrozenContext, &Options),
	(_model_id, _input): (String, String),
) -> Result<Value> {
	anyhow::bail!(Error::AiDisabled)
}

/// Generate an embedding vector for a text input using a provider-prefixed model.
#[cfg(feature = "ai")]
pub async fn embed(
	(ctx, opt): (&FrozenContext, &Options),
	(model_id, input): (String, String),
) -> Result<Value> {
	let ai_config = ai_config_overlay(ctx, opt).await;
	let embedding = crate::ai::embed::embed(&model_id, &input, ai_config.as_ref()).await?;
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
	_: (&FrozenContext, &Options),
	(_model_id, _prompt, _config): (String, String, Optional<Value>),
) -> Result<Value> {
	anyhow::bail!(Error::AiDisabled)
}

/// Generate text from a prompt using a provider-prefixed model.
#[cfg(feature = "ai")]
pub async fn generate(
	(ctx, opt): (&FrozenContext, &Options),
	(model_id, prompt, config): (String, String, Optional<Value>),
) -> Result<Value> {
	let ai_config = ai_config_overlay(ctx, opt).await;
	let config = parse_generation_config("ai::generate", config.0)?;
	let text =
		crate::ai::generate::generate(&model_id, &prompt, &config, ai_config.as_ref()).await?;
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
	_: (&FrozenContext, &Options),
	(_model_id, _messages, _config): (String, Value, Optional<Value>),
) -> Result<Value> {
	anyhow::bail!(Error::AiDisabled)
}

/// Conduct a multi-turn chat conversation using a provider-prefixed model.
#[cfg(feature = "ai")]
pub async fn chat(
	(ctx, opt): (&FrozenContext, &Options),
	(model_id, messages, config): (String, Value, Optional<Value>),
) -> Result<Value> {
	let ai_config = ai_config_overlay(ctx, opt).await;
	let messages = parse_chat_messages(&messages)?;
	let config = parse_generation_config("ai::chat", config.0)?;
	let text = crate::ai::chat::chat(&model_id, &messages, &config, ai_config.as_ref()).await?;
	Ok(Value::String(text))
}

/// Analyse the sentiment of a text input using a provider-prefixed model.
///
/// # SurrealQL
///
/// ```surql
/// ai::sentiment('openai:gpt-4-turbo', 'I love this product!')
/// -- Returns: { sentiment: 0.9, summary: 'positive' }
/// ```
///
/// Returns an `object` with `sentiment` (number from -1 to 1) and
/// `summary` (`'positive'`, `'negative'`, or `'mixed'`).
#[cfg(not(feature = "ai"))]
pub async fn sentiment(
	_: (&FrozenContext, &Options),
	(_model_id, _text): (String, String),
) -> Result<Value> {
	anyhow::bail!(Error::AiDisabled)
}

/// Analyse the sentiment of a text input using a provider-prefixed model.
#[cfg(feature = "ai")]
pub async fn sentiment(
	(ctx, opt): (&FrozenContext, &Options),
	(model_id, text): (String, String),
) -> Result<Value> {
	let ai_config = ai_config_overlay(ctx, opt).await;
	let prompt = build_sentiment_prompt(&text);
	let config = sentiment_generation_config();
	let raw =
		crate::ai::generate::generate(&model_id, &prompt, &config, ai_config.as_ref()).await?;
	parse_sentiment_response(&raw)
}

/// Build the prompt that instructs the model to return structured sentiment JSON.
#[cfg(feature = "ai")]
pub(crate) fn build_sentiment_prompt(text: &str) -> String {
	format!(
		"Analyze the sentiment of the following text and respond with ONLY a valid JSON object \
		 containing exactly two fields:\n\
		 - \"sentiment\": a number from -1.0 (most negative) to 1.0 (most positive), where 0.0 is neutral\n\
		 - \"summary\": exactly one of \"positive\", \"negative\", or \"mixed\"\n\n\
		 Do not include any other text, markdown formatting, or explanation. Output only the JSON object.\n\n\
		 Text: {text}"
	)
}

/// Return a `GenerationConfig` tuned for deterministic, concise sentiment responses.
#[cfg(feature = "ai")]
pub(crate) fn sentiment_generation_config() -> crate::ai::provider::GenerationConfig {
	crate::ai::provider::GenerationConfig {
		temperature: Some(0.1),
		max_tokens: Some(100),
		top_p: None,
		stop: None,
	}
}

/// Parse the raw model response into a validated sentiment `Value::Object`.
///
/// Returns an error if the response is not valid JSON, is missing required
/// fields, or contains out-of-range / unexpected values.
#[cfg(feature = "ai")]
pub(crate) fn parse_sentiment_response(raw: &str) -> Result<Value> {
	use std::collections::BTreeMap;

	use crate::val::Object;

	let trimmed = raw.trim();

	// Strip markdown code fences if the model wrapped its output.
	let json_str = if trimmed.starts_with("```") {
		let inner = trimmed
			.strip_prefix("```json")
			.or_else(|| trimmed.strip_prefix("```"))
			.unwrap_or(trimmed);
		inner.strip_suffix("```").unwrap_or(inner).trim()
	} else {
		trimmed
	};

	let parsed: serde_json::Value = serde_json::from_str(json_str).map_err(|e| {
		anyhow::anyhow!(Error::InvalidFunctionArguments {
			name: "ai::sentiment".to_owned(),
			message: format!(
				"The model returned a response that is not valid JSON: {e}. Raw response: {raw}"
			),
		})
	})?;

	let obj = parsed.as_object().ok_or_else(|| {
		anyhow::anyhow!(Error::InvalidFunctionArguments {
			name: "ai::sentiment".to_owned(),
			message: format!(
				"Expected a JSON object from the model, got: {}",
				serde_json::to_string(&parsed).unwrap_or_default()
			),
		})
	})?;

	// Extract and validate `sentiment`
	let sentiment_val = obj.get("sentiment").ok_or_else(|| {
		anyhow::anyhow!(Error::InvalidFunctionArguments {
			name: "ai::sentiment".to_owned(),
			message: "The model response is missing the 'sentiment' field".to_owned(),
		})
	})?;

	let sentiment = sentiment_val.as_f64().ok_or_else(|| {
		anyhow::anyhow!(Error::InvalidFunctionArguments {
			name: "ai::sentiment".to_owned(),
			message: format!("The 'sentiment' field must be a number, got: {sentiment_val}"),
		})
	})?;

	if !(-1.0..=1.0).contains(&sentiment) {
		anyhow::bail!(Error::InvalidFunctionArguments {
			name: "ai::sentiment".to_owned(),
			message: format!(
				"The 'sentiment' field must be between -1.0 and 1.0, got: {sentiment}"
			),
		});
	}

	// Extract and validate `summary`
	let summary_val = obj.get("summary").ok_or_else(|| {
		anyhow::anyhow!(Error::InvalidFunctionArguments {
			name: "ai::sentiment".to_owned(),
			message: "The model response is missing the 'summary' field".to_owned(),
		})
	})?;

	let summary = summary_val.as_str().ok_or_else(|| {
		anyhow::anyhow!(Error::InvalidFunctionArguments {
			name: "ai::sentiment".to_owned(),
			message: format!("The 'summary' field must be a string, got: {summary_val}"),
		})
	})?;

	match summary {
		"positive" | "negative" | "mixed" => {}
		other => {
			anyhow::bail!(Error::InvalidFunctionArguments {
				name: "ai::sentiment".to_owned(),
				message: format!(
					"The 'summary' field must be one of 'positive', 'negative', or 'mixed', got: '{other}'"
				),
			});
		}
	}

	// Build the result object
	let mut result = BTreeMap::new();
	result.insert("sentiment".to_string(), Value::from(sentiment));
	result.insert("summary".to_string(), Value::from(summary.to_owned()));
	Ok(Value::Object(Object::from(result)))
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
