//! AI functions for the streaming executor.
//!
//! These provide AI functionality (embeddings, text generation, etc.).
//! Note: AI functions require the "ai" feature to be enabled.

use anyhow::Result;

use crate::exec::function::FunctionRegistry;
use crate::exec::physical_expr::EvalContext;
use crate::val::Value;
use crate::{define_async_function, register_functions};

// =========================================================================
// Helper functions
// =========================================================================

#[cfg(not(feature = "ai"))]
async fn ai_disabled() -> Result<Value> {
	Err(anyhow::anyhow!(crate::err::Error::AiDisabled))
}

// =========================================================================
// AI Embed
// =========================================================================

#[cfg(feature = "ai")]
async fn ai_embed_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let model_id = match args.first() {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::embed".to_owned(),
				message: format!(
					"The first argument should be a string model ID (e.g. 'openai:text-embedding-3-small'), got: {}",
					v.kind_of()
				),
			}))
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::embed".to_owned(),
				message: "Missing model ID argument".to_string(),
			}))
		}
	};

	let input = match args.get(1) {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::embed".to_owned(),
				message: format!(
					"The second argument should be a string of text to embed, got: {}",
					v.kind_of()
				),
			}))
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::embed".to_owned(),
				message: "Missing text input argument".to_string(),
			}))
		}
	};

	let embedding = crate::ai::embed::embed(&model_id, &input).await?;
	let array: Vec<Value> = embedding.into_iter().map(Value::from).collect();
	Ok(Value::Array(array.into()))
}

#[cfg(not(feature = "ai"))]
async fn ai_embed_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	ai_disabled().await
}

// =========================================================================
// AI Generate
// =========================================================================

#[cfg(feature = "ai")]
async fn ai_generate_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	use crate::ai::provider::GenerationConfig;

	let model_id = match args.first() {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::generate".to_owned(),
				message: format!(
					"The first argument should be a string model ID (e.g. 'openai:gpt-4-turbo'), got: {}",
					v.kind_of()
				),
			}))
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::generate".to_owned(),
				message: "Missing model ID argument".to_string(),
			}))
		}
	};

	let prompt = match args.get(1) {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::generate".to_owned(),
				message: format!(
					"The second argument should be a string prompt, got: {}",
					v.kind_of()
				),
			}))
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::generate".to_owned(),
				message: "Missing prompt argument".to_string(),
			}))
		}
	};

	let config = match args.get(2) {
		Some(Value::Object(obj)) => {
			let temperature = match obj.get("temperature") {
				Some(Value::Number(n)) => Some(n.as_float()),
				Some(_) => {
					return Err(anyhow::anyhow!(
						crate::err::Error::InvalidFunctionArguments {
							name: "ai::generate".to_owned(),
							message: "The 'temperature' config field must be a number".to_owned(),
						}
					))
				}
				None => None,
			};

			let max_tokens = match obj.get("max_tokens") {
				Some(Value::Number(n)) => Some(n.as_int() as u64),
				Some(_) => {
					return Err(anyhow::anyhow!(
						crate::err::Error::InvalidFunctionArguments {
							name: "ai::generate".to_owned(),
							message: "The 'max_tokens' config field must be a number".to_owned(),
						}
					))
				}
				None => None,
			};

			let top_p = match obj.get("top_p") {
				Some(Value::Number(n)) => Some(n.as_float()),
				Some(_) => {
					return Err(anyhow::anyhow!(
						crate::err::Error::InvalidFunctionArguments {
							name: "ai::generate".to_owned(),
							message: "The 'top_p' config field must be a number".to_owned(),
						}
					))
				}
				None => None,
			};

			GenerationConfig {
				temperature,
				max_tokens,
				top_p,
				stop: None,
			}
		}
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::generate".to_owned(),
				message: format!(
					"The config argument must be an object, got: {}",
					v.kind_of()
				),
			}))
		}
		None => GenerationConfig::default(),
	};

	let text = crate::ai::generate::generate(&model_id, &prompt, &config).await?;
	Ok(Value::String(text.into()))
}

#[cfg(not(feature = "ai"))]
async fn ai_generate_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	ai_disabled().await
}

// =========================================================================
// AI Chat
// =========================================================================

#[cfg(feature = "ai")]
async fn ai_chat_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	use crate::ai::provider::{ChatMessage, GenerationConfig};

	let model_id = match args.first() {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chat".to_owned(),
				message: format!(
					"The first argument should be a string model ID (e.g. 'openai:gpt-4-turbo'), got: {}",
					v.kind_of()
				),
			}))
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chat".to_owned(),
				message: "Missing model ID argument".to_string(),
			}))
		}
	};

	let messages = match args.get(1) {
		Some(Value::Array(arr)) => {
			if arr.is_empty() {
				return Err(anyhow::anyhow!(
					crate::err::Error::InvalidFunctionArguments {
						name: "ai::chat".to_owned(),
						message: "The messages array must not be empty".to_owned(),
					}
				));
			}
			let mut msgs = Vec::with_capacity(arr.len());
			for (i, item) in arr.iter().enumerate() {
				let obj = match item {
					Value::Object(obj) => obj,
					v => {
						return Err(anyhow::anyhow!(
							crate::err::Error::InvalidFunctionArguments {
								name: "ai::chat".to_owned(),
								message: format!(
									"Message at index {i} must be an object, got: {}",
									v.kind_of()
								),
							}
						))
					}
				};
				let role = match obj.get("role") {
					Some(Value::String(s)) => s.to_string(),
					Some(v) => {
						return Err(anyhow::anyhow!(
							crate::err::Error::InvalidFunctionArguments {
								name: "ai::chat".to_owned(),
								message: format!(
									"Message at index {i}: 'role' must be a string, got: {}",
									v.kind_of()
								),
							}
						))
					}
					None => {
						return Err(anyhow::anyhow!(
							crate::err::Error::InvalidFunctionArguments {
								name: "ai::chat".to_owned(),
								message: format!(
									"Message at index {i} is missing the 'role' field"
								),
							}
						))
					}
				};
				let content = match obj.get("content") {
					Some(Value::String(s)) => s.to_string(),
					Some(v) => {
						return Err(anyhow::anyhow!(
							crate::err::Error::InvalidFunctionArguments {
								name: "ai::chat".to_owned(),
								message: format!(
									"Message at index {i}: 'content' must be a string, got: {}",
									v.kind_of()
								),
							}
						))
					}
					None => {
						return Err(anyhow::anyhow!(
							crate::err::Error::InvalidFunctionArguments {
								name: "ai::chat".to_owned(),
								message: format!(
									"Message at index {i} is missing the 'content' field"
								),
							}
						))
					}
				};
				msgs.push(ChatMessage {
					role,
					content,
				});
			}
			msgs
		}
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chat".to_owned(),
				message: format!(
					"The second argument should be an array of message objects, got: {}",
					v.kind_of()
				),
			}))
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chat".to_owned(),
				message: "Missing messages argument".to_string(),
			}))
		}
	};

	let config = match args.get(2) {
		Some(Value::Object(obj)) => {
			let temperature = match obj.get("temperature") {
				Some(Value::Number(n)) => Some(n.as_float()),
				Some(_) => {
					return Err(anyhow::anyhow!(
						crate::err::Error::InvalidFunctionArguments {
							name: "ai::chat".to_owned(),
							message: "The 'temperature' config field must be a number".to_owned(),
						}
					))
				}
				None => None,
			};

			let max_tokens = match obj.get("max_tokens") {
				Some(Value::Number(n)) => Some(n.as_int() as u64),
				Some(_) => {
					return Err(anyhow::anyhow!(
						crate::err::Error::InvalidFunctionArguments {
							name: "ai::chat".to_owned(),
							message: "The 'max_tokens' config field must be a number".to_owned(),
						}
					))
				}
				None => None,
			};

			let top_p = match obj.get("top_p") {
				Some(Value::Number(n)) => Some(n.as_float()),
				Some(_) => {
					return Err(anyhow::anyhow!(
						crate::err::Error::InvalidFunctionArguments {
							name: "ai::chat".to_owned(),
							message: "The 'top_p' config field must be a number".to_owned(),
						}
					))
				}
				None => None,
			};

			GenerationConfig {
				temperature,
				max_tokens,
				top_p,
				stop: None,
			}
		}
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chat".to_owned(),
				message: format!(
					"The config argument must be an object, got: {}",
					v.kind_of()
				),
			}))
		}
		None => GenerationConfig::default(),
	};

	let text = crate::ai::chat::chat(&model_id, &messages, &config).await?;
	Ok(Value::String(text.into()))
}

#[cfg(not(feature = "ai"))]
async fn ai_chat_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	ai_disabled().await
}

// =========================================================================
// Function definitions using the macro
// =========================================================================

define_async_function!(AiChat, "ai::chat", (model_id: String, messages: Array, ?config: Object) -> Any, ai_chat_impl);
define_async_function!(AiEmbed, "ai::embed", (model_id: String, input: String) -> Any, ai_embed_impl);
define_async_function!(AiGenerate, "ai::generate", (model_id: String, prompt: String, ?config: Object) -> Any, ai_generate_impl);

// =========================================================================
// Registration
// =========================================================================

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, AiChat, AiEmbed, AiGenerate,);
}
