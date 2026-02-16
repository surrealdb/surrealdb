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
// Function definitions using the macro
// =========================================================================

define_async_function!(AiEmbed, "ai::embed", (model_id: String, input: String) -> Any, ai_embed_impl);
define_async_function!(AiGenerate, "ai::generate", (model_id: String, prompt: String, ?config: Object) -> Any, ai_generate_impl);

// =========================================================================
// Registration
// =========================================================================

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, AiEmbed, AiGenerate,);
}
