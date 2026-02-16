//! AI functions for the streaming executor.
//!
//! These provide AI functionality (embeddings, etc.).
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
// Function definitions using the macro
// =========================================================================

define_async_function!(AiEmbed, "ai::embed", (model_id: String, input: String) -> Any, ai_embed_impl);

// =========================================================================
// Registration
// =========================================================================

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, AiEmbed,);
}
