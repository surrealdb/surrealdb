//! AI functions for the streaming executor.
//!
//! These provide AI functionality (embeddings, text generation, etc.).
//! Note: AI functions require the "ai" feature to be enabled.
//!
//! When a database is selected and has DEFINE CONFIG AI, those credentials
//! override environment variables for that database.

use anyhow::Result;

#[cfg(feature = "ai")]
use crate::catalog::providers::DatabaseProvider;
#[cfg(feature = "ai")]
use crate::dbs::capabilities::ExperimentalTarget;
#[cfg(feature = "ai")]
use crate::err::Error;
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

#[cfg(feature = "ai")]
fn check_ai_experimental(ctx: &EvalContext<'_>, fn_name: &str) -> Result<()> {
	if !ctx.capabilities().allows_experimental(&ExperimentalTarget::Ai) {
		return Err(Error::InvalidFunction {
			name: fn_name.to_string(),
			message: "Experimental capability `ai` is not enabled".to_string(),
		}
		.into());
	}
	Ok(())
}

#[cfg(feature = "ai")]
fn check_ai_provider(ctx: &EvalContext<'_>, model_id: &str) -> Result<()> {
	let (provider_name, _) = crate::ai::chat::parse_model_id(model_id)?;
	let frozen_ctx = ctx.exec_ctx.ctx();
	crate::ai::chat::check_ai_provider_allowed(frozen_ctx, provider_name)
}

#[cfg(feature = "ai")]
async fn check_ai_net(
	ctx: &EvalContext<'_>,
	model_id: &str,
	ai_config: Option<&crate::ai::config::AiConfigOverlay>,
) -> Result<()> {
	let (provider_name, _) = crate::ai::chat::parse_model_id(model_id)?;
	let frozen_ctx = ctx.exec_ctx.ctx();
	crate::ai::chat::check_provider_net_allowed(frozen_ctx, provider_name, ai_config).await
}

#[cfg(feature = "ai")]
async fn ai_config_overlay_from_ctx(
	ctx: &EvalContext<'_>,
) -> Option<crate::ai::config::AiConfigOverlay> {
	let txn = ctx.exec_ctx.txn();

	// Try to get namespace/database IDs, either from the database-level
	// execution context or (when the planner sets Root context for statements
	// like RETURN) via the legacy Options stored in the root context.
	let (ns, db) = if let Ok(db_ctx) = ctx.exec_ctx.database() {
		(db_ctx.db.namespace_id, db_ctx.db.database_id)
	} else if let Some(opt) = ctx.exec_ctx.root().options.as_ref() {
		let root_ctx = ctx.exec_ctx.root();
		root_ctx.ctx.try_ns_db_ids(opt).await.ok().flatten()?
	} else {
		return None;
	};

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

// =========================================================================
// AI Embed
// =========================================================================

#[cfg(feature = "ai")]
async fn ai_embed_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	check_ai_experimental(ctx, "ai::embed")?;
	let model_id = match args.first() {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::embed".to_owned(),
				message: format!(
					"The first argument should be a string model ID (e.g. 'openai:text-embedding-3-small'), got: {}",
					v.kind_of()
				),
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::embed".to_owned(),
				message: "Missing model ID argument".to_string(),
			}));
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
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::embed".to_owned(),
				message: "Missing text input argument".to_string(),
			}));
		}
	};

	check_ai_provider(ctx, &model_id)?;
	let ai_config = ai_config_overlay_from_ctx(ctx).await;
	check_ai_net(ctx, &model_id, ai_config.as_ref()).await?;
	let embedding = crate::ai::embed::embed(&model_id, &input, ai_config.as_ref()).await?;
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
async fn ai_generate_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	check_ai_experimental(ctx, "ai::generate")?;
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
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::generate".to_owned(),
				message: "Missing model ID argument".to_string(),
			}));
		}
	};

	check_ai_provider(ctx, &model_id)?;

	let prompt = match args.get(1) {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::generate".to_owned(),
				message: format!(
					"The second argument should be a string prompt, got: {}",
					v.kind_of()
				),
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::generate".to_owned(),
				message: "Missing prompt argument".to_string(),
			}));
		}
	};

	let config = match args.get(2) {
		Some(Value::Object(obj)) => {
			let temperature = match obj.get("temperature") {
				Some(Value::Number(n)) => Some(n.as_float()),
				Some(_) => {
					return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
						name: "ai::generate".to_owned(),
						message: "The 'temperature' config field must be a number".to_owned(),
					}));
				}
				None => None,
			};

			let max_tokens = match obj.get("max_tokens") {
				Some(Value::Number(n)) => Some(n.as_int() as u64),
				Some(_) => {
					return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
						name: "ai::generate".to_owned(),
						message: "The 'max_tokens' config field must be a number".to_owned(),
					}));
				}
				None => None,
			};

			let top_p = match obj.get("top_p") {
				Some(Value::Number(n)) => Some(n.as_float()),
				Some(_) => {
					return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
						name: "ai::generate".to_owned(),
						message: "The 'top_p' config field must be a number".to_owned(),
					}));
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
				message: format!("The config argument must be an object, got: {}", v.kind_of()),
			}));
		}
		None => GenerationConfig::default(),
	};

	let ai_config = ai_config_overlay_from_ctx(ctx).await;
	check_ai_net(ctx, &model_id, ai_config.as_ref()).await?;
	let text =
		crate::ai::generate::generate(&model_id, &prompt, &config, ai_config.as_ref()).await?;
	Ok(Value::String(text))
}

#[cfg(not(feature = "ai"))]
async fn ai_generate_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	ai_disabled().await
}

// =========================================================================
// AI Chat
// =========================================================================

#[cfg(feature = "ai")]
async fn ai_chat_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	check_ai_experimental(ctx, "ai::chat")?;
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
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chat".to_owned(),
				message: "Missing model ID argument".to_string(),
			}));
		}
	};

	check_ai_provider(ctx, &model_id)?;

	let messages = match args.get(1) {
		Some(Value::Array(arr)) => {
			if arr.is_empty() {
				return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
					name: "ai::chat".to_owned(),
					message: "The messages array must not be empty".to_owned(),
				}));
			}
			let mut msgs = Vec::with_capacity(arr.len());
			for (i, item) in arr.iter().enumerate() {
				let obj = match item {
					Value::Object(obj) => obj,
					v => {
						return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
							name: "ai::chat".to_owned(),
							message: format!(
								"Message at index {i} must be an object, got: {}",
								v.kind_of()
							),
						}));
					}
				};
				let role = match obj.get("role") {
					Some(Value::String(s)) => s.clone(),
					Some(v) => {
						return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
							name: "ai::chat".to_owned(),
							message: format!(
								"Message at index {i}: 'role' must be a string, got: {}",
								v.kind_of()
							),
						}));
					}
					None => {
						return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
							name: "ai::chat".to_owned(),
							message: format!("Message at index {i} is missing the 'role' field"),
						}));
					}
				};
				let content = match obj.get("content") {
					Some(Value::String(s)) => s.clone(),
					Some(v) => {
						return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
							name: "ai::chat".to_owned(),
							message: format!(
								"Message at index {i}: 'content' must be a string, got: {}",
								v.kind_of()
							),
						}));
					}
					None => {
						return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
							name: "ai::chat".to_owned(),
							message: format!("Message at index {i} is missing the 'content' field"),
						}));
					}
				};
				msgs.push(ChatMessage::text(role, content));
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
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chat".to_owned(),
				message: "Missing messages argument".to_string(),
			}));
		}
	};

	let config = match args.get(2) {
		Some(Value::Object(obj)) => {
			let temperature = match obj.get("temperature") {
				Some(Value::Number(n)) => Some(n.as_float()),
				Some(_) => {
					return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
						name: "ai::chat".to_owned(),
						message: "The 'temperature' config field must be a number".to_owned(),
					}));
				}
				None => None,
			};

			let max_tokens = match obj.get("max_tokens") {
				Some(Value::Number(n)) => Some(n.as_int() as u64),
				Some(_) => {
					return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
						name: "ai::chat".to_owned(),
						message: "The 'max_tokens' config field must be a number".to_owned(),
					}));
				}
				None => None,
			};

			let top_p = match obj.get("top_p") {
				Some(Value::Number(n)) => Some(n.as_float()),
				Some(_) => {
					return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
						name: "ai::chat".to_owned(),
						message: "The 'top_p' config field must be a number".to_owned(),
					}));
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
				message: format!("The config argument must be an object, got: {}", v.kind_of()),
			}));
		}
		None => GenerationConfig::default(),
	};

	let ai_config = ai_config_overlay_from_ctx(ctx).await;
	check_ai_net(ctx, &model_id, ai_config.as_ref()).await?;
	let text = crate::ai::chat::chat(&model_id, &messages, &config, ai_config.as_ref()).await?;
	Ok(Value::String(text))
}

#[cfg(not(feature = "ai"))]
async fn ai_chat_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	ai_disabled().await
}

// =========================================================================
// AI Sentiment
// =========================================================================

#[cfg(feature = "ai")]
async fn ai_sentiment_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	check_ai_experimental(ctx, "ai::sentiment")?;
	let model_id = match args.first() {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::sentiment".to_owned(),
				message: format!(
					"The first argument should be a string model ID (e.g. 'openai:gpt-4-turbo'), got: {}",
					v.kind_of()
				),
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::sentiment".to_owned(),
				message: "Missing model ID argument".to_string(),
			}));
		}
	};

	check_ai_provider(ctx, &model_id)?;

	let text = match args.get(1) {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::sentiment".to_owned(),
				message: format!(
					"The second argument should be a string of text to analyse, got: {}",
					v.kind_of()
				),
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::sentiment".to_owned(),
				message: "Missing text argument".to_string(),
			}));
		}
	};

	let ai_config = ai_config_overlay_from_ctx(ctx).await;
	check_ai_net(ctx, &model_id, ai_config.as_ref()).await?;
	let prompt = crate::fnc::ai::build_sentiment_prompt(&text);
	let config = crate::fnc::ai::sentiment_generation_config();
	let raw =
		crate::ai::generate::generate(&model_id, &prompt, &config, ai_config.as_ref()).await?;
	crate::fnc::ai::parse_sentiment_response(&raw)
}

#[cfg(not(feature = "ai"))]
async fn ai_sentiment_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	ai_disabled().await
}

// =========================================================================
// Function definitions using the macro
// =========================================================================

define_async_function!(AiChat, "ai::chat", (model_id: String, messages: Any, ?config: Any) -> Any, ai_chat_impl);
define_async_function!(AiEmbed, "ai::embed", (model_id: String, input: String) -> Any, ai_embed_impl);
define_async_function!(AiGenerate, "ai::generate", (model_id: String, prompt: String, ?config: Any) -> Any, ai_generate_impl);
define_async_function!(AiSentiment, "ai::sentiment", (model_id: String, text: String) -> Any, ai_sentiment_impl);

// =========================================================================
// Registration
// =========================================================================

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, AiChat, AiEmbed, AiGenerate, AiSentiment,);
}
