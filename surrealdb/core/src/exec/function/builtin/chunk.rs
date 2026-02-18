//! AI chunking functions for the streaming executor.
//!
//! Provides text chunking strategies (fixed, sentence, paragraph, recursive, semantic).
//! Note: AI functions require the "ai" feature to be enabled.

use anyhow::Result;

use crate::exec::function::FunctionRegistry;
use crate::exec::physical_expr::EvalContext;
use crate::val::Value;
use crate::{define_async_function, register_functions};

// =========================================================================
// Helper
// =========================================================================

#[cfg(not(feature = "ai"))]
async fn ai_disabled() -> Result<Value> {
	Err(anyhow::anyhow!(crate::err::Error::AiDisabled))
}

#[cfg(feature = "ai")]
fn extract_i64(fn_name: &str, field: &str, val: &Value) -> Result<i64> {
	match val {
		Value::Number(n) => Ok(n.as_int()),
		v => Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
			name: fn_name.to_owned(),
			message: format!("The '{field}' argument must be a number, got: {}", v.kind_of()),
		})),
	}
}

// =========================================================================
// ai::chunk::fixed
// =========================================================================

#[cfg(feature = "ai")]
async fn ai_chunk_fixed_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	use crate::fnc::args::Optional;

	let text = match args.first() {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::fixed".to_owned(),
				message: format!(
					"The first argument must be a string of text, got: {}",
					v.kind_of()
				),
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::fixed".to_owned(),
				message: "Missing text argument".to_string(),
			}));
		}
	};

	let size = match args.get(1) {
		Some(v) => extract_i64("ai::chunk::fixed", "size", v)?,
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::fixed".to_owned(),
				message: "Missing size argument".to_string(),
			}));
		}
	};

	let overlap = args.get(2).map(|v| extract_i64("ai::chunk::fixed", "overlap", v)).transpose()?;

	crate::fnc::chunk::fixed::run((text, size, Optional(overlap)))
}

#[cfg(not(feature = "ai"))]
async fn ai_chunk_fixed_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	ai_disabled().await
}

// =========================================================================
// ai::chunk::sentence
// =========================================================================

#[cfg(feature = "ai")]
async fn ai_chunk_sentence_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	use crate::fnc::args::Optional;

	let text = match args.first() {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::sentence".to_owned(),
				message: format!(
					"The first argument must be a string of text, got: {}",
					v.kind_of()
				),
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::sentence".to_owned(),
				message: "Missing text argument".to_string(),
			}));
		}
	};

	let size = args.get(1).map(|v| extract_i64("ai::chunk::sentence", "size", v)).transpose()?;

	let overlap =
		args.get(2).map(|v| extract_i64("ai::chunk::sentence", "overlap", v)).transpose()?;

	crate::fnc::chunk::sentence::run((text, Optional(size), Optional(overlap)))
}

#[cfg(not(feature = "ai"))]
async fn ai_chunk_sentence_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	ai_disabled().await
}

// =========================================================================
// ai::chunk::paragraph
// =========================================================================

#[cfg(feature = "ai")]
async fn ai_chunk_paragraph_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	use crate::fnc::args::Optional;

	let text = match args.first() {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::paragraph".to_owned(),
				message: format!(
					"The first argument must be a string of text, got: {}",
					v.kind_of()
				),
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::paragraph".to_owned(),
				message: "Missing text argument".to_string(),
			}));
		}
	};

	let size = args.get(1).map(|v| extract_i64("ai::chunk::paragraph", "size", v)).transpose()?;

	let overlap =
		args.get(2).map(|v| extract_i64("ai::chunk::paragraph", "overlap", v)).transpose()?;

	crate::fnc::chunk::paragraph::run((text, Optional(size), Optional(overlap)))
}

#[cfg(not(feature = "ai"))]
async fn ai_chunk_paragraph_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	ai_disabled().await
}

// =========================================================================
// ai::chunk::recursive
// =========================================================================

#[cfg(feature = "ai")]
async fn ai_chunk_recursive_impl(_ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	use crate::fnc::args::Optional;

	let text = match args.first() {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::recursive".to_owned(),
				message: format!(
					"The first argument must be a string of text, got: {}",
					v.kind_of()
				),
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::recursive".to_owned(),
				message: "Missing text argument".to_string(),
			}));
		}
	};

	let size = match args.get(1) {
		Some(v) => extract_i64("ai::chunk::recursive", "size", v)?,
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::recursive".to_owned(),
				message: "Missing size argument".to_string(),
			}));
		}
	};

	let overlap =
		args.get(2).map(|v| extract_i64("ai::chunk::recursive", "overlap", v)).transpose()?;

	crate::fnc::chunk::recursive::run((text, size, Optional(overlap)))
}

#[cfg(not(feature = "ai"))]
async fn ai_chunk_recursive_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	ai_disabled().await
}

// =========================================================================
// ai::chunk::semantic
// =========================================================================

#[cfg(feature = "ai")]
async fn ai_chunk_semantic_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	use crate::catalog::providers::DatabaseProvider;

	let model_id = match args.first() {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::semantic".to_owned(),
				message: format!(
					"The first argument must be a string model ID (e.g. 'openai:text-embedding-3-small'), got: {}",
					v.kind_of()
				),
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::semantic".to_owned(),
				message: "Missing model ID argument".to_string(),
			}));
		}
	};

	let text = match args.get(1) {
		Some(Value::String(s)) => s.clone(),
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::semantic".to_owned(),
				message: format!(
					"The second argument must be a string of text, got: {}",
					v.kind_of()
				),
			}));
		}
		None => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::semantic".to_owned(),
				message: "Missing text argument".to_string(),
			}));
		}
	};

	let config = args.get(2).cloned();

	// Build a FrozenContext + Options to call through the fnc path
	let txn = ctx.exec_ctx.txn();

	let (ns, db) = if let Ok(db_ctx) = ctx.exec_ctx.database() {
		(db_ctx.db.namespace_id, db_ctx.db.database_id)
	} else if let Some(opt) = ctx.exec_ctx.root().options.as_ref() {
		let root_ctx = ctx.exec_ctx.root();
		match root_ctx.ctx.try_ns_db_ids(opt).await.ok().flatten() {
			Some(ids) => ids,
			None => {
				return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
					name: "ai::chunk::semantic".to_owned(),
					message: "No database context available for AI config".to_string(),
				}));
			}
		}
	} else {
		return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
			name: "ai::chunk::semantic".to_owned(),
			message: "No database context available for AI config".to_string(),
		}));
	};

	let ai_config = {
		let config_entry = txn.get_db_config(ns, db, "ai").await.ok().flatten();
		config_entry.and_then(|c| {
			let catalog_ai = c.try_as_ai().ok()?;
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
		})
	};

	// Parse config
	let default_threshold: f64 = 0.5;
	let (max_size, threshold) = match config {
		None => (None, default_threshold),
		Some(Value::Object(ref obj)) => {
			let size = match obj.get("size") {
				Some(Value::Number(n)) => {
					let v = n.as_int();
					if v <= 0 {
						return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
							name: "ai::chunk::semantic".to_owned(),
							message: format!(
								"The 'size' config field must be a positive integer, got: {v}"
							),
						}));
					}
					Some(v as usize)
				}
				Some(_) => {
					return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
						name: "ai::chunk::semantic".to_owned(),
						message: "The 'size' config field must be a number".to_owned(),
					}));
				}
				None => None,
			};
			let threshold = match obj.get("threshold") {
				Some(Value::Number(n)) => {
					let v = n.as_float();
					if !(0.0..=1.0).contains(&v) {
						return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
							name: "ai::chunk::semantic".to_owned(),
							message: format!(
								"The 'threshold' config field must be between 0.0 and 1.0, got: {v}"
							),
						}));
					}
					v
				}
				Some(_) => {
					return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
						name: "ai::chunk::semantic".to_owned(),
						message: "The 'threshold' config field must be a number".to_owned(),
					}));
				}
				None => default_threshold,
			};
			(size, threshold)
		}
		Some(v) => {
			return Err(anyhow::anyhow!(crate::err::Error::InvalidFunctionArguments {
				name: "ai::chunk::semantic".to_owned(),
				message: format!("The config argument must be an object, got: {}", v.kind_of()),
			}));
		}
	};

	// Split into sentences
	let sentences = crate::fnc::chunk::split_sentences(&text);
	if sentences.len() <= 1 {
		let chunks: Vec<Value> =
			sentences.into_iter().map(|s| Value::from(s.to_string())).collect();
		return Ok(Value::Array(chunks.into()));
	}

	// Generate embeddings
	let mut embeddings = Vec::with_capacity(sentences.len());
	for sent in &sentences {
		let emb = crate::ai::embed::embed(&model_id, sent, ai_config.as_ref()).await?;
		embeddings.push(emb);
	}

	// Cosine similarity
	fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
		let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
		let mag_a: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
		let mag_b: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
		if mag_a == 0.0 || mag_b == 0.0 {
			return 0.0;
		}
		dot / (mag_a * mag_b)
	}

	// Find split points
	let mut groups: Vec<Vec<&str>> = Vec::new();
	let mut current_group: Vec<&str> = vec![sentences[0]];

	for i in 1..sentences.len() {
		let sim = cosine_similarity(&embeddings[i - 1], &embeddings[i]);
		if sim < threshold {
			groups.push(current_group);
			current_group = vec![sentences[i]];
		} else {
			current_group.push(sentences[i]);
		}
	}
	groups.push(current_group);

	// Join and optionally enforce max size
	let mut chunks: Vec<String> = Vec::new();
	for group in groups {
		let joined = group.join(" ");
		match max_size {
			Some(max) if joined.len() > max => {
				chunks.extend(crate::fnc::chunk::group_segments(&group, max, 0));
			}
			_ => {
				chunks.push(joined);
			}
		}
	}

	let arr: Vec<Value> = chunks.into_iter().map(Value::from).collect();
	Ok(Value::Array(arr.into()))
}

#[cfg(not(feature = "ai"))]
async fn ai_chunk_semantic_impl(_ctx: &EvalContext<'_>, _args: Vec<Value>) -> Result<Value> {
	ai_disabled().await
}

// =========================================================================
// Function definitions using the macro
// =========================================================================

define_async_function!(AiChunkFixed, "ai::chunk::fixed", (text: String, size: Any, ?overlap: Any) -> Any, ai_chunk_fixed_impl);
define_async_function!(AiChunkSentence, "ai::chunk::sentence", (text: String, ?size: Any, ?overlap: Any) -> Any, ai_chunk_sentence_impl);
define_async_function!(AiChunkParagraph, "ai::chunk::paragraph", (text: String, ?size: Any, ?overlap: Any) -> Any, ai_chunk_paragraph_impl);
define_async_function!(AiChunkRecursive, "ai::chunk::recursive", (text: String, size: Any, ?overlap: Any) -> Any, ai_chunk_recursive_impl);
define_async_function!(AiChunkSemantic, "ai::chunk::semantic", (model_id: String, text: String, ?config: Any) -> Any, ai_chunk_semantic_impl);

// =========================================================================
// Registration
// =========================================================================

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		AiChunkFixed,
		AiChunkSentence,
		AiChunkParagraph,
		AiChunkRecursive,
		AiChunkSemantic,
	);
}
