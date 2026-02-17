//! AI engine module for embedding generation, model inference, and agents.
//!
//! The `agent::types` submodule is always available (needed by the parser,
//! AST, and catalog). The rest of the AI functionality (providers, chat,
//! embed, generate, agent runtime) requires the `ai` feature.
//!
//! Provider-prefixed model identification scheme:
//! - `openai:text-embedding-3-small` — calls the OpenAI embeddings API
//! - `huggingface:BAAI/bge-small-en-v1.5` — calls the HuggingFace Inference API
//! - `voyage:voyage-3.5` — calls the Voyage AI API (also `claude:` or `anthropic:`)
//! - `google:text-embedding-005` — calls the Google Gemini API (also `gemini:`)
pub mod agent;
#[cfg(feature = "ai")]
pub mod chat;
#[cfg(feature = "ai")]
pub mod embed;
#[cfg(feature = "ai")]
pub mod generate;
#[cfg(feature = "ai")]
pub mod provider;
#[cfg(feature = "ai")]
pub mod providers;
