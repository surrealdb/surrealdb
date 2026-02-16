//! AI engine module for embedding generation and model inference.
//!
//! This module provides the core AI functionality accessible via `ai::*`
//! functions in SurrealQL. It supports multiple providers using a
//! provider-prefixed model identification scheme:
//!
//! - `openai:text-embedding-3-small` — calls the OpenAI embeddings API
//! - `huggingface:BAAI/bge-small-en-v1.5` — calls the HuggingFace Inference API
//! - `voyage:voyage-3.5` — calls the Voyage AI API (also `claude:` or `anthropic:`)
//! - `google:text-embedding-005` — calls the Google Gemini API (also `gemini:`)
pub mod chat;
pub mod embed;
pub mod generate;
pub mod provider;
pub mod providers;
