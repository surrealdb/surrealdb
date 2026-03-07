//! AI Agent types and runtime.
//!
//! The `types` submodule is always available (used by parser, AST, catalog).
//! The runtime modules (engine, tools, memory, etc.) require the `ai` feature.
pub mod types;

#[cfg(feature = "ai")]
pub mod engine;
#[cfg(feature = "ai")]
pub mod guardrails;
#[cfg(feature = "ai")]
pub mod memory;
#[cfg(feature = "ai")]
pub mod tools;
