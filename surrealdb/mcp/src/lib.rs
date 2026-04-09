//! Built-in MCP (Model Context Protocol) server for SurrealDB.
//!
//! Provides MCP tools, resources, prompts, and completions for interacting
//! with a SurrealDB datastore directly. Designed to be mounted as an Axum
//! service at `/mcp` or served over stdio for IDE integration.

mod auth;
mod completions;
pub(crate) mod error;
mod logging;
pub mod prompts;
pub mod resources;
pub mod service;
pub(crate) mod session;
pub mod tools;

pub use service::McpService;
