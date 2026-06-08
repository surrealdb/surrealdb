//! Built-in MCP (Model Context Protocol) server for SurrealDB.
//!
//! Provides MCP tools, resources, prompts, and completions for interacting
//! with a SurrealDB datastore directly. Designed to be mounted as an Axum
//! service at `/mcp` or served over stdio for IDE integration.
#![recursion_limit = "256"]

mod audit;
pub(crate) mod auth;
pub mod cnf;
mod completions;
pub(crate) mod error;
pub mod metrics;
pub mod prompts;
pub mod resources;
pub mod service;
pub(crate) mod session;
pub mod tools;

pub use metrics::{McpMetricsRecorder, McpToolOutcome};
pub use service::McpService;
