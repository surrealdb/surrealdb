//! Auth context extraction from HTTP request parts.
//!
//! When running behind SurrealDB's `SurrealAuth` middleware, the authenticated
//! `Session` is placed into request extensions. This module extracts it during
//! MCP session initialization.

use surrealdb_core::dbs::Session;

/// Extract the authenticated `Session` from HTTP request parts in the extensions.
///
/// The rmcp `RequestContext` stores http::request::Parts in its extensions.
/// The SurrealDB auth middleware places the `Session` into the Parts' extensions.
pub fn extract_session_from_parts(parts: &http::request::Parts) -> Option<Session> {
	parts.extensions.get::<Session>().cloned()
}
