//! MCP resource implementations exposing database metadata.
//!
//! # URI design
//!
//! Resources that are server-wide (instructions, server info, version) use
//! stable singleton URIs. Schema resources are inherently scoped to a
//! `(namespace, database[, table])` triple and embed that triple in the
//! URI itself -- **not** as implicit session state. This guarantees:
//!
//! - Globally unique identity: `surrealdb://schema/ns/foo/db/bar/table/users` unambiguously refers
//!   to the `users` table in `foo/bar` regardless of what the caller's current `use` context is.
//! - Safe client caching / subscription: an MCP client that caches bodies by URI, or subscribes to
//!   `notifications/resources/updated`, will not serve the wrong namespace's schema after the
//!   session runs a `use`.
//! - Shareable references: an LLM can emit a resource URI in its output and another session (or
//!   tool) can resolve it without first negotiating context.
//!
//! The literal `ns` / `db` / `table` path segments disambiguate parsing so
//! that a table genuinely named `ns` is addressable as
//! `.../table/ns` rather than clashing with the top-level path shape.

pub mod instructions;
pub mod schema;

use rmcp::ErrorData;
use rmcp::model::{
	AnnotateAble, RawResource, RawResourceTemplate, ReadResourceResult, Resource, ResourceContents,
	ResourceTemplate,
};

use crate::session::McpSession;

pub const INSTRUCTIONS_URI: &str = "surrealdb://instructions";
pub const INFO_URI: &str = "surrealdb://info";
pub const VERSION_URI: &str = "surrealdb://version";

/// URI template for a whole-database schema resource.
pub const DATABASE_SCHEMA_TEMPLATE: &str = "surrealdb://schema/ns/{namespace}/db/{database}";

/// URI template for a single-table schema resource.
pub const TABLE_SCHEMA_TEMPLATE: &str =
	"surrealdb://schema/ns/{namespace}/db/{database}/table/{table}";

/// URI prefix shared by all schema resources; used for routing in
/// [`read_resource`].
const SCHEMA_URI_PREFIX: &str = "surrealdb://schema/";

pub fn list_resources() -> Vec<Resource> {
	// Only stable, context-independent resources are listed here. Schema
	// resources require a `(namespace, database[, table])` triple and are
	// advertised via [`list_resource_templates`] instead, so their URIs
	// remain globally unique and safe for clients to cache.
	vec![
		RawResource::new(INSTRUCTIONS_URI, "SurrealDB Instructions")
			.with_description("Usage instructions for the SurrealDB MCP server")
			.with_mime_type("text/markdown")
			.no_annotation(),
		RawResource::new(INFO_URI, "Server Info")
			.with_description("SurrealDB server version and capabilities")
			.with_mime_type("application/json")
			.no_annotation(),
		RawResource::new(VERSION_URI, "SurrealDB Version")
			.with_description("SurrealDB version string")
			.with_mime_type("text/plain")
			.no_annotation(),
	]
}

/// Advertise the schema URI templates so MCP clients can discover the
/// parameterised schema surface via `resources/templates/list`.
pub fn list_resource_templates() -> Vec<ResourceTemplate> {
	vec![
		RawResourceTemplate::new(DATABASE_SCHEMA_TEMPLATE, "Database Schema")
			.with_description(
				"Full schema for a specific namespace/database. Replace \
				 `{namespace}` and `{database}` with the target identifiers. \
				 The URI is globally unique so it is safe to cache and subscribe to.",
			)
			.with_mime_type("application/json")
			.no_annotation(),
		RawResourceTemplate::new(TABLE_SCHEMA_TEMPLATE, "Table Schema")
			.with_description(
				"Schema for a specific table inside a specific namespace/database. \
				 Replace `{namespace}`, `{database}`, and `{table}` with the target \
				 identifiers. The URI is globally unique so it is safe to cache and \
				 subscribe to.",
			)
			.with_mime_type("application/json")
			.no_annotation(),
	]
}

/// Parsed schema URI target.
///
/// Matches one of the schema URI templates; anything else is rejected by
/// [`parse_schema_uri`] so the dispatcher can return `resource_not_found`.
enum SchemaTarget<'a> {
	Database {
		namespace: &'a str,
		database: &'a str,
	},
	Table {
		namespace: &'a str,
		database: &'a str,
		table: &'a str,
	},
}

/// Parse a schema URI into its typed target.
///
/// Accepts exactly the two URI shapes advertised via
/// [`list_resource_templates`]; anything else returns `None` so callers can
/// surface a `resource_not_found` JSON-RPC error rather than silently
/// falling back to ambient session state.
fn parse_schema_uri(uri: &str) -> Option<SchemaTarget<'_>> {
	let tail = uri.strip_prefix(SCHEMA_URI_PREFIX)?;
	let segments: Vec<&str> = tail.split('/').collect();
	match segments.as_slice() {
		["ns", ns, "db", db] if !ns.is_empty() && !db.is_empty() => Some(SchemaTarget::Database {
			namespace: ns,
			database: db,
		}),
		["ns", ns, "db", db, "table", table]
			if !ns.is_empty() && !db.is_empty() && !table.is_empty() =>
		{
			Some(SchemaTarget::Table {
				namespace: ns,
				database: db,
				table,
			})
		}
		_ => None,
	}
}

pub async fn read_resource(
	session: &McpSession,
	uri: &str,
) -> Result<ReadResourceResult, ErrorData> {
	let text = match uri {
		INSTRUCTIONS_URI => instructions::get_instructions().to_string(),
		INFO_URI => schema::get_server_info(),
		VERSION_URI => schema::get_version(),
		_ => match parse_schema_uri(uri) {
			Some(SchemaTarget::Database {
				namespace,
				database,
			}) => schema::get_database_schema(session, namespace, database).await?,
			Some(SchemaTarget::Table {
				namespace,
				database,
				table,
			}) => schema::get_table_schema(session, namespace, database, table).await?,
			None => {
				return Err(ErrorData::resource_not_found(
					format!("Unknown resource: {uri}"),
					None,
				));
			}
		},
	};

	Ok(ReadResourceResult::new(vec![ResourceContents::text(text, uri)]))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn database_template_parses() {
		let Some(SchemaTarget::Database {
			namespace,
			database,
		}) = parse_schema_uri("surrealdb://schema/ns/foo/db/bar")
		else {
			panic!("expected Database target");
		};
		assert_eq!(namespace, "foo");
		assert_eq!(database, "bar");
	}

	#[test]
	fn table_template_parses() {
		let Some(SchemaTarget::Table {
			namespace,
			database,
			table,
		}) = parse_schema_uri("surrealdb://schema/ns/foo/db/bar/table/users")
		else {
			panic!("expected Table target");
		};
		assert_eq!(namespace, "foo");
		assert_eq!(database, "bar");
		assert_eq!(table, "users");
	}

	/// A table genuinely named `ns` must still be addressable via the
	/// fully qualified template. The literal `ns` / `db` / `table` keyword
	/// segments are what disambiguate the shape, so this is the worst-case
	/// regression target.
	#[test]
	fn table_literally_named_ns_is_addressable() {
		let Some(SchemaTarget::Table {
			namespace,
			database,
			table,
		}) = parse_schema_uri("surrealdb://schema/ns/foo/db/bar/table/ns")
		else {
			panic!("expected Table target");
		};
		assert_eq!(namespace, "foo");
		assert_eq!(database, "bar");
		assert_eq!(table, "ns");
	}

	#[test]
	fn rejects_legacy_session_scoped_shapes() {
		// `surrealdb://schema` and `surrealdb://schema/<table>` were the
		// old session-scoped forms. They are deliberately not parseable
		// so clients see an explicit "unknown resource" rather than silent
		// context bleed.
		assert!(parse_schema_uri("surrealdb://schema").is_none());
		assert!(parse_schema_uri("surrealdb://schema/users").is_none());
		assert!(parse_schema_uri("surrealdb://schema/ns/foo").is_none());
		assert!(parse_schema_uri("surrealdb://schema/ns/foo/db/").is_none());
		assert!(parse_schema_uri("surrealdb://schema/ns//db/bar").is_none());
	}
}
