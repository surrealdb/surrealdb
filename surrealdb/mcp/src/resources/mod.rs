//! MCP resource implementations exposing database metadata.

pub mod instructions;
pub mod schema;

use rmcp::model::{AnnotateAble, RawResource, ReadResourceResult, Resource, ResourceContents};

use crate::session::McpSession;

pub const INSTRUCTIONS_URI: &str = "surrealdb://instructions";
pub const INFO_URI: &str = "surrealdb://info";
pub const SCHEMA_URI: &str = "surrealdb://schema";

pub fn list_resources() -> Vec<Resource> {
	vec![
		RawResource::new(INSTRUCTIONS_URI, "SurrealDB Instructions")
			.with_description("Usage instructions for the SurrealDB MCP server")
			.with_mime_type("text/markdown")
			.no_annotation(),
		RawResource::new(INFO_URI, "Server Info")
			.with_description("SurrealDB server version and capabilities")
			.with_mime_type("application/json")
			.no_annotation(),
		RawResource::new(SCHEMA_URI, "Database Schema")
			.with_description("Full database schema for the current namespace/database")
			.with_mime_type("application/json")
			.no_annotation(),
	]
}

pub async fn read_resource(
	session: &McpSession,
	uri: &str,
) -> Result<ReadResourceResult, rmcp::ErrorData> {
	let text = match uri {
		INSTRUCTIONS_URI => instructions::get_instructions().to_string(),
		INFO_URI => schema::get_server_info(),
		SCHEMA_URI => schema::get_database_schema(session).await,
		_ if uri.starts_with("surrealdb://schema/") => {
			let table = uri.trim_start_matches("surrealdb://schema/");
			schema::get_table_schema(session, table).await
		}
		_ => {
			return Err(rmcp::ErrorData::resource_not_found(
				format!("Unknown resource: {uri}"),
				None,
			));
		}
	};

	Ok(ReadResourceResult::new(vec![ResourceContents::text(text, uri)]))
}
