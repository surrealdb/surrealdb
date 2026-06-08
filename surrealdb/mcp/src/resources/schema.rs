//! Schema and server info resources.
//!
//! Schema readers take an explicit `(namespace, database[, table])` triple
//! rather than implicitly reading the caller's current `use` context. The
//! URI template embeds the triple so resource identity is globally unique
//! and safe for MCP clients to cache or subscribe to; see
//! [`crate::resources`] module docs for the reasoning.

use rmcp::ErrorData;
use rmcp::model::ProtocolVersion;
use serde_json::json;

use crate::session::McpSession;
use crate::tools::output::cap_value;
use crate::tools::validate_table_name;

/// Emit the `surrealdb://info` resource.
///
/// The protocol version is taken from [`ProtocolVersion::LATEST`] so this
/// value cannot drift from what we actually negotiate during the MCP
/// handshake. Capabilities mirror what [`crate::service::McpService::get_info`]
/// advertises so clients can introspect server features from a single
/// resource fetch without re-reading the initialize response.
pub fn get_server_info() -> String {
	json!({
		"name": "SurrealDB",
		"version": env!("CARGO_PKG_VERSION"),
		"protocol_version": ProtocolVersion::LATEST.to_string(),
		"capabilities": {
			"tools": true,
			"resources": true,
			"prompts": true,
			"completions": true,
		},
	})
	.to_string()
}

/// Short human-readable version string, e.g. `SurrealDB 3.0.0`.
pub fn get_version() -> String {
	format!("SurrealDB {}", env!("CARGO_PKG_VERSION"))
}

/// Read the full schema for `{namespace}/{database}`: the database-level
/// `INFO FOR DB` plus a `tables` map enriched with per-table fields,
/// indexes, and events.
///
/// `INFO FOR DB` on its own only returns table-level `DEFINE TABLE`
/// summaries, which forced an LLM client to issue a follow-up
/// `INFO FOR TABLE` per table just to see field definitions. To avoid
/// that N-fold round-trip, this resource calls `INFO FOR DB` once and
/// then walks the resulting tables, replacing each table's DDL string
/// with an object of the form
/// `{ definition, fields, indexes, events, lives, tables }` matching
/// the per-table `INFO FOR TABLE` shape.
///
/// Executes under a session scoped explicitly to `(namespace, database)` so
/// the result is tied to the URI, not to the caller's live `use` state.
/// Returns a self-describing JSON document so a client that holds the
/// response can still identify which namespace/database it came from.
///
/// To bound work on databases with many tables, enrichment is capped at
/// [`crate::cnf::McpConfig::schema_resource_max_tables`]; tables beyond
/// the cap keep their bare DDL string and the response carries a
/// `tables_truncated_at` marker. The serialised body is also subject to
/// the standard [`cap_value`] size cap.
pub async fn get_database_schema(
	session: &McpSession,
	namespace: &str,
	database: &str,
) -> Result<String, ErrorData> {
	validate_identifier_segment("namespace", namespace)?;
	validate_identifier_segment("database", database)?;

	let results = session.execute_in(namespace, database, "INFO FOR DB", None).await?;
	let result = results.into_iter().next().ok_or_else(|| {
		tracing::warn!(
			target: "surrealdb::mcp",
			namespace,
			database,
			"INFO FOR DB returned no statements"
		);
		ErrorData::internal_error("INFO FOR DB returned no statements", None)
	})?;

	let body = match result.result {
		Ok(value) => {
			let mut schema = value.into_json_value();
			let tables_truncated_at =
				enrich_tables(session, namespace, database, &mut schema).await;
			let (schema, truncated) = cap_value(schema, session.config().max_result_bytes);
			let mut body = json!({
				"namespace": namespace,
				"database": database,
				"truncated": truncated,
				"schema": schema,
			});
			if let Some(at) = tables_truncated_at {
				body.as_object_mut()
					.expect("body is an object")
					.insert("tables_truncated_at".to_string(), json!(at));
			}
			body
		}
		Err(err) => json!({
			"namespace": namespace,
			"database": database,
			"error": err.message(),
			"kind": err.kind_str(),
		}),
	};

	Ok(serialize_body(&body))
}

/// Walk the `tables` map of an `INFO FOR DB` body and replace each
/// table's bare DDL string with a structured object containing its
/// fields, indexes, and events.
///
/// Returns `Some(cap)` if the per-table enrichment was capped at
/// [`crate::cnf::McpConfig::schema_resource_max_tables`] tables; `None`
/// otherwise. On a per-table fetch failure the table's value is
/// rewritten to `{ "definition": "...", "error": "..." }` so the
/// overall body remains useful.
async fn enrich_tables(
	session: &McpSession,
	namespace: &str,
	database: &str,
	schema: &mut serde_json::Value,
) -> Option<usize> {
	let max_tables = session.config().schema_resource_max_tables;
	let tables =
		schema.as_object_mut().and_then(|m| m.get_mut("tables")).and_then(|t| t.as_object_mut())?;
	let total = tables.len();
	let truncated_at = if total > max_tables {
		Some(max_tables)
	} else {
		None
	};
	// Sort keys so the truncation set is stable and the response is
	// reproducible across calls when the cap fires.
	let names: Vec<String> = {
		let mut keys: Vec<String> = tables.keys().cloned().collect();
		keys.sort();
		keys.into_iter().take(max_tables).collect()
	};
	for name in names {
		// Tables without a stored definition render with an empty string so
		// the JSON envelope stays well-formed for downstream consumers.
		let definition = tables
			.get(&name)
			.and_then(|v| v.as_str())
			.map(str::to_string)
			.unwrap_or_else(String::new);
		let entry = match fetch_table_schema(session, namespace, database, &name).await {
			Ok(per_table) => {
				let mut obj = match per_table {
					serde_json::Value::Object(map) => map,
					_ => serde_json::Map::new(),
				};
				obj.insert("definition".to_string(), serde_json::Value::String(definition));
				serde_json::Value::Object(obj)
			}
			Err(err) => json!({
				"definition": definition,
				"error": err,
			}),
		};
		tables.insert(name, entry);
	}
	truncated_at
}

/// Run `INFO FOR TABLE <name>` against the prospective `(ns, db)` and
/// return its body. Errors are downgraded to a string so the caller can
/// embed them inline rather than failing the whole resource.
///
/// `table` arrives here as a *bare* identifier read from the parsed
/// `INFO FOR DB` JSON object (parser-stored verbatim, with backticks
/// stripped), so a table created via `DEFINE TABLE \`my table\`` lands
/// here as the literal string `"my table"`. Bare-ident validation
/// would reject names with spaces, hyphens, leading digits, or
/// reserved keywords, silently collapsing those tables to a
/// `{definition, error}` stub in the enrichment loop. We re-quote the
/// name with backticks (after sanity-checking for embedded
/// backticks/newlines/CR/NUL that would break the quoting) so any
/// stored identifier round-trips correctly.
async fn fetch_table_schema(
	session: &McpSession,
	namespace: &str,
	database: &str,
	table: &str,
) -> Result<serde_json::Value, String> {
	if table.is_empty() {
		return Err("table name cannot be empty".to_string());
	}
	if table.bytes().any(|b| matches!(b, b'`' | b'\n' | b'\r' | 0)) {
		return Err(format!("table name contains invalid characters: {table}"));
	}
	let quoted = format!("`{table}`");
	// Validation must accept the quoted form; if it doesn't the name
	// failed our own sanity check above and falling through would
	// emit an inscrutable error.
	let _ = validate_table_name(&quoted).map_err(|_| format!("invalid table name: {table}"))?;
	let query = format!("INFO FOR TABLE {quoted}");
	let results = session
		.execute_in(namespace, database, &query, None)
		.await
		.map_err(|e| format!("execute failed: {e:?}"))?;
	let result = results
		.into_iter()
		.next()
		.ok_or_else(|| "INFO FOR TABLE returned no statements".to_string())?;
	match result.result {
		Ok(value) => Ok(value.into_json_value()),
		Err(err) => Err(err.message().to_string()),
	}
}

/// Read the schema for a single table via `INFO FOR TABLE <table>`.
///
/// Executes under a session scoped explicitly to `(namespace, database)` so
/// the result is tied to the URI, not to the caller's live `use` state.
pub async fn get_table_schema(
	session: &McpSession,
	namespace: &str,
	database: &str,
	table: &str,
) -> Result<String, ErrorData> {
	validate_identifier_segment("namespace", namespace)?;
	validate_identifier_segment("database", database)?;
	let table = validate_table_name(table)?;

	let query = format!("INFO FOR TABLE {table}");
	let results = session.execute_in(namespace, database, &query, None).await?;
	let result = results.into_iter().next().ok_or_else(|| {
		tracing::warn!(
			target: "surrealdb::mcp",
			namespace,
			database,
			table,
			"INFO FOR TABLE returned no statements"
		);
		ErrorData::internal_error("INFO FOR TABLE returned no statements", None)
	})?;

	let body = match result.result {
		Ok(value) => {
			let (schema, truncated) =
				cap_value(value.into_json_value(), session.config().max_result_bytes);
			json!({
				"namespace": namespace,
				"database": database,
				"table": table,
				"truncated": truncated,
				"schema": schema,
			})
		}
		Err(err) => json!({
			"namespace": namespace,
			"database": database,
			"table": table,
			"error": err.message(),
			"kind": err.kind_str(),
		}),
	};

	Ok(serialize_body(&body))
}

/// Validate a URI-embedded identifier segment (namespace or database name).
///
/// Defers to [`validate_table_name`] because the rules are identical: bare
/// `[A-Za-z_][A-Za-z0-9_]*` or a backtick-quoted body. The caller-facing
/// `field` label is used to produce a targeted error message.
fn validate_identifier_segment(field: &str, value: &str) -> Result<(), ErrorData> {
	validate_table_name(value).map_err(|_| {
		crate::error::invalid_params(format!(
			"Invalid {field} identifier in resource URI: '{value}'"
		))
	})?;
	Ok(())
}

/// Pretty-serialise a schema body. The fallback is a valid JSON literal so
/// an MCP client always receives parseable JSON even on the vanishingly
/// rare serialisation failure.
fn serialize_body(body: &serde_json::Value) -> String {
	serde_json::to_string_pretty(body).unwrap_or_else(|e| {
		tracing::warn!(
			target: "surrealdb::mcp",
			error = %e,
			"Failed to serialize schema body"
		);
		"null".to_string()
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn server_info_is_valid_json_with_version_key() {
		let text = get_server_info();
		let json: serde_json::Value =
			serde_json::from_str(&text).expect("server info must be valid JSON");
		let version = json.get("version").and_then(|v| v.as_str()).expect("version key");
		assert!(!version.is_empty(), "version should be non-empty");
		assert_eq!(json.get("name").and_then(|v| v.as_str()), Some("SurrealDB"));
	}

	#[test]
	fn version_contains_surrealdb_name() {
		let text = get_version();
		assert!(text.starts_with("SurrealDB "), "version: {text}");
	}

	#[test]
	fn identifier_segment_rejects_injection() {
		assert!(validate_identifier_segment("namespace", "foo; DROP").is_err());
		assert!(validate_identifier_segment("namespace", "").is_err());
		assert!(validate_identifier_segment("namespace", "valid_name").is_ok());
	}

	/// Regression: the database-level schema resource must enrich each
	/// table with its `fields`/`indexes`/`events` subtree so a single
	/// fetch yields the full schema. Before the fix the resource only
	/// returned the table-level `DEFINE TABLE` summary, forcing N
	/// follow-up `INFO FOR TABLE` calls.
	#[tokio::test]
	async fn database_schema_includes_per_table_fields() {
		use std::sync::Arc;

		use surrealdb_core::dbs::Session;
		use surrealdb_core::kvs::Datastore;

		use crate::session::McpSession;

		let ds = Arc::new(Datastore::new("memory").await.expect("datastore"));
		ds.execute("DEFINE NAMESPACE acme; DEFINE DATABASE prod;", &Session::owner(), None)
			.await
			.expect("seed ns/db");
		ds.execute(
			"DEFINE TABLE customer SCHEMAFULL; \
			 DEFINE FIELD email ON customer TYPE string; \
			 DEFINE INDEX customer_email_unique ON customer FIELDS email UNIQUE;",
			&Session::owner().with_ns("acme").with_db("prod"),
			None,
		)
		.await
		.expect("seed schema");

		let session = McpSession::new(ds, Session::owner());
		let body = get_database_schema(&session, "acme", "prod").await.expect("schema");
		let json: serde_json::Value = serde_json::from_str(&body).expect("body must be valid JSON");
		let customer = json
			.pointer("/schema/tables/customer")
			.expect("customer entry must exist after enrichment");

		assert!(
			customer.is_object(),
			"customer must be an object after enrichment, got: {customer}"
		);
		// The original DDL string is preserved under `definition`.
		assert!(
			customer
				.get("definition")
				.and_then(|v| v.as_str())
				.map(|s| s.starts_with("DEFINE TABLE customer"))
				.unwrap_or(false),
			"definition string must round-trip"
		);
		// Field DDL is now visible without a follow-up fetch.
		let email =
			customer.pointer("/fields/email").and_then(|v| v.as_str()).expect("email field DDL");
		assert!(email.contains("DEFINE FIELD email"));
		assert!(email.contains("string"));
		// And the index is too.
		assert!(
			customer
				.pointer("/indexes/customer_email_unique")
				.and_then(|v| v.as_str())
				.map(|s| s.contains("UNIQUE"))
				.unwrap_or(false)
		);
	}

	/// Regression: the database-level schema resource must enrich
	/// tables whose names require backtick quoting (spaces, hyphens,
	/// reserved keywords, leading digits). The keys come from the
	/// parsed `INFO FOR DB` response as bare strings (with backticks
	/// stripped by the parser); without re-quoting them on the
	/// outbound `INFO FOR TABLE ...` call the enrichment collapses
	/// to `{definition, error: "invalid table name: ..."}` and the
	/// resource silently loses the table's fields/indexes/events.
	#[tokio::test]
	async fn database_schema_enriches_backtick_quoted_tables() {
		use std::sync::Arc;

		use surrealdb_core::dbs::Session;
		use surrealdb_core::kvs::Datastore;

		use crate::session::McpSession;

		let ds = Arc::new(Datastore::new("memory").await.expect("datastore"));
		ds.execute("DEFINE NAMESPACE acme; DEFINE DATABASE prod;", &Session::owner(), None)
			.await
			.expect("seed ns/db");
		// Three forms that all need backtick quoting in SurrealQL:
		//   1. embedded space (`my table`),
		//   2. hyphen (`user-events`),
		//   3. reserved keyword (`order`).
		ds.execute(
			"DEFINE TABLE `my table` SCHEMAFULL; \
			 DEFINE FIELD title ON `my table` TYPE string; \
			 DEFINE TABLE `user-events` SCHEMAFULL; \
			 DEFINE FIELD kind ON `user-events` TYPE string; \
			 DEFINE TABLE `order` SCHEMAFULL; \
			 DEFINE FIELD total ON `order` TYPE decimal;",
			&Session::owner().with_ns("acme").with_db("prod"),
			None,
		)
		.await
		.expect("seed schema with backtick-named tables");

		let session = McpSession::new(ds, Session::owner());
		let body = get_database_schema(&session, "acme", "prod").await.expect("schema");
		let json: serde_json::Value = serde_json::from_str(&body).expect("body must be valid JSON");

		for (table, field) in [("my table", "title"), ("user-events", "kind"), ("order", "total")] {
			let entry = json
				.pointer(&format!("/schema/tables/{table}"))
				.unwrap_or_else(|| panic!("entry for `{table}` must exist after enrichment"));
			assert!(
				entry.is_object(),
				"`{table}` must be an enriched object, not the bare definition string; got: {entry}"
			);
			assert!(
				entry.get("error").is_none(),
				"`{table}` enrichment must succeed, not collapse to an error stub; got: {entry}"
			);
			let field_ddl = entry
				.pointer(&format!("/fields/{field}"))
				.and_then(|v| v.as_str())
				.unwrap_or_else(|| panic!("`{table}` must surface its `{field}` field DDL"));
			assert!(field_ddl.contains(&format!("DEFINE FIELD {field}")));
		}
	}

	/// Regression: a table name carrying characters that break our
	/// backtick wrapping (a literal backtick, newline, CR, or NUL) is
	/// not constructible via `DEFINE TABLE` in well-formed SurrealQL
	/// but the JSON keys come from the database, so we still defend
	/// against malformed stored state to avoid emitting a query the
	/// parser cannot read.
	#[tokio::test]
	async fn fetch_table_schema_rejects_quote_breaking_names() {
		use std::sync::Arc;

		use surrealdb_core::dbs::Session;
		use surrealdb_core::kvs::Datastore;

		use crate::session::McpSession;

		let ds = Arc::new(Datastore::new("memory").await.expect("datastore"));
		ds.execute("DEFINE NAMESPACE acme; DEFINE DATABASE prod;", &Session::owner(), None)
			.await
			.expect("seed ns/db");
		let session = McpSession::new(ds, Session::owner());
		for bad in ["with`tick", "with\nnewline", "with\rcr", "with\0nul"] {
			let err = fetch_table_schema(&session, "acme", "prod", bad)
				.await
				.expect_err("backtick-breaking name must be rejected");
			assert!(err.contains("invalid characters"), "expected sanity-check error, got: {err}");
		}
	}
}
