//! End-to-end tests for the MCP server over an in-process stdio-compatible
//! transport.
//!
//! These tests drive `McpService` through the exact same code path as
//! `serve_stdio` by swapping real stdin/stdout for a `tokio::io::duplex` pipe.
//! The server-side `rmcp::ServiceExt::serve(service, io)` is identical; only
//! the underlying `AsyncRead`/`AsyncWrite` implementation differs, so the
//! JSON-RPC framing, request routing, and error sanitization are all
//! exercised for real.

#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]

mod common;

use std::time::Duration;

use common::test_datastore;
use rmcp::ServiceExt;
use rmcp::model::{
	CallToolRequestParams, ClientInfo, CompleteRequestParams, GetPromptRequestParams,
	PromptMessageContent, ReadResourceRequestParams, Reference, ResourceContents,
};
use rmcp::service::RunningService;
use serde_json::json;
use surrealdb_core::dbs::Session;
use surrealdb_mcp::McpService;
use tokio::time::timeout;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Spin up an `McpService` on one side of an in-memory duplex and return a
/// connected rmcp client peer plus the join handle for the server task.
///
/// With `auth_enabled = false` (the default for `Datastore::new("memory")`),
/// `Session::default()` behaves as owner, so the server can DEFINE schema for
/// the duration of the test without extra bootstrapping.
async fn spawn_server()
-> (RunningService<rmcp::RoleClient, ClientInfo>, tokio::task::JoinHandle<Result<(), String>>) {
	let ds = test_datastore().await;
	let service =
		McpService::new(ds, Some("test".to_string()), Some("test".to_string()), Session::owner());
	let (server_io, client_io) = tokio::io::duplex(8192);

	let server_handle = tokio::spawn(async move {
		let running = service.serve(server_io).await.map_err(|e| format!("serve: {e}"))?;
		running.waiting().await.map_err(|e| format!("wait: {e}"))?;
		Ok(())
	});

	// `ClientInfo` implements `ClientHandler` and provides a sensible default
	// InitializeRequest via `get_info()`, so we can drive the handshake
	// without constructing any non-exhaustive structs ourselves.
	let client =
		ClientInfo::default().serve(client_io).await.expect("client failed to complete handshake");
	(client, server_handle)
}

/// Collect all `text` fragments of a tool-call result into one string.
fn tool_text(result: &rmcp::model::CallToolResult) -> String {
	result
		.content
		.iter()
		.filter_map(|c| c.raw.as_text())
		.map(|t| t.text.as_str())
		.collect::<Vec<_>>()
		.join("\n")
}

/// Collect all `text` resource fragments.
fn resource_text(result: &rmcp::model::ReadResourceResult) -> String {
	result
		.contents
		.iter()
		.filter_map(|c| match c {
			ResourceContents::TextResourceContents {
				text,
				..
			} => Some(text.as_str()),
			_ => None,
		})
		.collect::<Vec<_>>()
		.join("\n")
}

// ---------------------------------------------------------------------------
// Handshake / capabilities
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stdio_handshake_reports_server_info() {
	let (client, server) = spawn_server().await;
	let info = client.peer_info().expect("server info should be set after handshake").clone();

	// Instructions are our curated LLM-facing docs -- must be non-empty.
	let instructions = info.instructions.expect("server should send instructions");
	assert!(!instructions.is_empty(), "instructions should be non-empty");
	assert!(
		instructions.contains("SurrealDB"),
		"instructions should mention SurrealDB: {instructions}"
	);

	// Tools, resources, prompts, completions must all be advertised.
	assert!(info.capabilities.tools.is_some(), "tools capability must be advertised");
	assert!(info.capabilities.resources.is_some(), "resources capability must be advertised");
	assert!(info.capabilities.prompts.is_some(), "prompts capability must be advertised");
	assert!(info.capabilities.completions.is_some(), "completions capability must be advertised");

	client.cancel().await.expect("client cancel");
	timeout(DEFAULT_TIMEOUT, server)
		.await
		.expect("server join timeout")
		.expect("server task panic")
		.ok();
}

// ---------------------------------------------------------------------------
// tools/list: exact published surface
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stdio_tools_list_matches_surface() {
	let (client, server) = spawn_server().await;
	let tools = client.list_all_tools().await.expect("list_all_tools");

	let mut names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
	names.sort_unstable();

	let mut expected = vec![
		"query", "select", "create", "insert", "upsert", "update", "delete", "relate", "info",
		"list", "use", "run",
	];
	expected.sort_unstable();
	assert_eq!(names, expected, "published tool surface drifted");

	// Regression guard: none of the removed legacy names should resurface.
	for stale in [
		"list_namespaces",
		"list_databases",
		"list_tables",
		"describe_table",
		"use_namespace",
		"use_database",
		"version",
		"explain",
	] {
		assert!(
			!tools.iter().any(|t| t.name.as_ref() == stale),
			"tool `{stale}` should have been removed"
		);
	}

	client.cancel().await.expect("client cancel");
	server.abort();
}

// ---------------------------------------------------------------------------
// tools/call: query / run / list / use / invalid params
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stdio_call_query() {
	let (client, server) = spawn_server().await;
	let args = json!({ "query": "RETURN 1 + 1" }).as_object().cloned().unwrap();
	let result = client
		.call_tool(CallToolRequestParams::new("query").with_arguments(args))
		.await
		.expect("query tool call");
	let text = tool_text(&result);
	assert!(text.contains('2'), "expected 2 in result: {text}");

	client.cancel().await.expect("client cancel");
	server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stdio_call_run_preserves_types() {
	let (client, server) = spawn_server().await;
	let args = json!({
		"function": "math::sum",
		"args": [[1, 2, 3, 4]]
	})
	.as_object()
	.cloned()
	.unwrap();
	let result = client
		.call_tool(CallToolRequestParams::new("run").with_arguments(args))
		.await
		.expect("run tool call");
	let text = tool_text(&result);
	assert!(text.contains("10"), "expected sum=10, got: {text}");

	client.cancel().await.expect("client cancel");
	server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stdio_call_list_tables_after_define() {
	let (client, server) = spawn_server().await;

	let define = json!({ "query": "DEFINE TABLE widget;" }).as_object().cloned().unwrap();
	client
		.call_tool(CallToolRequestParams::new("query").with_arguments(define))
		.await
		.expect("define table via query");

	let list = json!({ "kind": "tables" }).as_object().cloned().unwrap();
	let result = client
		.call_tool(CallToolRequestParams::new("list").with_arguments(list))
		.await
		.expect("list tables call");
	let text = tool_text(&result);
	assert!(text.contains("widget"), "expected `widget` in list output: {text}");

	client.cancel().await.expect("client cancel");
	server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stdio_call_use_switches_context() {
	// Pre-create the target NS/DB so the `use` tool's existence check
	// succeeds. The tool refuses to auto-provision, by design.
	let ds = test_datastore().await;
	ds.execute("DEFINE NAMESPACE ns2;", &Session::owner(), None).await.expect("seed ns2");
	ds.execute("DEFINE DATABASE db2;", &Session::owner().with_ns("ns2"), None)
		.await
		.expect("seed db2");
	let service =
		McpService::new(ds, Some("test".to_string()), Some("test".to_string()), Session::owner());
	let (server_io, client_io) = tokio::io::duplex(8192);
	let server = tokio::spawn(async move {
		let running = service.serve(server_io).await.map_err(|e| format!("serve: {e}"))?;
		running.waiting().await.map_err(|e| format!("wait: {e}"))?;
		Ok::<(), String>(())
	});
	let client =
		ClientInfo::default().serve(client_io).await.expect("client failed to complete handshake");

	let args = json!({ "namespace": "ns2", "database": "db2" }).as_object().cloned().unwrap();
	let result = client
		.call_tool(CallToolRequestParams::new("use").with_arguments(args))
		.await
		.expect("use call");
	let text = tool_text(&result);
	assert!(!result.is_error.unwrap_or(false), "use should succeed: {text}");
	assert!(text.contains("ns2"), "expected ns2 in result: {text}");
	assert!(text.contains("db2"), "expected db2 in result: {text}");

	client.cancel().await.expect("client cancel");
	server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stdio_call_use_rejects_nonexistent_namespace() {
	let (client, server) = spawn_server().await;

	let args = json!({ "namespace": "missing_ns" }).as_object().cloned().unwrap();
	let result = client
		.call_tool(CallToolRequestParams::new("use").with_arguments(args))
		.await
		.expect("use call should complete with an in-band error");
	assert_eq!(result.is_error, Some(true));
	let text = tool_text(&result).to_ascii_lowercase();
	assert!(text.contains("missing_ns") && text.contains("does not exist"), "got: {text}");

	client.cancel().await.expect("client cancel");
	server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stdio_call_invalid_params_returns_error() {
	let (client, server) = spawn_server().await;

	// Injection attempt via the function name -- must surface as a structured
	// error, not a panic or a raw executor message.
	let args =
		json!({ "function": "math::sum; DROP person", "args": [] }).as_object().cloned().unwrap();
	let err = client
		.call_tool(CallToolRequestParams::new("run").with_arguments(args))
		.await
		.expect_err("invalid function name should produce an error");

	let msg = format!("{err}");
	assert!(!msg.contains("src/"), "error must not leak source paths: {msg}");
	assert!(!msg.contains("panicked"), "error must not leak panics: {msg}");

	client.cancel().await.expect("client cancel");
	server.abort();
}

// ---------------------------------------------------------------------------
// resources/list + resources/read
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stdio_resources_list_and_read() {
	let (client, server) = spawn_server().await;

	// Seed a table so the schema endpoints have something to read.
	let seed = json!({ "query": "DEFINE TABLE cat;" }).as_object().cloned().unwrap();
	client
		.call_tool(CallToolRequestParams::new("query").with_arguments(seed))
		.await
		.expect("seed cat table");

	// Fixed, context-independent resources only. Schema URIs are advertised
	// via `resources/templates/list`, not `resources/list`, because they
	// require a `(namespace, database[, table])` expansion.
	let resources = client.list_all_resources().await.expect("list_all_resources");
	let uris: Vec<&str> = resources.iter().map(|r| r.uri.as_ref()).collect();
	let mut expected_static =
		vec!["surrealdb://instructions", "surrealdb://info", "surrealdb://version"];
	expected_static.sort_unstable();
	let mut actual_static = uris.clone();
	actual_static.sort_unstable();
	assert_eq!(actual_static, expected_static, "static resource surface drifted");

	for uri in &expected_static {
		let result = client
			.read_resource(ReadResourceRequestParams::new(*uri))
			.await
			.unwrap_or_else(|e| panic!("read {uri} failed: {e}"));
		let text = resource_text(&result);
		assert!(!text.is_empty(), "resource {uri} should have non-empty text");
	}

	// The parameterised schema URIs should expose both templates.
	let templates =
		client.list_all_resource_templates().await.expect("list_all_resource_templates");
	let template_uris: Vec<&str> = templates.iter().map(|t| t.uri_template.as_ref()).collect();
	for expected in [
		"surrealdb://schema/ns/{namespace}/db/{database}",
		"surrealdb://schema/ns/{namespace}/db/{database}/table/{table}",
	] {
		assert!(
			template_uris.contains(&expected),
			"missing resource template {expected} in {template_uris:?}"
		);
	}

	// Whole-database schema: must come back as self-describing JSON that
	// echoes the namespace/database from the URI (so clients caching by URI
	// cannot be tricked into misattributing the body).
	let db_schema = client
		.read_resource(ReadResourceRequestParams::new("surrealdb://schema/ns/test/db/test"))
		.await
		.expect("read database schema");
	let db_text = resource_text(&db_schema);
	assert!(!db_text.is_empty(), "database schema should be non-empty");
	let db_json: serde_json::Value =
		serde_json::from_str(&db_text).expect("database schema body must be valid JSON");
	assert_eq!(db_json.get("namespace").and_then(|v| v.as_str()), Some("test"));
	assert_eq!(db_json.get("database").and_then(|v| v.as_str()), Some("test"));
	assert!(db_json.get("schema").is_some(), "schema key missing: {db_text}");

	// Per-table schema. Body should likewise echo the full identity.
	let table_schema = client
		.read_resource(ReadResourceRequestParams::new(
			"surrealdb://schema/ns/test/db/test/table/cat",
		))
		.await
		.expect("read per-table schema");
	let table_text = resource_text(&table_schema);
	assert!(!table_text.is_empty(), "per-table schema should be non-empty");
	let table_json: serde_json::Value =
		serde_json::from_str(&table_text).expect("table schema body must be valid JSON");
	assert_eq!(table_json.get("namespace").and_then(|v| v.as_str()), Some("test"));
	assert_eq!(table_json.get("database").and_then(|v| v.as_str()), Some("test"));
	assert_eq!(table_json.get("table").and_then(|v| v.as_str()), Some("cat"));

	// Legacy session-scoped URIs must be rejected so clients get an explicit
	// "resource not found" instead of silently observing whichever NS/DB the
	// session happens to be on.
	let legacy = client.read_resource(ReadResourceRequestParams::new("surrealdb://schema")).await;
	assert!(legacy.is_err(), "legacy surrealdb://schema URI should no longer resolve");
	let legacy_table =
		client.read_resource(ReadResourceRequestParams::new("surrealdb://schema/cat")).await;
	assert!(
		legacy_table.is_err(),
		"legacy surrealdb://schema/<table> URI should no longer resolve"
	);

	client.cancel().await.expect("client cancel");
	server.abort();
}

// ---------------------------------------------------------------------------
// prompts/list + prompts/get
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stdio_prompts_list_and_get() {
	let (client, server) = spawn_server().await;

	let prompts = client.list_all_prompts().await.expect("list_all_prompts");
	let mut names: Vec<&str> = prompts.iter().map(|p| p.name.as_ref()).collect();
	names.sort_unstable();

	let mut expected = vec![
		"query_builder",
		"schema_explorer",
		"data_modeler",
		"transaction_guide",
		"graph_traversal",
		"search_guide",
	];
	expected.sort_unstable();
	assert_eq!(names, expected, "prompt surface drifted");

	// Getting an existing prompt must return a non-empty message whose text
	// refers only to the current tool surface.
	let result = client
		.get_prompt(GetPromptRequestParams::new("schema_explorer"))
		.await
		.expect("get schema_explorer prompt");
	assert!(!result.messages.is_empty(), "schema_explorer prompt must have messages");
	let combined: String = result
		.messages
		.iter()
		.filter_map(|m| match &m.content {
			PromptMessageContent::Text {
				text,
			} => Some(text.as_str()),
			_ => None,
		})
		.collect::<Vec<_>>()
		.join("\n");

	for stale in ["list_namespaces", "describe_table", "use_namespace", "explain"] {
		assert!(
			!combined.contains(stale),
			"schema_explorer prompt still references removed tool `{stale}`"
		);
	}

	// Unknown prompt names must surface as an error.
	let err = client
		.get_prompt(GetPromptRequestParams::new("does_not_exist"))
		.await
		.expect_err("unknown prompt should error");
	let msg = format!("{err}");
	assert!(msg.contains("does_not_exist"), "error should name the unknown prompt: {msg}");

	client.cancel().await.expect("client cancel");
	server.abort();
}

// ---------------------------------------------------------------------------
// completion/complete
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stdio_completion_suggests_tables() {
	let (client, server) = spawn_server().await;

	// Seed a pair of tables so completion has something to return.
	let seed =
		json!({ "query": "DEFINE TABLE alpha; DEFINE TABLE beta;" }).as_object().cloned().unwrap();
	client
		.call_tool(CallToolRequestParams::new("query").with_arguments(seed))
		.await
		.expect("seed tables");

	let req = CompleteRequestParams::new(
		Reference::for_prompt("query_builder"),
		rmcp::model::ArgumentInfo {
			name: "table".to_string(),
			value: String::new(),
		},
	);
	let result = client.complete(req).await.expect("complete");
	let values = result.completion.values;
	assert!(
		values.iter().any(|v| v == "alpha"),
		"expected `alpha` among completion values: {values:?}"
	);
	assert!(
		values.iter().any(|v| v == "beta"),
		"expected `beta` among completion values: {values:?}"
	);

	client.cancel().await.expect("client cancel");
	server.abort();
}

// ---------------------------------------------------------------------------
// Auth: STDIO path must use its configured base session when no HTTP parts
// are present on the request context (see issue where --user/--pass were
// ignored and every tool call failed due to Session::default()).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stdio_uses_base_session_when_auth_enabled_and_guest_disabled() {
	use std::sync::Arc;

	use surrealdb_core::dbs::Capabilities;
	use surrealdb_core::kvs::Datastore;

	// Datastore with auth *enabled* and guest access *disabled* -- the worst
	// case for the old behaviour where `Session::default()` was used as the
	// fallback. With the fix in place, the STDIO path should supply
	// `Session::owner()` as the base session and every tool call should work.
	let ds = Arc::new(
		Datastore::builder()
			.with_auth(true)
			.with_capabilities(Capabilities::default())
			.build_with_path("memory")
			.await
			.expect("datastore"),
	);
	// Bootstrap NS/DB so `default_ns`/`default_db` resolve to something real.
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.expect("ns");
	ds.execute("DEFINE DATABASE test;", &Session::owner().with_ns("test"), None).await.expect("db");

	let service =
		McpService::new(ds, Some("test".to_string()), Some("test".to_string()), Session::owner());
	let (server_io, client_io) = tokio::io::duplex(8192);
	let server_handle = tokio::spawn(async move {
		let running = service.serve(server_io).await.map_err(|e| format!("serve: {e}"))?;
		running.waiting().await.map_err(|e| format!("wait: {e}"))?;
		Ok::<_, String>(())
	});
	let client =
		ClientInfo::default().serve(client_io).await.expect("client failed to complete handshake");

	// Any schema-modifying operation requires a non-anonymous session, so
	// this is a tight proof that the base session is actually threaded in.
	let args = json!({ "query": "DEFINE TABLE widget;" }).as_object().cloned().unwrap();
	let result = client
		.call_tool(CallToolRequestParams::new("query").with_arguments(args))
		.await
		.expect("define table must succeed with base owner session");
	let text = tool_text(&result);
	assert!(
		!text.to_lowercase().contains("not enough permissions"),
		"base session should be owner, got permission error: {text}"
	);

	client.cancel().await.expect("client cancel");
	server_handle.abort();
}

// ---------------------------------------------------------------------------
// Clean shutdown
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stdio_server_terminates_cleanly() {
	let (client, server) = spawn_server().await;
	// Dropping the client closes its side of the duplex; the server task must
	// then exit on its own within a reasonable timeout.
	client.cancel().await.expect("client cancel");
	let joined = timeout(DEFAULT_TIMEOUT, server).await.expect("server did not terminate in time");
	joined.expect("server task panicked").expect("server task returned an error");
}
