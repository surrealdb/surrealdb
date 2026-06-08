//! Integration tests for the MCP server.

#![recursion_limit = "256"]

mod common;

use common::{content_text, init_service, root_session, test_datastore};
use surrealdb_core::dbs::Session;
use surrealdb_mcp::McpService;
use surrealdb_mcp::tools::{connection, crud, query, run as run_tool, schema};

// ---------------------------------------------------------------------------
// Service lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_create_service() {
	let ds = test_datastore().await;
	let _service = McpService::new(ds, None, None, Session::owner());
}

#[tokio::test]
async fn test_init_session() {
	let ds = test_datastore().await;
	let service =
		McpService::new(ds, Some("test".to_string()), Some("test".to_string()), Session::owner());
	service.init_session(root_session()).expect("Failed to init session");

	// Second init should fail
	assert!(service.init_session(Session::default()).is_err());
}

#[tokio::test]
async fn test_default_ns_db() {
	let ds = test_datastore().await;
	let service = McpService::new(
		ds,
		Some("default_ns".to_string()),
		Some("default_db".to_string()),
		Session::owner(),
	);
	service.init_session(Session::default()).expect("Failed to init session");
}

// ---------------------------------------------------------------------------
// Identifier validation
// ---------------------------------------------------------------------------

#[test]
fn test_validate_identifier_valid() {
	assert!(surrealdb_mcp::tools::validate_identifier("person").is_ok());
	assert!(surrealdb_mcp::tools::validate_identifier("person:john").is_ok());
	assert!(surrealdb_mcp::tools::validate_identifier("my_table").is_ok());
	assert!(surrealdb_mcp::tools::validate_identifier("_private").is_ok());
	assert!(surrealdb_mcp::tools::validate_identifier("person:42").is_ok());
	assert!(
		surrealdb_mcp::tools::validate_identifier("person:550e8400-e29b-41d4-a716-446655440000")
			.is_ok()
	);
	assert!(surrealdb_mcp::tools::validate_identifier("`quoted table`").is_ok());
	assert!(surrealdb_mcp::tools::validate_identifier("`quoted table`:john").is_ok());
	assert!(surrealdb_mcp::tools::validate_identifier("person:`complex key`").is_ok());
	assert!(surrealdb_mcp::tools::validate_identifier("`quoted table`:`complex key`").is_ok());
}

#[test]
fn test_validate_identifier_rejects_injection() {
	// Statement terminators / control characters
	assert!(surrealdb_mcp::tools::validate_identifier("person; DELETE FROM person").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("person\n DELETE FROM person").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("tab\0le").is_err());

	// Intra-statement SurrealQL injection via spaces / keywords / operators
	assert!(surrealdb_mcp::tools::validate_identifier("person WHERE 1=1").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("person, admin").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("person FETCH ->knows->person").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("person --").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("person EXPLAIN").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("person->knows->person").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("person<-knows<-person").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("person:john WHERE 1=1").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("person:john, admin:bob").is_err());

	// Leading digits / invalid first character
	assert!(surrealdb_mcp::tools::validate_identifier("1person").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("-person").is_err());

	// Empty components
	assert!(surrealdb_mcp::tools::validate_identifier(":john").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("person:").is_err());

	// Malformed backtick quoting
	assert!(surrealdb_mcp::tools::validate_identifier("`unterminated").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("``").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("`bad\nbody`").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("`a`trailing").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("`a` `b`").is_err());

	// Complex record-id keys (objects, arrays, UUID prefix literals, ranges).
	// These must go through the raw `query` tool instead.
	assert!(surrealdb_mcp::tools::validate_identifier("person:{a: 1}").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("person:[1, 2]").is_err());
	assert!(
		surrealdb_mcp::tools::validate_identifier("person:u'550e8400-e29b-41d4-a716-446655440000'")
			.is_err()
	);
	assert!(surrealdb_mcp::tools::validate_identifier("person:1..10").is_err());
}

#[test]
fn test_validate_table_name_accepts_bare_and_quoted() {
	assert!(surrealdb_mcp::tools::validate_table_name("person").is_ok());
	assert!(surrealdb_mcp::tools::validate_table_name("my_table").is_ok());
	assert!(surrealdb_mcp::tools::validate_table_name("_private").is_ok());
	assert!(surrealdb_mcp::tools::validate_table_name("`quoted table`").is_ok());
	assert!(surrealdb_mcp::tools::validate_table_name("`with-dash and space`").is_ok());
}

#[test]
fn test_validate_table_name_rejects_record_ids_and_injection() {
	// Record IDs are not valid table names.
	assert!(surrealdb_mcp::tools::validate_table_name("person:john").is_err());
	assert!(surrealdb_mcp::tools::validate_table_name("`quoted table`:john").is_err());
	assert!(surrealdb_mcp::tools::validate_table_name("person:`complex key`").is_err());

	// Empty / leading digit.
	assert!(surrealdb_mcp::tools::validate_table_name("").is_err());
	assert!(surrealdb_mcp::tools::validate_table_name("1person").is_err());

	// Injection / whitespace / terminators.
	assert!(surrealdb_mcp::tools::validate_table_name("person; DROP TABLE person").is_err());
	assert!(surrealdb_mcp::tools::validate_table_name("person WHERE 1=1").is_err());
	assert!(surrealdb_mcp::tools::validate_table_name("person\n DELETE").is_err());

	// Malformed backticks.
	assert!(surrealdb_mcp::tools::validate_table_name("`unterminated").is_err());
	assert!(surrealdb_mcp::tools::validate_table_name("``").is_err());
	assert!(surrealdb_mcp::tools::validate_table_name("`a`trailing").is_err());
	assert!(surrealdb_mcp::tools::validate_table_name("`bad\nbody`").is_err());
}

// ---------------------------------------------------------------------------
// JSON to SurrealDB Value conversion
// ---------------------------------------------------------------------------

#[test]
fn test_json_to_variables_object() {
	use surrealdb_core::cnf::CommonConfig;
	use surrealdb_mcp::cnf::McpConfig;
	let mcp = McpConfig::default();
	let core = CommonConfig::default();
	let json = serde_json::json!({"name": "John", "age": 30, "active": true});
	let vars = surrealdb_mcp::tools::json_to_variables(&json, &mcp, &core);
	assert!(vars.is_ok());
}

#[test]
fn test_json_to_variables_rejects_non_object() {
	use surrealdb_core::cnf::CommonConfig;
	use surrealdb_mcp::cnf::McpConfig;
	let mcp = McpConfig::default();
	let core = CommonConfig::default();
	let json = serde_json::json!("not an object");
	assert!(surrealdb_mcp::tools::json_to_variables(&json, &mcp, &core).is_err());

	let json = serde_json::json!([1, 2, 3]);
	assert!(surrealdb_mcp::tools::json_to_variables(&json, &mcp, &core).is_err());
}

// ---------------------------------------------------------------------------
// Tool execution -- query / CRUD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_tool() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = query::execute(
		session,
		query::QueryParams {
			query: "RETURN 1 + 1".to_string(),
			parameters: None,
		},
	)
	.await;
	assert!(result.is_ok());
}

#[tokio::test]
async fn test_query_tool_with_params() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = query::execute(
		session,
		query::QueryParams {
			query: "RETURN $x + $y".to_string(),
			parameters: Some(serde_json::json!({"x": 10, "y": 20})),
		},
	)
	.await;
	assert!(result.is_ok());
}

#[tokio::test]
async fn test_create_with_data() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = crud::create(
		session,
		crud::CreateParams {
			target: "person".to_string(),
			data: Some(serde_json::json!({"name": "Alice", "age": 30})),
		},
	)
	.await;
	assert!(result.is_ok());
}

#[tokio::test]
async fn test_select_after_create() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	crud::create(
		session,
		crud::CreateParams {
			target: "person".to_string(),
			data: Some(serde_json::json!({"name": "Bob"})),
		},
	)
	.await
	.expect("create should succeed");

	let result = crud::select(
		session,
		crud::SelectParams {
			target: "person".to_string(),
			fields: None,
			where_clause: None,
			order_clause: None,
			limit_clause: None,
			start_clause: None,
			group_clause: None,
			split_clause: None,
			fetch_clause: None,
		},
	)
	.await;
	assert!(result.is_ok());
}

#[tokio::test]
async fn test_identifier_validation_in_crud() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = crud::create(
		session,
		crud::CreateParams {
			target: "person; DELETE FROM person".to_string(),
			data: None,
		},
	)
	.await;
	assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// use (polymorphic context switch)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_use_namespace_only() {
	let ds = test_datastore().await;
	// Pre-create target namespace so the existence check passes.
	ds.execute("DEFINE NAMESPACE other_ns;", &Session::owner(), None).await.expect("seed ns");
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = connection::r#use(
		session,
		connection::UseParams {
			namespace: Some("other_ns".to_string()),
			database: None,
		},
	)
	.await
	.expect("use should not raise a protocol error");
	assert!(!result.is_error.unwrap_or(false), "unexpected tool error: {result:?}");
	assert_eq!(session.current_ns().await.as_deref(), Some("other_ns"));
}

#[tokio::test]
async fn test_use_database_only() {
	let ds = test_datastore().await;
	// Pre-create target database under the default `test` namespace.
	ds.execute("DEFINE DATABASE other_db;", &Session::owner().with_ns("test"), None)
		.await
		.expect("seed db");
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = connection::r#use(
		session,
		connection::UseParams {
			namespace: None,
			database: Some("other_db".to_string()),
		},
	)
	.await
	.expect("use should not raise a protocol error");
	assert!(!result.is_error.unwrap_or(false), "unexpected tool error: {result:?}");
	assert_eq!(session.current_db().await.as_deref(), Some("other_db"));
}

#[tokio::test]
async fn test_use_both() {
	let ds = test_datastore().await;
	// Pre-create target namespace and database.
	ds.execute("DEFINE NAMESPACE new_ns;", &Session::owner(), None).await.expect("seed ns");
	ds.execute("DEFINE DATABASE new_db;", &Session::owner().with_ns("new_ns"), None)
		.await
		.expect("seed db");
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = connection::r#use(
		session,
		connection::UseParams {
			namespace: Some("new_ns".to_string()),
			database: Some("new_db".to_string()),
		},
	)
	.await
	.expect("use should not raise a protocol error");
	assert!(!result.is_error.unwrap_or(false), "unexpected tool error: {result:?}");
	assert_eq!(session.current_ns().await.as_deref(), Some("new_ns"));
	assert_eq!(session.current_db().await.as_deref(), Some("new_db"));
}

#[tokio::test]
async fn test_use_rejects_nonexistent_namespace() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = connection::r#use(
		session,
		connection::UseParams {
			namespace: Some("no_such_ns".to_string()),
			database: None,
		},
	)
	.await
	.expect("use should not raise a protocol error");
	assert_eq!(result.is_error, Some(true), "should surface as tool error");
	let text = content_text(&result);
	assert!(
		text.contains("no_such_ns") && text.to_ascii_lowercase().contains("does not exist"),
		"expected NotFound message, got: {text}"
	);
	// The session context must not change when the probe fails.
	assert_eq!(session.current_ns().await.as_deref(), Some("test"));
}

#[tokio::test]
async fn test_use_rejects_nonexistent_database() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = connection::r#use(
		session,
		connection::UseParams {
			namespace: None,
			database: Some("no_such_db".to_string()),
		},
	)
	.await
	.expect("use should not raise a protocol error");
	assert_eq!(result.is_error, Some(true));
	assert_eq!(session.current_db().await.as_deref(), Some("test"));
}

#[tokio::test]
async fn test_use_rejects_guest_when_guest_queries_denied() {
	// A datastore that denies arbitrary queries from guest subjects must
	// not let an anonymous MCP session pin a namespace/database either.
	use std::sync::Arc;

	use surrealdb_core::dbs::Capabilities;
	use surrealdb_core::dbs::capabilities::{ArbitraryQueryTarget, Targets};
	use surrealdb_core::kvs::Datastore;

	let ds = Arc::new(
		Datastore::builder()
			.with_capabilities(Capabilities::default().without_arbitrary_query(Targets::<
				ArbitraryQueryTarget,
			>::Some(
				[ArbitraryQueryTarget::Guest].into_iter().collect(),
			)))
			.build_with_path("memory")
			.await
			.expect("datastore"),
	);
	ds.execute("DEFINE NAMESPACE other_ns;", &Session::owner(), None).await.expect("seed ns");
	let service = McpService::new(ds, None, None, Session::owner());
	service.init_session(Session::default()).expect("init session");
	let session = service.session_ref().expect("session should be set");

	let result = connection::r#use(
		session,
		connection::UseParams {
			namespace: Some("other_ns".to_string()),
			database: None,
		},
	)
	.await
	.expect("use should not raise a protocol error");
	assert_eq!(result.is_error, Some(true), "guest session must not switch ns");
	let text = content_text(&result);
	assert!(
		text.to_ascii_lowercase().contains("not allowed"),
		"expected NotAllowed error, got: {text}"
	);
}

#[tokio::test]
async fn test_use_neither_errors() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = connection::r#use(
		session,
		connection::UseParams {
			namespace: None,
			database: None,
		},
	)
	.await;
	assert!(result.is_err());
}

#[tokio::test]
async fn test_use_rejects_injection_in_identifier() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = connection::r#use(
		session,
		connection::UseParams {
			namespace: Some("ns; DEFINE NAMESPACE pwn".to_string()),
			database: None,
		},
	)
	.await;
	assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// list (polymorphic schema enumeration)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_tables_empty() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Tables,
			table: None,
			scope: None,
		},
	)
	.await;
	assert!(result.is_ok());
}

#[tokio::test]
async fn test_list_tables_after_define() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	query::execute(
		session,
		query::QueryParams {
			query: "DEFINE TABLE cat;".to_string(),
			parameters: None,
		},
	)
	.await
	.expect("define should succeed");

	let result = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Tables,
			table: None,
			scope: None,
		},
	)
	.await
	.expect("list should succeed");
	let text = content_text(&result);
	assert!(text.contains("cat"), "expected `cat` in result: {text}");
}

/// Regression test for the `list` tool's structured-content shape:
/// it must be `{ items: [...], truncated: bool }` so it matches the
/// declared `output_schema`. Strict MCP clients that validate
/// `structured_content` against the advertised schema would otherwise
/// reject every `list` response.
#[tokio::test]
async fn test_list_structured_content_envelope_shape() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	query::execute(
		session,
		query::QueryParams {
			query: "DEFINE TABLE widget;".to_string(),
			parameters: None,
		},
	)
	.await
	.expect("define should succeed");

	let result = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Tables,
			table: None,
			scope: None,
		},
	)
	.await
	.expect("list should succeed");

	let structured = result.structured_content.as_ref().expect("list must emit structured content");
	assert!(
		structured.is_object(),
		"list structured_content must be an object envelope; got: {structured}"
	);
	assert_eq!(
		structured.get("truncated").and_then(|v| v.as_bool()),
		Some(false),
		"a small schema must not flag truncation; got: {structured}"
	);
	let items = structured.get("items").and_then(|v| v.as_array()).expect("items must be an array");
	assert!(!items.is_empty(), "expected at least the `widget` entry, got empty items array");
	for entry in items {
		assert!(
			entry.is_object(),
			"each list item must be an object (per output_schema); got: {entry}"
		);
	}
}

/// Regression test for the `list` tool's response cap: when the
/// serialised subtree exceeds `SURREAL_MCP_MAX_RESULT_BYTES`, the
/// envelope must surface `truncated: true` and replace `items` with
/// the standard truncation marker rather than blasting the LLM
/// context window with the full DDL blob.
#[tokio::test]
async fn test_list_truncates_oversized_subtree() {
	use std::sync::Arc;

	use surrealdb_mcp::cnf::McpConfig;

	// Build a service with a tiny `max_result_bytes` cap (256 bytes)
	// so a small handful of `DEFINE TABLE` definitions is guaranteed
	// to overflow without having to seed hundreds of tables.
	let ds = test_datastore().await;
	let config = McpConfig {
		max_result_bytes: Some(256),
		..McpConfig::default()
	};
	let service = McpService::new_with_config(
		ds,
		Some("test".to_string()),
		Some("test".to_string()),
		Session::owner(),
		Arc::new(config),
	);
	service.init_session(root_session()).expect("init session");
	let session = service.session_ref().expect("session should be set");

	// Seed enough tables that the structured `tables` subtree exceeds
	// the 256-byte cap. Each `DEFINE TABLE` emits a structured
	// definition object that's well over a few bytes serialised.
	for i in 0..32 {
		query::execute(
			session,
			query::QueryParams {
				query: format!("DEFINE TABLE table_{i};"),
				parameters: None,
			},
		)
		.await
		.expect("define table");
	}

	let result = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Tables,
			table: None,
			scope: None,
		},
	)
	.await
	.expect("list should succeed");
	let structured = result.structured_content.as_ref().expect("list must emit structured content");
	assert_eq!(
		structured.get("truncated").and_then(|v| v.as_bool()),
		Some(true),
		"oversized subtree must flag truncation; got: {structured}"
	);
	// `items` should now be the standard truncation marker rather
	// than the full array of definitions.
	let items = structured.get("items").expect("items key must exist");
	let marker =
		items.as_object().expect("truncated items must be a JSON object marker, not an array");
	assert_eq!(
		marker.get("$truncated").and_then(|v| v.as_bool()),
		Some(true),
		"truncation marker must carry the canonical $truncated flag"
	);
}

#[tokio::test]
async fn test_list_namespaces_and_databases() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let ns_result = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Namespaces,
			table: None,
			scope: None,
		},
	)
	.await;
	assert!(ns_result.is_ok());

	let db_result = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Databases,
			table: None,
			scope: None,
		},
	)
	.await;
	assert!(db_result.is_ok());
}

#[tokio::test]
async fn test_list_fields_requires_table() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let missing = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Fields,
			table: None,
			scope: None,
		},
	)
	.await;
	assert!(missing.is_err());
}

#[tokio::test]
async fn test_list_users_requires_scope() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let missing = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Users,
			table: None,
			scope: None,
		},
	)
	.await;
	assert!(missing.is_err());

	let with_scope = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Users,
			table: None,
			scope: Some(schema::ListScope::Db),
		},
	)
	.await;
	assert!(with_scope.is_ok());
}

#[tokio::test]
async fn test_list_rejects_inapplicable_params() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	// table set on a kind that doesn't take it
	let bad_table = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Tables,
			table: Some("person".to_string()),
			scope: None,
		},
	)
	.await;
	assert!(bad_table.is_err());

	// scope set on a kind that doesn't take it
	let bad_scope = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Tables,
			table: None,
			scope: Some(schema::ListScope::Db),
		},
	)
	.await;
	assert!(bad_scope.is_err());
}

#[tokio::test]
async fn test_list_fields_after_define() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	query::execute(
		session,
		query::QueryParams {
			query: "DEFINE TABLE widget SCHEMAFULL; DEFINE FIELD name ON widget TYPE string;"
				.to_string(),
			parameters: None,
		},
	)
	.await
	.expect("define should succeed");

	let result = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Fields,
			table: Some("widget".to_string()),
			scope: None,
		},
	)
	.await
	.expect("list should succeed");
	let text = content_text(&result);
	assert!(text.contains("name"), "expected `name` in result: {text}");
}

#[tokio::test]
async fn test_list_rejects_malicious_table() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Fields,
			table: Some("person; DROP TABLE person".to_string()),
			scope: None,
		},
	)
	.await;
	assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// run (polymorphic function invocation)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_run_builtin_no_args() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = run_tool::run(
		session,
		run_tool::RunParams {
			function: "time::now".to_string(),
			args: None,
		},
	)
	.await;
	assert!(result.is_ok());
}

#[tokio::test]
async fn test_run_builtin_with_args() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = run_tool::run(
		session,
		run_tool::RunParams {
			function: "math::sum".to_string(),
			args: Some(vec![serde_json::json!([1, 2, 3, 4])]),
		},
	)
	.await
	.expect("run should succeed");
	let text = content_text(&result);
	assert!(text.contains("10"), "expected sum=10 in result: {text}");
}

#[tokio::test]
async fn test_run_user_defined_function() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	query::execute(
		session,
		query::QueryParams {
			query: "DEFINE FUNCTION fn::double($x: number) { RETURN $x * 2; };".to_string(),
			parameters: None,
		},
	)
	.await
	.expect("define function should succeed");

	let result = run_tool::run(
		session,
		run_tool::RunParams {
			function: "fn::double".to_string(),
			args: Some(vec![serde_json::json!(21)]),
		},
	)
	.await
	.expect("run should succeed");
	let text = content_text(&result);
	assert!(text.contains("42"), "expected 42 in result: {text}");
}

#[tokio::test]
async fn test_run_rejects_invalid_function_name() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	// Injection attempt via the function name.
	let injection = run_tool::run(
		session,
		run_tool::RunParams {
			function: "math::sum; DELETE person".to_string(),
			args: None,
		},
	)
	.await;
	assert!(injection.is_err());

	// Parentheses / whitespace not allowed.
	let parens = run_tool::run(
		session,
		run_tool::RunParams {
			function: "math::sum()".to_string(),
			args: None,
		},
	)
	.await;
	assert!(parens.is_err());

	// Empty name.
	let empty = run_tool::run(
		session,
		run_tool::RunParams {
			function: String::new(),
			args: None,
		},
	)
	.await;
	assert!(empty.is_err());
}

#[tokio::test]
async fn test_run_arg_types_preserved() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	// If args were string-interpolated, numeric args would round-trip as strings
	// and type::is::number would return false.
	let result = run_tool::run(
		session,
		run_tool::RunParams {
			function: "type::is_number".to_string(),
			args: Some(vec![serde_json::json!(42)]),
		},
	)
	.await
	.expect("run should succeed");
	let text = content_text(&result);
	assert!(text.contains("true"), "expected true in result: {text}");
}

// ---------------------------------------------------------------------------
// info (dump scope)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_info_defaults_to_db() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = schema::info(
		session,
		schema::InfoParams {
			target: None,
		},
	)
	.await;
	assert!(result.is_ok());
}

// ---------------------------------------------------------------------------
// Version resource
// ---------------------------------------------------------------------------

#[test]
fn test_version_resource() {
	let text = surrealdb_mcp::resources::schema::get_version();
	assert!(text.contains("SurrealDB"));
}

#[tokio::test]
async fn test_get_table_schema_resource_backtick_quoted() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	query::execute(
		session,
		query::QueryParams {
			query: "DEFINE TABLE `my table` SCHEMAFULL; \
			        DEFINE FIELD name ON `my table` TYPE string;"
				.to_string(),
			parameters: None,
		},
	)
	.await
	.expect("define should succeed");

	let text =
		surrealdb_mcp::resources::schema::get_table_schema(session, "test", "test", "`my table`")
			.await
			.expect("schema read should succeed");
	// Body is self-describing JSON; assert both the context echo and that
	// the table's field definitions landed in the `schema` subtree.
	let json: serde_json::Value =
		serde_json::from_str(&text).expect("table schema body must be valid JSON");
	assert_eq!(json.get("namespace").and_then(|v| v.as_str()), Some("test"));
	assert_eq!(json.get("database").and_then(|v| v.as_str()), Some("test"));
	assert_eq!(json.get("table").and_then(|v| v.as_str()), Some("`my table`"));
	assert!(text.contains("name"), "expected `name` field in resource: {text}");
}

#[tokio::test]
async fn test_get_table_schema_resource_rejects_record_id() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	// A record id (`person:john`) is not a valid table name, so identifier
	// validation must reject it before it ever reaches the datastore.
	let err =
		surrealdb_mcp::resources::schema::get_table_schema(session, "test", "test", "person:john")
			.await
			.expect_err("record id must be rejected as a table name");
	assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
}

#[tokio::test]
async fn test_schema_resource_rejects_malformed_namespace() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	// Namespace / database identifiers go straight into `INFO FOR ...`, so
	// the resource reader must reject anything that isn't a valid
	// identifier before we touch the datastore.
	let err = surrealdb_mcp::resources::schema::get_database_schema(
		session,
		"bad; DROP NAMESPACE",
		"test",
	)
	.await
	.expect_err("malformed namespace must be rejected");
	assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
}

// ---------------------------------------------------------------------------
// Error sanitization
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_error_sanitization_in_results() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = query::execute(
		session,
		query::QueryParams {
			query: "INVALID SYNTAX HERE ???".to_string(),
			parameters: None,
		},
	)
	.await;

	// The query should either fail at the tool level (ErrorData) or succeed
	// with sanitized error text in the result. Either way, it should not
	// contain internal implementation details.
	match result {
		Ok(tool_result) => {
			let text = format!("{tool_result:?}");
			assert!(!text.contains("src/"));
			assert!(!text.contains("panicked"));
		}
		Err(_) => {
			// ErrorData is also acceptable -- it goes through our sanitization
		}
	}
}

// ---------------------------------------------------------------------------
// CRUD round-trips for insert / upsert / update / delete / relate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_round_trip() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = crud::insert(
		session,
		crud::InsertParams {
			target: "animal".to_string(),
			data: serde_json::json!([
				{"name": "cat", "legs": 4},
				{"name": "bee", "legs": 6}
			]),
			ignore: false,
			relation: false,
		},
	)
	.await
	.expect("insert should succeed");
	let text = content_text(&result);
	assert!(text.contains("cat"), "expected inserted rows in result: {text}");

	let select = crud::select(
		session,
		crud::SelectParams {
			target: "animal".to_string(),
			fields: None,
			where_clause: None,
			order_clause: Some("name ASC".to_string()),
			limit_clause: None,
			start_clause: None,
			group_clause: None,
			split_clause: None,
			fetch_clause: None,
		},
	)
	.await
	.expect("select should succeed");
	let select_text = content_text(&select);
	assert!(select_text.contains("bee"), "expected `bee` in select: {select_text}");
}

#[tokio::test]
async fn test_upsert_merge_then_read() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	crud::create(
		session,
		crud::CreateParams {
			target: "person:alice".to_string(),
			data: Some(serde_json::json!({"name": "Alice", "age": 30})),
		},
	)
	.await
	.expect("create should succeed");

	crud::upsert(
		session,
		crud::UpsertParams {
			target: "person:alice".to_string(),
			content_data: None,
			merge_data: Some(serde_json::json!({"nickname": "Ally"})),
			patch_data: None,
			where_clause: None,
		},
	)
	.await
	.expect("upsert merge should succeed");

	let select = crud::select(
		session,
		crud::SelectParams {
			target: "person:alice".to_string(),
			fields: None,
			where_clause: None,
			order_clause: None,
			limit_clause: None,
			start_clause: None,
			group_clause: None,
			split_clause: None,
			fetch_clause: None,
		},
	)
	.await
	.expect("select should succeed");
	let text = content_text(&select);
	assert!(text.contains("Ally"), "expected merged nickname in: {text}");
	assert!(text.contains("Alice"), "expected existing name preserved in: {text}");
}

#[tokio::test]
async fn test_upsert_requires_exactly_one_mode() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let none = crud::upsert(
		session,
		crud::UpsertParams {
			target: "person".to_string(),
			content_data: None,
			merge_data: None,
			patch_data: None,
			where_clause: None,
		},
	)
	.await;
	assert!(none.is_err(), "upsert without any mode must fail");

	let both = crud::upsert(
		session,
		crud::UpsertParams {
			target: "person".to_string(),
			content_data: Some(serde_json::json!({"a": 1})),
			merge_data: Some(serde_json::json!({"b": 2})),
			patch_data: None,
			where_clause: None,
		},
	)
	.await;
	assert!(both.is_err(), "upsert with multiple modes must fail");
}

#[tokio::test]
async fn test_update_content_replaces_record() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	crud::create(
		session,
		crud::CreateParams {
			target: "note:one".to_string(),
			data: Some(serde_json::json!({"title": "draft", "tags": ["a", "b"]})),
		},
	)
	.await
	.expect("create should succeed");

	crud::update(
		session,
		crud::UpdateParams {
			target: "note:one".to_string(),
			content_data: Some(serde_json::json!({"title": "final"})),
			merge_data: None,
			patch_data: None,
			where_clause: None,
		},
	)
	.await
	.expect("update content should succeed");

	let select = crud::select(
		session,
		crud::SelectParams {
			target: "note:one".to_string(),
			fields: None,
			where_clause: None,
			order_clause: None,
			limit_clause: None,
			start_clause: None,
			group_clause: None,
			split_clause: None,
			fetch_clause: None,
		},
	)
	.await
	.expect("select should succeed");
	let text = content_text(&select);
	assert!(text.contains("final"), "expected new title: {text}");
	assert!(!text.contains("draft"), "CONTENT should have replaced the record: {text}");
}

#[tokio::test]
async fn test_delete_round_trip() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	crud::create(
		session,
		crud::CreateParams {
			target: "tmp:1".to_string(),
			data: Some(serde_json::json!({"n": 1})),
		},
	)
	.await
	.expect("create should succeed");

	crud::delete(
		session,
		crud::DeleteParams {
			target: "tmp:1".to_string(),
			where_clause: None,
		},
	)
	.await
	.expect("delete should succeed");

	let select = crud::select(
		session,
		crud::SelectParams {
			target: "tmp:1".to_string(),
			fields: None,
			where_clause: None,
			order_clause: None,
			limit_clause: None,
			start_clause: None,
			group_clause: None,
			split_clause: None,
			fetch_clause: None,
		},
	)
	.await
	.expect("select after delete should succeed");
	let text = content_text(&select);
	assert!(
		text.contains("[]") || text.contains("null") || text.contains("NONE"),
		"expected empty result after delete: {text}"
	);
}

#[tokio::test]
async fn test_relate_creates_edge() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	crud::create(
		session,
		crud::CreateParams {
			target: "person:a".to_string(),
			data: None,
		},
	)
	.await
	.expect("seed a");
	crud::create(
		session,
		crud::CreateParams {
			target: "person:b".to_string(),
			data: None,
		},
	)
	.await
	.expect("seed b");

	let edge = crud::relate(
		session,
		crud::RelateParams {
			from: "person:a".to_string(),
			table: "knows".to_string(),
			with: "person:b".to_string(),
			content_data: Some(serde_json::json!({"since": 2024})),
		},
	)
	.await
	.expect("relate should succeed");
	let text = content_text(&edge);
	assert!(text.contains("knows"), "expected edge table in result: {text}");

	let select = crud::select(
		session,
		crud::SelectParams {
			target: "knows".to_string(),
			fields: None,
			where_clause: None,
			order_clause: None,
			limit_clause: None,
			start_clause: None,
			group_clause: None,
			split_clause: None,
			fetch_clause: None,
		},
	)
	.await
	.expect("select knows");
	let select_text = content_text(&select);
	assert!(select_text.contains("person"), "expected edge to reference person: {select_text}");
}

#[tokio::test]
async fn test_all_crud_tools_reject_injection() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");
	let bad = "t; DROP TABLE t";

	assert!(
		crud::select(
			session,
			crud::SelectParams {
				target: bad.to_string(),
				fields: None,
				where_clause: None,
				order_clause: None,
				limit_clause: None,
				start_clause: None,
				group_clause: None,
				split_clause: None,
				fetch_clause: None,
			},
		)
		.await
		.is_err()
	);
	assert!(
		crud::create(
			session,
			crud::CreateParams {
				target: bad.to_string(),
				data: None,
			},
		)
		.await
		.is_err()
	);
	assert!(
		crud::insert(
			session,
			crud::InsertParams {
				target: bad.to_string(),
				data: serde_json::json!([{}]),
				ignore: false,
				relation: false,
			},
		)
		.await
		.is_err()
	);
	assert!(
		crud::upsert(
			session,
			crud::UpsertParams {
				target: bad.to_string(),
				content_data: Some(serde_json::json!({"a": 1})),
				merge_data: None,
				patch_data: None,
				where_clause: None,
			},
		)
		.await
		.is_err()
	);
	assert!(
		crud::update(
			session,
			crud::UpdateParams {
				target: bad.to_string(),
				content_data: Some(serde_json::json!({"a": 1})),
				merge_data: None,
				patch_data: None,
				where_clause: None,
			},
		)
		.await
		.is_err()
	);
	assert!(
		crud::delete(
			session,
			crud::DeleteParams {
				target: bad.to_string(),
				where_clause: None,
			},
		)
		.await
		.is_err()
	);
	assert!(
		crud::relate(
			session,
			crud::RelateParams {
				from: bad.to_string(),
				table: "knows".to_string(),
				with: "person:b".to_string(),
				content_data: None,
			},
		)
		.await
		.is_err()
	);
	assert!(
		crud::relate(
			session,
			crud::RelateParams {
				from: "person:a".to_string(),
				table: bad.to_string(),
				with: "person:b".to_string(),
				content_data: None,
			},
		)
		.await
		.is_err()
	);
	assert!(
		crud::relate(
			session,
			crud::RelateParams {
				from: "person:a".to_string(),
				table: "knows".to_string(),
				with: bad.to_string(),
				content_data: None,
			},
		)
		.await
		.is_err()
	);
}

// ---------------------------------------------------------------------------
// info(target=...) for each scope variant
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_info_target_root() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = schema::info(
		session,
		schema::InfoParams {
			target: Some("root".to_string()),
		},
	)
	.await
	.expect("info root should succeed");
	let text = content_text(&result);
	assert!(text.contains("namespaces"), "expected `namespaces` in ROOT info: {text}");
}

#[tokio::test]
async fn test_info_target_ns() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = schema::info(
		session,
		schema::InfoParams {
			target: Some("ns".to_string()),
		},
	)
	.await
	.expect("info ns should succeed");
	let text = content_text(&result);
	assert!(text.contains("databases"), "expected `databases` in NS info: {text}");
}

#[tokio::test]
async fn test_info_target_db() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = schema::info(
		session,
		schema::InfoParams {
			target: Some("db".to_string()),
		},
	)
	.await
	.expect("info db should succeed");
	let text = content_text(&result);
	assert!(text.contains("tables"), "expected `tables` in DB info: {text}");
}

#[tokio::test]
async fn test_info_target_table() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	query::execute(
		session,
		query::QueryParams {
			query: "DEFINE TABLE widget SCHEMAFULL; DEFINE FIELD name ON widget TYPE string;"
				.to_string(),
			parameters: None,
		},
	)
	.await
	.expect("define should succeed");

	let result = schema::info(
		session,
		schema::InfoParams {
			target: Some("widget".to_string()),
		},
	)
	.await
	.expect("info <table> should succeed");
	let text = content_text(&result);
	assert!(text.contains("name"), "expected `name` field in table info: {text}");
}

#[tokio::test]
async fn test_info_target_backtick_quoted_table() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	query::execute(
		session,
		query::QueryParams {
			query: "DEFINE TABLE `my table` SCHEMAFULL; \
			        DEFINE FIELD name ON `my table` TYPE string;"
				.to_string(),
			parameters: None,
		},
	)
	.await
	.expect("define should succeed");

	let result = schema::info(
		session,
		schema::InfoParams {
			target: Some("`my table`".to_string()),
		},
	)
	.await
	.expect("info on backtick-quoted table should succeed");
	let text = content_text(&result);
	assert!(text.contains("name"), "expected `name` field in table info: {text}");
}

#[tokio::test]
async fn test_list_fields_backtick_quoted_table() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	query::execute(
		session,
		query::QueryParams {
			query: "DEFINE TABLE `my table` SCHEMAFULL; \
			        DEFINE FIELD name ON `my table` TYPE string;"
				.to_string(),
			parameters: None,
		},
	)
	.await
	.expect("define should succeed");

	let result = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Fields,
			table: Some("`my table`".to_string()),
			scope: None,
		},
	)
	.await
	.expect("list fields on backtick-quoted table should succeed");
	let text = content_text(&result);
	assert!(text.contains("name"), "expected `name` in result: {text}");
}

#[tokio::test]
async fn test_info_target_rejects_record_id() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	// Record IDs are not valid table-name targets for INFO FOR TABLE, so
	// they must be rejected at the validator.
	let result = schema::info(
		session,
		schema::InfoParams {
			target: Some("person:john".to_string()),
		},
	)
	.await;
	assert!(result.is_err());
}

#[tokio::test]
async fn test_info_target_rejects_injection() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = schema::info(
		session,
		schema::InfoParams {
			target: Some("t; DROP TABLE t".to_string()),
		},
	)
	.await;
	assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// list (additional DB-scope kinds)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_functions() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	query::execute(
		session,
		query::QueryParams {
			query: "DEFINE FUNCTION fn::hello() { RETURN 'hi'; };".to_string(),
			parameters: None,
		},
	)
	.await
	.expect("define function should succeed");

	let result = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Functions,
			table: None,
			scope: None,
		},
	)
	.await
	.expect("list functions should succeed");
	let text = content_text(&result);
	assert!(text.contains("hello"), "expected `hello` in functions: {text}");
}

#[tokio::test]
async fn test_list_analyzers() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	query::execute(
		session,
		query::QueryParams {
			query: "DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;".to_string(),
			parameters: None,
		},
	)
	.await
	.expect("define analyzer should succeed");

	let result = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Analyzers,
			table: None,
			scope: None,
		},
	)
	.await
	.expect("list analyzers should succeed");
	let text = content_text(&result);
	assert!(text.contains("simple"), "expected `simple` in analyzers: {text}");
}

#[tokio::test]
async fn test_list_params() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	query::execute(
		session,
		query::QueryParams {
			query: "DEFINE PARAM $maximum VALUE 100;".to_string(),
			parameters: None,
		},
	)
	.await
	.expect("define param should succeed");

	let result = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Params,
			table: None,
			scope: None,
		},
	)
	.await
	.expect("list params should succeed");
	let text = content_text(&result);
	assert!(text.contains("maximum"), "expected `maximum` in params: {text}");
}

#[tokio::test]
async fn test_list_configs_does_not_error() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	// No configs are defined by default, but the subtree should still resolve
	// rather than error out.
	let result = schema::list(
		session,
		schema::ListParams {
			kind: schema::ListKind::Configs,
			table: None,
			scope: None,
		},
	)
	.await
	.expect("list configs should succeed");
	let text = content_text(&result);
	assert!(!text.is_empty(), "list configs should return textual JSON");
}

// ---------------------------------------------------------------------------
// run permissions: custom function with PERMISSIONS NONE must be rejected for
// a non-owner session
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_run_respects_function_permissions() {
	use std::sync::Arc;

	use surrealdb_core::dbs::Capabilities;
	use surrealdb_core::kvs::Datastore;

	// Build a datastore with auth *enabled* so `PERMISSIONS NONE` bites.
	// `Capabilities::all()` grants guest access so anonymous sessions are
	// allowed into the DB, but function-level PERMISSIONS NONE still applies.
	let ds = Arc::new(
		Datastore::builder()
			.with_auth(true)
			.with_capabilities(Capabilities::all())
			.build_with_path("memory")
			.await
			.expect("datastore"),
	);

	// Owner session bootstraps NS/DB and defines the locked-down function.
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.expect("bootstrap NS");
	ds.execute("DEFINE DATABASE test;", &Session::owner().with_ns("test"), None)
		.await
		.expect("bootstrap DB");
	ds.execute(
		"DEFINE FUNCTION fn::secret() { RETURN 1; } PERMISSIONS NONE;",
		&Session::owner().with_ns("test").with_db("test"),
		None,
	)
	.await
	.expect("define locked function");

	// Anonymous session should not be able to invoke fn::secret.
	let service =
		McpService::new(ds, Some("test".to_string()), Some("test".to_string()), Session::default());
	service.init_session(Session::default().with_ns("test").with_db("test")).expect("init");
	let session = service.session_ref().expect("session");

	let result = run_tool::run(
		session,
		run_tool::RunParams {
			function: "fn::secret".to_string(),
			args: None,
		},
	)
	.await;

	match result {
		Ok(tool_result) => {
			let text = content_text(&tool_result);
			assert!(!text.contains("src/"), "must not leak source paths: {text}");
			assert!(!text.contains("panicked"), "must not leak panics: {text}");
			assert!(
				text.to_lowercase().contains("error")
					|| text.to_lowercase().contains("permission")
					|| text.to_lowercase().contains("denied"),
				"expected permission error text, got: {text}"
			);
		}
		Err(err) => {
			let msg = format!("{err}");
			assert!(!msg.contains("src/"), "must not leak source paths: {msg}");
			assert!(!msg.contains("panicked"), "must not leak panics: {msg}");
		}
	}
}

// ---------------------------------------------------------------------------
// Output formatting: failing statements, mixed results, truncation
// ---------------------------------------------------------------------------

/// A failing statement is surfaced as an in-band tool error (is_error=true)
/// with the real SurrealDB error message, *not* as a JSON-RPC protocol
/// error. This is what lets the LLM self-correct.
#[tokio::test]
async fn test_failing_statement_surfaces_as_tool_error() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = query::execute(
		session,
		query::QueryParams {
			// Permissions error: record IDs aren't numbers.
			query: "CREATE person:1 SET age = 'not a number' + 5".to_string(),
			parameters: None,
		},
	)
	.await
	.expect("failing statements should still return Ok(CallToolResult)");

	assert_eq!(result.is_error, Some(true), "single failing statement must set is_error");
	let text = content_text(&result);
	assert!(
		text.to_ascii_lowercase().contains("error"),
		"expected human-readable error text, got: {text}"
	);
	// Must expose the real SurrealDB error message so the LLM can correct.
	assert!(
		!text.to_lowercase().contains("operation failed"),
		"must not be sanitized to a generic message: {text}"
	);
	let structured = result.structured_content.as_ref().expect("structured content must be set");
	assert_eq!(structured.get("status").and_then(|v| v.as_str()), Some("error"));
	assert!(structured.get("kind").and_then(|v| v.as_str()).is_some(), "kind must be set");
}

/// A multi-statement query mixing successes and failures stays is_error=false
/// (partial success), and each entry carries its per-statement status so the
/// LLM sees exactly which statement broke.
#[tokio::test]
async fn test_mixed_statement_results_expose_per_statement_status() {
	let ds = test_datastore().await;
	let service = init_service(ds).await;
	let session = service.session_ref().expect("session should be set");

	let result = query::execute(
		session,
		query::QueryParams {
			query: "RETURN 1; THROW 'boom'; RETURN 2;".to_string(),
			parameters: None,
		},
	)
	.await
	.expect("multi-statement query should return Ok");

	// Partial success: is_error stays false even though one statement failed.
	assert_ne!(result.is_error, Some(true), "partial success must not raise is_error");

	let structured = result.structured_content.as_ref().expect("structured content must be set");
	assert_eq!(structured.get("has_errors").and_then(|v| v.as_bool()), Some(true));
	let entries = structured.get("results").and_then(|v| v.as_array()).expect("results array");
	assert_eq!(entries.len(), 3);
	assert_eq!(entries[0].get("status").and_then(|v| v.as_str()), Some("ok"));
	assert_eq!(entries[1].get("status").and_then(|v| v.as_str()), Some("error"));
	assert_eq!(entries[2].get("status").and_then(|v| v.as_str()), Some("ok"));
	// Real error text must be present, not a generic "Operation failed".
	let err_text = entries[1].get("error").and_then(|v| v.as_str()).unwrap_or("");
	assert!(
		err_text.to_ascii_lowercase().contains("boom"),
		"expected real THROW message in second statement: {err_text}"
	);
}

// Note: the serialised-result truncation path is exercised directly by the
// unit tests in `src/tools/output.rs` (`cap_value_truncates_large_payloads`
// and `single_statement_result_surfaces_truncation`). Those tests bypass
// the datastore -- whose independent per-call allocation limits make it
// awkward to construct a payload that is simultaneously below the engine's
// limits and above the MCP response cap -- and are the authoritative
// regression surface for the MCP truncation contract.
