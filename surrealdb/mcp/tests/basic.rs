//! Integration tests for the MCP server.

use std::sync::Arc;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mcp::McpService;
use surrealdb_mcp::tools::{connection, crud, query, schema};

async fn test_datastore() -> Arc<Datastore> {
	Arc::new(Datastore::new("memory").await.expect("Failed to create datastore"))
}

fn root_session() -> Session {
	// For a default Session with an in-memory datastore, auth is typically
	// disabled which means anonymous sessions have full access.
	Session {
		ns: Some("test".to_string()),
		db: Some("test".to_string()),
		..Default::default()
	}
}

fn init_service(ds: Arc<Datastore>) -> McpService {
	let service = McpService::new(ds, Some("test".to_string()), Some("test".to_string()));
	service.init_session(root_session()).expect("Failed to init session");
	service
}

// ---------------------------------------------------------------------------
// Service lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_create_service() {
	let ds = test_datastore().await;
	let _service = McpService::new(ds, None, None);
}

#[tokio::test]
async fn test_init_session() {
	let ds = test_datastore().await;
	let service = McpService::new(ds, Some("test".to_string()), Some("test".to_string()));
	service.init_session(root_session()).expect("Failed to init session");

	// Second init should fail
	assert!(service.init_session(Session::default()).is_err());
}

#[tokio::test]
async fn test_default_ns_db() {
	let ds = test_datastore().await;
	let service =
		McpService::new(ds, Some("default_ns".to_string()), Some("default_db".to_string()));
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
	assert!(surrealdb_mcp::tools::validate_identifier("`quoted table`").is_ok());
}

#[test]
fn test_validate_identifier_rejects_injection() {
	assert!(surrealdb_mcp::tools::validate_identifier("person; DELETE FROM person").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("person\n DELETE FROM person").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("").is_err());
	assert!(surrealdb_mcp::tools::validate_identifier("tab\0le").is_err());
}

// ---------------------------------------------------------------------------
// JSON to SurrealDB Value conversion
// ---------------------------------------------------------------------------

#[test]
fn test_json_to_variables_object() {
	let json = serde_json::json!({"name": "John", "age": 30, "active": true});
	let vars = surrealdb_mcp::tools::json_to_variables(&json);
	assert!(vars.is_ok());
}

#[test]
fn test_json_to_variables_rejects_non_object() {
	let json = serde_json::json!("not an object");
	assert!(surrealdb_mcp::tools::json_to_variables(&json).is_err());

	let json = serde_json::json!([1, 2, 3]);
	assert!(surrealdb_mcp::tools::json_to_variables(&json).is_err());
}

// ---------------------------------------------------------------------------
// Tool execution
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_tool() {
	let ds = test_datastore().await;
	let service = init_service(ds);
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
	let service = init_service(ds);
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
	let service = init_service(ds);
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
	let service = init_service(ds);
	let session = service.session_ref().expect("session should be set");

	// Create a record
	crud::create(
		session,
		crud::CreateParams {
			target: "person".to_string(),
			data: Some(serde_json::json!({"name": "Bob"})),
		},
	)
	.await
	.expect("create should succeed");

	// Select it back
	let result = crud::select(
		session,
		crud::SelectParams {
			targets: "person".to_string(),
			fields: None,
			where_clause: None,
			order_clause: None,
			limit_clause: None,
			start_clause: None,
			group_clause: None,
			split_clause: None,
		},
	)
	.await;
	assert!(result.is_ok());
}

#[tokio::test]
async fn test_use_namespace_database() {
	let ds = test_datastore().await;
	let service = init_service(ds);
	let session = service.session_ref().expect("session should be set");

	let result = connection::use_namespace(
		session,
		connection::UseNamespaceParams {
			namespace: "new_ns".to_string(),
		},
	)
	.await;
	assert!(result.is_ok());

	let result = connection::use_database(
		session,
		connection::UseDatabaseParams {
			database: "new_db".to_string(),
		},
	)
	.await;
	assert!(result.is_ok());
}

#[tokio::test]
async fn test_version_tool() {
	let result = schema::version();
	// Should contain version info
	let text = format!("{result:?}");
	assert!(text.contains("SurrealDB"));
}

#[tokio::test]
async fn test_identifier_validation_in_crud() {
	let ds = test_datastore().await;
	let service = init_service(ds);
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
// Error sanitization
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_error_sanitization_in_results() {
	let ds = test_datastore().await;
	let service = init_service(ds);
	let session = service.session_ref().expect("session should be set");

	// Execute an invalid query that will produce an error
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
			// Should not contain file paths or stack traces
			assert!(!text.contains("src/"));
			assert!(!text.contains("panicked"));
		}
		Err(_) => {
			// ErrorData is also acceptable -- it goes through our sanitization
		}
	}
}
