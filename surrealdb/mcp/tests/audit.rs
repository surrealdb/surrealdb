//! Audit-pipeline regression tests.
//!
//! These tests pin the README contract that "every tool invocation
//! emits exactly one structured `tracing::info!` record on the
//! `surrealdb::mcp::audit` target" — including, critically, the
//! credential-mismatch rejection path that the spec-mandated
//! session-hijack defence relies on operators detecting in their SIEM.
//!
//! Audit capture is process-global (rmcp's HTTP transport spawns
//! worker tasks whose poll thread isn't pinned to the test thread, so
//! a thread-local subscriber would drop events). Putting these tests
//! in their own test binary keeps them isolated from the
//! tool-invoking tests in `tests/http.rs`, whose audit emissions
//! would otherwise leak into our shared buffer.

#![cfg(feature = "server-http")]
#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]

use std::sync::Arc;

use bytes::Bytes;
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use serde_json::{Value, json};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mcp::service::{McpHttpService, create_http_service};

mod common;
use common::{install_audit_capture, test_datastore};

fn post_request(
	body: &Value,
	session_id: Option<&str>,
	session: Option<Session>,
) -> Request<Full<Bytes>> {
	let mut builder = Request::builder()
		.method(Method::POST)
		.uri("/mcp")
		.header("host", "localhost")
		.header("content-type", "application/json")
		.header("accept", "application/json, text/event-stream");
	if let Some(id) = session_id {
		builder = builder.header("mcp-session-id", id);
	}
	let mut req = builder.body(Full::new(Bytes::from(body.to_string()))).unwrap();
	if let Some(s) = session {
		req.extensions_mut().insert(s);
	}
	req
}

async fn body_to_string(
	resp: http::Response<http_body_util::combinators::BoxBody<Bytes, std::convert::Infallible>>,
) -> String {
	let bytes = resp.into_body().collect().await.unwrap().to_bytes();
	String::from_utf8(bytes.to_vec()).unwrap_or_default()
}

async fn initialize(service: &McpHttpService, attach_session: Option<Session>) -> String {
	let body = json!({
		"jsonrpc": "2.0",
		"id": 1,
		"method": "initialize",
		"params": {
			"protocolVersion": "2025-06-18",
			"capabilities": {},
			"clientInfo": { "name": "audit-test", "version": "0.0.0" },
		},
	});
	let resp = service.handle(post_request(&body, None, attach_session)).await;
	assert_eq!(resp.status(), StatusCode::OK, "initialize should return 200");
	let session_id = resp
		.headers()
		.get("mcp-session-id")
		.expect("initialize must allocate a session id")
		.to_str()
		.unwrap()
		.to_string();
	// Drain the body so the rmcp worker future completes before we
	// move on to the handshake notification.
	let _ = body_to_string(resp).await;
	session_id
}

async fn complete_handshake(service: &McpHttpService, session_id: &str) {
	let notif = json!({
		"jsonrpc": "2.0",
		"method": "notifications/initialized",
	});
	let resp = service.handle(post_request(&notif, Some(session_id), None)).await;
	assert_eq!(resp.status(), StatusCode::ACCEPTED);
	let _ = body_to_string(resp).await;
}

fn setup_service(ds: Arc<Datastore>) -> McpHttpService {
	create_http_service(ds)
}

fn owner_session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

/// A second authenticated session with a *different* identity than
/// `owner_session()`. Used to drive the credential-mismatch path.
fn db_user_session() -> Session {
	use surrealdb_core::iam::{Auth, Role};
	Session {
		au: std::sync::Arc::new(Auth::for_db(Role::Editor, "test", "test")),
		ns: Some("test".into()),
		db: Some("test".into()),
		..Session::default()
	}
}

/// Regression test for the audit-pipeline contract: a credential
/// mismatch on a live session must still emit exactly one structured
/// audit record on the `surrealdb::mcp::audit` target with
/// `outcome = protocol_error` and `kind = INVALID_PARAMS`. The
/// README pins this contract ("Every tool invocation emits exactly
/// one structured `tracing::info!` record on the
/// `surrealdb::mcp::audit` target") and it is the SIEM detection
/// surface for the spec-mandated session-hijack defence; silently
/// dropping the rejection event would undermine that defence.
#[tokio::test]
async fn credential_mismatch_emits_audit_record() {
	// Take the audit-capture lock *before* spinning up the service so
	// the subscriber is exclusive to us for the entire test,
	// including any worker tasks rmcp's transport spawns. The lock
	// also drains any leftover events from a previous test.
	let audit = install_audit_capture();

	let ds = test_datastore().await;
	let service = setup_service(ds);
	let session_id = initialize(&service, Some(owner_session())).await;
	complete_handshake(&service, &session_id).await;

	let req = json!({
		"jsonrpc": "2.0",
		"id": 11,
		"method": "tools/call",
		"params": { "name": "query", "arguments": { "query": "RETURN 1" } },
	});
	let resp = service.handle(post_request(&req, Some(&session_id), Some(db_user_session()))).await;
	assert_eq!(resp.status(), StatusCode::OK);
	// Drain the response body fully. rmcp's transport processes the
	// tools/call on a worker task that emits the audit record while
	// streaming the SSE response, so consuming the body forces the
	// worker future to completion before we sample the buffer.
	let body = body_to_string(resp).await;
	assert!(
		body.contains("Credentials do not match"),
		"body must surface the credential-mismatch error: {body}"
	);

	let audit_events = audit.audit_events();
	assert_eq!(
		audit_events.len(),
		1,
		"credential-mismatch dispatch must emit exactly one audit event; got: {audit_events:#?}"
	);
	let event = &audit_events[0];
	assert_eq!(event.field("tool"), Some("query"), "audit event must carry the tool name");
	assert_eq!(
		event.field("outcome"),
		Some("protocol_error"),
		"credential-mismatch must classify as protocol_error"
	);
	assert_eq!(
		event.field("kind"),
		Some("INVALID_PARAMS"),
		"audit kind must be the well-known JSON-RPC label"
	);
	// The bound subject (owner) should be visible to operators
	// forwarding the audit feed so they can correlate the rejection
	// with the bound session, not the impersonating subject.
	assert!(
		event.field("subject").map(|s| !s.is_empty()).unwrap_or(false),
		"audit event must carry the bound subject label"
	);
}

/// Symmetric positive case for the audit-pipeline contract: a
/// successful tool call must emit exactly one audit record with
/// `outcome = ok`. Establishes that the regression test above is
/// detecting the protocol_error path specifically rather than just
/// "any audit emission".
#[tokio::test]
async fn successful_tool_call_emits_audit_record() {
	let audit = install_audit_capture();

	let ds = test_datastore().await;
	let service = setup_service(ds);
	let session_id = initialize(&service, Some(owner_session())).await;
	complete_handshake(&service, &session_id).await;

	let req = json!({
		"jsonrpc": "2.0",
		"id": 12,
		"method": "tools/call",
		"params": { "name": "query", "arguments": { "query": "RETURN 1" } },
	});
	let resp = service.handle(post_request(&req, Some(&session_id), Some(owner_session()))).await;
	assert_eq!(resp.status(), StatusCode::OK);
	// Drain the body so the rmcp worker future completes and the
	// audit record lands in the buffer before we sample it.
	let _ = body_to_string(resp).await;

	let audit_events = audit.audit_events();
	assert_eq!(
		audit_events.len(),
		1,
		"successful dispatch must emit exactly one audit event; got: {audit_events:#?}"
	);
	let event = &audit_events[0];
	assert_eq!(event.field("tool"), Some("query"));
	assert_eq!(event.field("outcome"), Some("ok"));
}
