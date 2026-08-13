//! Integration tests for the MCP HTTP transport.
//!
//! These tests drive `StreamableHttpService` directly via its `handle`
//! method using hand-built [`http::Request`]s, so every test exercises the
//! exact same code path that [`surrealdb_mcp::service::create_http_service`]
//! exposes behind SurrealDB's `SurrealAuth` middleware. Auth plumbing is
//! tested by pre-injecting a `Session` into `request.extensions()`, which
//! is what `SurrealAuth` does in production.

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
use surrealdb_mcp::cnf::McpConfig;
use surrealdb_mcp::service::{
	McpHttpService, create_http_service, create_http_service_with_config,
};

mod common;
use common::test_datastore;

/// Build a POST /mcp request carrying a JSON-RPC body.
fn post_request(
	body: &Value,
	session_id: Option<&str>,
	session: Option<Session>,
) -> Request<Full<Bytes>> {
	post_request_with_headers(body, session_id, session, &[])
}

/// [`post_request`] plus arbitrary extra headers, for the protocol-revision and
/// SEP-2243 headers only a few tests need. The base header set is shared so an
/// rmcp change to what `StreamableHttpService` requires of a POST is a one-line
/// fix rather than one per hand-rolled builder.
fn post_request_with_headers(
	body: &Value,
	session_id: Option<&str>,
	session: Option<Session>,
	extra_headers: &[(&str, &str)],
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
	for (name, value) in extra_headers {
		builder = builder.header(*name, *value);
	}
	let mut req = builder.body(Full::new(Bytes::from(body.to_string()))).unwrap();
	if let Some(s) = session {
		// Mirror what the SurrealAuth middleware does in production: attach
		// an authenticated session to the request extensions so the MCP
		// service can pick it up during `initialize`.
		req.extensions_mut().insert(s);
	}
	req
}

fn delete_request(session_id: &str) -> Request<Full<Bytes>> {
	Request::builder()
		.method(Method::DELETE)
		.uri("/mcp")
		.header("host", "localhost")
		.header("mcp-session-id", session_id)
		.body(Full::new(Bytes::new()))
		.unwrap()
}

fn get_request(session_id: &str) -> Request<Full<Bytes>> {
	Request::builder()
		.method(Method::GET)
		.uri("/mcp")
		.header("host", "localhost")
		.header("accept", "text/event-stream")
		.header("mcp-session-id", session_id)
		.body(Full::new(Bytes::new()))
		.unwrap()
}

/// Collect the response body into a single `String`.
async fn body_to_string(
	resp: http::Response<http_body_util::combinators::BoxBody<Bytes, std::convert::Infallible>>,
) -> String {
	let bytes = resp.into_body().collect().await.unwrap().to_bytes();
	String::from_utf8(bytes.to_vec()).unwrap_or_default()
}

/// Extract the last non-empty `data: ...` payload out of an SSE body and
/// parse it as JSON. An SSE stream may include keep-alive or retry frames
/// before the actual response; the JSON-RPC reply is always on a `data:`
/// line that parses successfully.
fn parse_sse_json(body: &str) -> Value {
	let mut last: Option<Value> = None;
	for line in body.lines() {
		if let Some(rest) = line.strip_prefix("data: ").map(|s| s.trim())
			&& !rest.is_empty()
			&& let Ok(v) = serde_json::from_str::<Value>(rest)
		{
			last = Some(v);
		}
	}
	last.unwrap_or_else(|| serde_json::from_str(body).unwrap_or(Value::Null))
}

/// Send an `initialize` POST and return (session-id, parsed body).
async fn initialize(service: &McpHttpService, attach_session: Option<Session>) -> (String, Value) {
	initialize_with_version(service, "2025-06-18", attach_session).await
}

/// [`initialize`] naming a specific protocol revision. A revision outside
/// `SUPPORTED_PROTOCOL_VERSIONS` exercises rmcp's negotiation fallback, which
/// answers with the server's advertised revision.
async fn initialize_with_version(
	service: &McpHttpService,
	version: &str,
	attach_session: Option<Session>,
) -> (String, Value) {
	let body = json!({
		"jsonrpc": "2.0",
		"id": 1,
		"method": "initialize",
		"params": {
			"protocolVersion": version,
			"capabilities": {},
			"clientInfo": { "name": "http-test", "version": "0.0.0" },
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
	let body = body_to_string(resp).await;
	let parsed = parse_sse_json(&body);
	(session_id, parsed)
}

fn setup_service(ds: Arc<Datastore>) -> McpHttpService {
	create_http_service(ds)
}

/// Build an `initialize` POST carrying an explicit `Host` header, used to
/// exercise rmcp's DNS-rebinding allowlist.
fn initialize_with_host(host: &str) -> Request<Full<Bytes>> {
	let body = json!({
		"jsonrpc": "2.0",
		"id": 1,
		"method": "initialize",
		"params": {
			"protocolVersion": "2025-06-18",
			"capabilities": {},
			"clientInfo": { "name": "http-test", "version": "0.0.0" },
		},
	});
	Request::builder()
		.method(Method::POST)
		.uri("/mcp")
		.header("host", host)
		.header("content-type", "application/json")
		.header("accept", "application/json, text/event-stream")
		.body(Full::new(Bytes::from(body.to_string())))
		.unwrap()
}

/// Regression test for the "Host header 403": with the default
/// (loopback-only) allowlist, a request carrying a real public `Host` is
/// rejected by rmcp's DNS-rebinding guard before any SurrealDB logic runs.
/// An explicit `McpConfig::default()` keeps this independent of ambient
/// `SURREAL_MCP_*` env in the test process.
#[tokio::test]
async fn default_config_rejects_non_loopback_host() {
	let ds = test_datastore().await;
	let service = create_http_service_with_config(ds, None, Arc::new(McpConfig::default()));
	let resp = service.handle(initialize_with_host("public.example.com")).await;
	assert_eq!(resp.status(), StatusCode::FORBIDDEN);
	let body = body_to_string(resp).await;
	assert!(
		body.contains("Host header is not allowed"),
		"expected rmcp host-guard body, got: {body:?}"
	);
}

/// Option A: an explicit allowlist admits the configured public host that
/// the default would have rejected.
#[tokio::test]
async fn configured_allowed_host_is_accepted() {
	let ds = test_datastore().await;
	let cfg = McpConfig {
		allowed_hosts: vec!["public.example.com".to_string()],
		..McpConfig::default()
	};
	let service = create_http_service_with_config(ds, None, Arc::new(cfg));
	let resp = service.handle(initialize_with_host("public.example.com")).await;
	assert_eq!(resp.status(), StatusCode::OK);
	assert!(
		resp.headers().get("mcp-session-id").is_some(),
		"initialize should allocate a session id"
	);
}

/// Option B: the disable toggle admits an arbitrary host.
#[tokio::test]
async fn allow_all_hosts_accepts_any_host() {
	let ds = test_datastore().await;
	let cfg = McpConfig {
		allow_all_hosts: true,
		..McpConfig::default()
	};
	let service = create_http_service_with_config(ds, None, Arc::new(cfg));
	let resp = service.handle(initialize_with_host("anything.at.all")).await;
	assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn initialize_allocates_session_id_and_negotiates_protocol() {
	let ds = test_datastore().await;
	let service = setup_service(ds);
	let (session_id, body) = initialize(&service, None).await;
	assert!(!session_id.is_empty(), "session id must not be empty");
	let result = body.get("result").expect("initialize must return a result");
	let proto =
		result.get("protocolVersion").and_then(|v| v.as_str()).expect("protocolVersion field");
	assert_eq!(proto, "2025-06-18", "server should negotiate the latest protocol version");
}

#[tokio::test]
async fn second_post_with_same_session_id_reuses_session_state() {
	let ds = test_datastore().await;
	let service = setup_service(ds);
	let (session_id, _) = initialize(&service, Some(owner_session())).await;

	// Send the `initialized` notification so the server promotes the
	// handshake to a live session -- only after this will it accept tool
	// calls on the same session id.
	let notif = json!({
		"jsonrpc": "2.0",
		"method": "notifications/initialized",
	});
	let resp = service.handle(post_request(&notif, Some(&session_id), None)).await;
	assert_eq!(resp.status(), StatusCode::ACCEPTED, "notifications/initialized must be accepted");

	// Now call `tools/list` with the same session id. Follow-up requests on an
	// authenticated session must present its credentials, `tools/list` included.
	let req = json!({
		"jsonrpc": "2.0",
		"id": 2,
		"method": "tools/list",
	});
	let resp = service.handle(post_request(&req, Some(&session_id), Some(owner_session()))).await;
	assert_eq!(resp.status(), StatusCode::OK);
	let body = body_to_string(resp).await;
	let parsed = parse_sse_json(&body);
	let tools = parsed
		.get("result")
		.and_then(|r| r.get("tools"))
		.and_then(|t| t.as_array())
		.unwrap_or_else(|| panic!("tools/list must return a tools array; body: {body}"));
	assert!(tools.iter().any(|t| t.get("name").and_then(|n| n.as_str()) == Some("query")));
}

/// `tools/list` is subject to the same binding as every other method. The
/// listing is static, so the leak is the inference rather than the payload:
/// answering it confirms the replayed `mcp-session-id` is live, which is exactly
/// what possession of the id alone must not establish.
#[tokio::test]
async fn tools_list_without_credentials_on_existing_session_is_rejected() {
	let ds = test_datastore().await;
	let service = setup_service(ds);
	let (session_id, _) = initialize(&service, Some(owner_session())).await;
	complete_handshake(&service, &session_id).await;

	let req = json!({
		"jsonrpc": "2.0",
		"id": 3,
		"method": "tools/list",
	});
	let resp = service.handle(post_request(&req, Some(&session_id), None)).await;
	assert_eq!(resp.status(), StatusCode::OK);
	let body = body_to_string(resp).await;
	let parsed = parse_sse_json(&body);
	let err = parsed
		.get("error")
		.unwrap_or_else(|| panic!("tools/list must not answer without credentials; body: {body}"));
	let message = err.get("message").and_then(|v| v.as_str()).unwrap_or("");
	assert!(
		message.contains("Credentials required"),
		"error message must mention missing credentials, got: {message}"
	);
}

#[tokio::test]
async fn auth_plumbing_picks_up_injected_session() {
	// The HTTP test harness injects a `Session` into request extensions,
	// emulating what `SurrealAuth` does in production. Confirm the MCP
	// service uses it instead of the anonymous default session by issuing
	// a `tools/call` that requires NS/DB context which only the injected
	// session carries.
	//
	// On the HTTP transport, the strict subject check in
	// `verify_request_subject` requires every follow-up request to present
	// credentials matching the subject bound at handshake; we therefore
	// inject the same `Session` on the notification and on the query.
	let ds = test_datastore().await;
	let service = setup_service(ds);
	let session = Session::owner().with_ns("test").with_db("test");
	let (session_id, _) = initialize(&service, Some(session.clone())).await;

	// Complete the handshake (re-present the bound credentials).
	let notif = json!({
		"jsonrpc": "2.0",
		"method": "notifications/initialized",
	});
	let resp = service.handle(post_request(&notif, Some(&session_id), Some(session.clone()))).await;
	assert_eq!(resp.status(), StatusCode::ACCEPTED);

	// Issue a simple `query` that relies on the session carrying NS/DB.
	let req = json!({
		"jsonrpc": "2.0",
		"id": 3,
		"method": "tools/call",
		"params": { "name": "query", "arguments": { "query": "RETURN 1" } },
	});
	let resp = service.handle(post_request(&req, Some(&session_id), Some(session))).await;
	assert_eq!(resp.status(), StatusCode::OK);
	let raw = body_to_string(resp).await;
	let parsed = parse_sse_json(&raw);
	let result = parsed
		.get("result")
		.unwrap_or_else(|| panic!("query result must include a result field; body: {raw}"));
	// Structured content is optional depending on the rmcp envelope version.
	if let Some(structured) = result.get("structuredContent") {
		assert_eq!(structured.get("status").and_then(|v| v.as_str()), Some("ok"));
	}
	let content = result
		.get("content")
		.and_then(|c| c.as_array())
		.and_then(|a| a.first())
		.and_then(|c| c.get("text"))
		.and_then(|t| t.as_str())
		.unwrap_or_else(|| panic!("query must return content text; body: {raw}"));
	assert!(content.contains('1'), "RETURN 1 should produce '1' in content, got: {content}");
}

#[tokio::test]
async fn delete_cleans_up_session_state() {
	let ds = test_datastore().await;
	let service = setup_service(ds);
	let (session_id, _) = initialize(&service, Some(owner_session())).await;

	// DELETE should accept and clean up the session.
	let resp = service.handle(delete_request(&session_id)).await;
	assert!(
		resp.status().is_success() || resp.status() == StatusCode::ACCEPTED,
		"DELETE /mcp must clean up the session, got {}",
		resp.status()
	);

	// Re-using the now-stale id on a new call should be rejected: the
	// StreamableHttpService returns 4xx when the session is unknown.
	let req = json!({
		"jsonrpc": "2.0",
		"id": 1,
		"method": "tools/list",
	});
	let resp = service.handle(post_request(&req, Some(&session_id), None)).await;
	assert!(
		resp.status().is_client_error(),
		"stale session id must be rejected after DELETE, got {}",
		resp.status()
	);
}

#[tokio::test]
async fn non_post_methods_without_session_id_are_rejected() {
	let ds = test_datastore().await;
	let service = setup_service(ds);
	// Rather than GET (which requires SSE handshake specifics), use an
	// unsupported method to confirm the router rejects it cleanly.
	let req = Request::builder()
		.method(Method::PATCH)
		.uri("/mcp")
		.header("host", "localhost")
		.body(Full::new(Bytes::new()))
		.unwrap();
	let resp = service.handle(req).await;
	assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
	let allow =
		resp.headers().get("allow").and_then(|v| v.to_str().ok()).unwrap_or_default().to_string();
	assert!(allow.contains("POST"), "Allow header must list POST, got '{allow}'");
}

#[tokio::test]
async fn get_on_live_session_opens_event_stream_or_accepts() {
	// In stateful mode a GET /mcp with a valid session-id opens an SSE
	// stream. We don't consume it here (long-lived); we only assert the
	// initial status code is well-formed so the endpoint is wired up.
	let ds = test_datastore().await;
	let service = setup_service(ds);
	let (session_id, _) = initialize(&service, Some(owner_session())).await;
	let notif = json!({
		"jsonrpc": "2.0",
		"method": "notifications/initialized",
	});
	let _ = service.handle(post_request(&notif, Some(&session_id), None)).await;

	let resp = service.handle(get_request(&session_id)).await;
	assert!(
		resp.status().is_success() || resp.status() == StatusCode::METHOD_NOT_ALLOWED,
		"GET /mcp must be wired; got {}",
		resp.status()
	);
}

fn owner_session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

/// A second authenticated session with a *different* identity than
/// `owner_session()`. Used to drive the credential-mismatch path: we
/// initialize as user A and then replay a tool call as user B on the
/// same session id.
fn db_user_session() -> Session {
	use surrealdb_iam::{Auth, Role};
	Session {
		au: std::sync::Arc::new(Auth::for_db(Role::Editor, "test", "test")),
		ns: Some("test".into()),
		db: Some("test".into()),
		..Session::default()
	}
}

/// Drive the protocol past the handshake notification so the server is
/// ready to accept tool calls. Mirrors the pattern used by the other
/// tests but extracted here for the auth scenarios.
async fn complete_handshake(service: &McpHttpService, session_id: &str) {
	let notif = json!({
		"jsonrpc": "2.0",
		"method": "notifications/initialized",
	});
	let resp = service.handle(post_request(&notif, Some(session_id), None)).await;
	assert_eq!(resp.status(), StatusCode::ACCEPTED);
}

#[tokio::test]
async fn credential_mismatch_on_existing_session_is_rejected() {
	// Spec: "MCP servers MUST verify all inbound requests." Bind to user A
	// at initialize, then replay as user B on the same MCP session id.
	// The server must surface a JSON-RPC error rather than serve user B's
	// request as user A.
	let ds = test_datastore().await;
	let service = setup_service(ds);
	let (session_id, _) = initialize(&service, Some(owner_session())).await;
	complete_handshake(&service, &session_id).await;

	let req = json!({
		"jsonrpc": "2.0",
		"id": 7,
		"method": "tools/call",
		"params": { "name": "query", "arguments": { "query": "RETURN 1" } },
	});
	let resp = service.handle(post_request(&req, Some(&session_id), Some(db_user_session()))).await;
	assert_eq!(resp.status(), StatusCode::OK);
	let body = body_to_string(resp).await;
	let parsed = parse_sse_json(&body);
	let err = parsed.get("error").unwrap_or_else(|| {
		panic!("mismatched-credential request must surface a JSON-RPC error; body: {body}")
	});
	let message = err.get("message").and_then(|v| v.as_str()).unwrap_or("");
	assert!(
		message.contains("Credentials do not match"),
		"error message must mention credential mismatch, got: {message}"
	);
}

#[tokio::test]
async fn missing_credentials_on_existing_session_is_rejected() {
	// Follow-up requests on an authenticated MCP session must continue to
	// present credentials for the bound subject; possession of only the
	// session id is insufficient.
	let ds = test_datastore().await;
	let service = setup_service(ds);
	let (session_id, _) = initialize(&service, Some(owner_session())).await;
	complete_handshake(&service, &session_id).await;

	let req = json!({
		"jsonrpc": "2.0",
		"id": 8,
		"method": "tools/call",
		"params": { "name": "query", "arguments": { "query": "RETURN 1" } },
	});
	let resp = service.handle(post_request(&req, Some(&session_id), None)).await;
	assert_eq!(resp.status(), StatusCode::OK);
	let body = body_to_string(resp).await;
	let parsed = parse_sse_json(&body);
	let err = parsed.get("error").unwrap_or_else(|| {
		panic!("missing-credential request must surface a JSON-RPC error; body: {body}")
	});
	let message = err.get("message").and_then(|v| v.as_str()).unwrap_or("");
	assert!(
		message.contains("Credentials required"),
		"error message must mention missing credentials, got: {message}"
	);
}

/// A handshake naming a revision this server does not implement is answered
/// with the advertised one, and the advertised revision is the sessionless
/// `2026-07-28` — yet rmcp classifies that handshake by the *requested*
/// revision, so it still runs the legacy lifecycle and issues an
/// `mcp-session-id`. The result is a session that reports the sessionless
/// revision from `ctx.protocol_version()` on every later request while holding
/// both a session and a bound subject.
///
/// Deciding statelessness from that reported revision therefore skips the
/// subject check on a session that has one, and possession of the session id
/// alone is enough to run tool calls as the handshake's identity with no
/// credentials at all. The binding, not the revision, has to be the
/// discriminator.
#[tokio::test]
async fn credentialless_replay_is_rejected_when_negotiation_reports_the_sessionless_revision() {
	let ds = test_datastore().await;
	let service = setup_service(ds);
	// Any revision outside `SUPPORTED_PROTOCOL_VERSIONS` triggers the fallback.
	// It must sort below the sessionless revision, or rmcp routes the handshake
	// itself sessionlessly and never allocates the session id this needs.
	let (session_id, init) =
		initialize_with_version(&service, "2024-10-07", Some(owner_session())).await;
	assert_eq!(
		init.pointer("/result/protocolVersion").and_then(Value::as_str),
		Some("2026-07-28"),
		"fixture requires negotiation to answer with the advertised sessionless revision; body: {init}"
	);
	complete_handshake(&service, &session_id).await;

	let req = json!({
		"jsonrpc": "2.0",
		"id": 10,
		"method": "tools/call",
		"params": {
			"name": "query",
			"arguments": { "query": "RETURN [session::ns(), session::db()]" },
		},
	});
	let resp = service.handle(post_request(&req, Some(&session_id), None)).await;
	assert_eq!(resp.status(), StatusCode::OK);
	let body = body_to_string(resp).await;
	let parsed = parse_sse_json(&body);
	let err = parsed.get("error").unwrap_or_else(|| {
		panic!(
			"credential-less replay on a negotiation-fallback session must be rejected, \
			 not served as the bound owner; body: {body}"
		)
	});
	let message = err.get("message").and_then(|v| v.as_str()).unwrap_or("");
	assert!(
		message.contains("Credentials required"),
		"error message must mention missing credentials, got: {message}"
	);

	// Control: the same session still serves its own credentials, so the
	// rejection above is about the credentials and not about the odd handshake.
	let resp = service.handle(post_request(&req, Some(&session_id), Some(owner_session()))).await;
	assert_eq!(resp.status(), StatusCode::OK);
	let body = body_to_string(resp).await;
	let parsed = parse_sse_json(&body);
	let scope = parsed
		.pointer("/result/structuredContent/value")
		.unwrap_or_else(|| panic!("matching credentials should return a scope; body: {body}"));
	assert_eq!(
		scope,
		&json!(["test", "test"]),
		"matching credentials must resolve the handshake scope; body: {body}"
	);
}

/// rmcp treats a request that names the sessionless revision in its own `_meta`
/// as sessionless *regardless of any `mcp-session-id` it carries*: it builds a
/// fresh service from the factory and never looks the id up. The handler's
/// contract leans on that, so pin it — an rmcp change that started routing such
/// a request to the named session would put a request claiming to be
/// sessionless in front of a service that holds a session.
///
/// The scope of the resolved session is the discriminator. The handshake bound
/// `test`/`test`; a request served sessionlessly resolves neither, since the
/// HTTP factory configures no default namespace or database. Query success
/// cannot discriminate — `test_datastore` runs with auth disabled, so an
/// anonymous session executes the same statements as an owner one.
#[tokio::test]
async fn request_naming_the_sessionless_revision_is_not_served_by_a_replayed_session() {
	let ds = test_datastore().await;
	let service = setup_service(ds);
	let (session_id, _) = initialize(&service, Some(owner_session())).await;
	complete_handshake(&service, &session_id).await;

	let scope_probe = json!({
		"jsonrpc": "2.0",
		"id": 10,
		"method": "tools/call",
		"params": {
			"_meta": {
				// Fully-qualified keys: rmcp ignores a bare `protocolVersion`,
				// which would leave the request legacy and defuse this test.
				"io.modelcontextprotocol/protocolVersion": "2026-07-28",
				"io.modelcontextprotocol/clientCapabilities": {},
			},
			"name": "query",
			"arguments": { "query": "RETURN [session::ns(), session::db()]" },
		},
	});
	// A sessionless-revision request must carry the agreeing protocol header and
	// the SEP-2243 method/name headers, or rmcp rejects it before dispatch and
	// the assertion below would pass without exercising anything.
	let resp = service
		.handle(post_request_with_headers(
			&scope_probe,
			Some(&session_id),
			None,
			&[
				("mcp-protocol-version", "2026-07-28"),
				("mcp-method", "tools/call"),
				("mcp-name", "query"),
			],
		))
		.await;
	assert_eq!(resp.status(), StatusCode::OK);
	let body = body_to_string(resp).await;
	let parsed = parse_sse_json(&body);
	let scope = parsed
		.pointer("/result/structuredContent/value")
		.unwrap_or_else(|| panic!("sessionless call should return a scope; body: {body}"));
	assert_eq!(
		scope,
		&json!([null, null]),
		"a sessionless-revision request must not inherit the replayed session's scope; body: {body}"
	);
}

#[tokio::test]
async fn matching_credentials_on_existing_session_are_accepted() {
	// Symmetric positive case: replaying with the *same* authenticated
	// identity must pass the verification check.
	let ds = test_datastore().await;
	let service = setup_service(ds);
	let (session_id, _) = initialize(&service, Some(owner_session())).await;
	complete_handshake(&service, &session_id).await;

	let req = json!({
		"jsonrpc": "2.0",
		"id": 9,
		"method": "tools/call",
		"params": { "name": "query", "arguments": { "query": "RETURN 1" } },
	});
	let resp = service.handle(post_request(&req, Some(&session_id), Some(owner_session()))).await;
	assert_eq!(resp.status(), StatusCode::OK);
	let body = body_to_string(resp).await;
	let parsed = parse_sse_json(&body);
	assert!(
		parsed.get("error").is_none(),
		"matching credentials must pass verification; body: {body}"
	);
}
