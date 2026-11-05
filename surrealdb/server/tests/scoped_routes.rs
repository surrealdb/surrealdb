//! End-to-end tests for the scoped routes `/ns/{ns}[/db/{db}]/{method}/…`.
//!
//! Each test serves the real community router on an ephemeral port and checks
//! which namespace and database a scoped request runs in, how the scope
//! interacts with the `surreal-ns`/`surreal-db` headers and with credentials,
//! and that routes outside the scoped set stay unreachable under a scope.

// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]
#![recursion_limit = "256"]

use std::net::SocketAddr;

use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};
use surrealdb_cnf::ConfigMap;
use surrealdb_core::CommunityComposer;
use surrealdb_core::kvs::Datastore;
use surrealdb_rpc::capabilities::{Capabilities, Targets};
use surrealdb_server::ntw::{RouterOptions, SurrealRouter};
use tokio_util::sync::CancellationToken;

const USER: &str = "root";
const PASS: &str = "root";

struct TestServer {
	address: SocketAddr,
	canceller: CancellationToken,
	handle: axum_server::Handle<SocketAddr>,
}

impl Drop for TestServer {
	fn drop(&mut self) {
		self.canceller.cancel();
		self.handle.shutdown();
	}
}

impl TestServer {
	async fn start() -> Self {
		let (send, recv) = surrealdb_core::channel::bounded(128);
		let datastore = Datastore::builder()
			.with_capabilities(Capabilities::all().with_experimental(Targets::All))
			.with_config(
				ConfigMap::empty()
					.with_key_value("bucket_presign_secret", "scoped-routes-presign-secret-0000"),
			)
			.with_notify(send)
			.with_auth(true)
			.build_with_path("memory")
			.await
			.expect("datastore");
		datastore.initialise_credentials(USER, PASS).await.expect("root credentials");
		let canceller = CancellationToken::new();
		let router = SurrealRouter::build::<CommunityComposer>(
			RouterOptions::default(),
			datastore,
			recv,
			canceller.clone(),
			(),
		)
		.await
		.unwrap_or_else(|e| panic!("router: {e}"));
		router.spawn_notifications();
		let handle = axum_server::Handle::new();
		let app = router.into_router();
		tokio::spawn({
			let handle = handle.clone();
			async move {
				axum_server::bind("127.0.0.1:0".parse().expect("address"))
					.handle(handle)
					.serve(app.into_make_service_with_connect_info::<SocketAddr>())
					.await
					.ok();
			}
		});
		let address = handle.listening().await.expect("the server to bind");
		Self {
			address,
			canceller,
			handle,
		}
	}

	fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
		Client::new().request(method, format!("http://{}{path}", self.address))
	}

	/// `POST {path}` with a SurrealQL body, authenticated as root.
	fn sql(&self, path: &str, query: &str) -> reqwest::RequestBuilder {
		self.request(Method::POST, path)
			.basic_auth(USER, Some(PASS))
			.header("Content-Type", "text/plain")
			.header("Accept", "application/json")
			.body(query.to_string())
	}

	/// Sends a single-statement `/sql` request and returns the statement's result,
	/// asserting that both the request and the statement succeeded.
	async fn selected(&self, request: reqwest::RequestBuilder) -> Value {
		let res = request.send().await.expect("request");
		assert_eq!(res.status(), StatusCode::OK, "body: {}", res.text().await.unwrap_or_default());
		let body: Value = serde_json::from_str(&res.text().await.expect("body")).expect("json");
		assert_eq!(body[0]["status"], json!("OK"), "body: {body}");
		body[0]["result"].clone()
	}
}

const SELECTED: &str = "RETURN [session::ns(), session::db()]";

#[tokio::test]
async fn scope_selects_namespace_and_database() {
	let server = TestServer::start().await;
	let pair = server.selected(server.sql("/ns/foo/db/bar/sql", SELECTED)).await;
	assert_eq!(pair, json!(["foo", "bar"]));
}

#[tokio::test]
async fn namespace_scope_leaves_database_to_header() {
	let server = TestServer::start().await;
	let pair =
		server.selected(server.sql("/ns/foo/sql", SELECTED).header("surreal-db", "bar")).await;
	assert_eq!(pair, json!(["foo", "bar"]));
}

#[tokio::test]
async fn scope_segments_are_percent_decoded() {
	let server = TestServer::start().await;
	let pair = server.selected(server.sql("/ns/a%20b/db/c%2Fd/sql", SELECTED)).await;
	assert_eq!(pair, json!(["a b", "c/d"]));
}

#[tokio::test]
async fn matching_header_is_accepted() {
	let server = TestServer::start().await;
	let pair = server
		.selected(
			server
				.sql("/ns/foo/db/bar/sql", SELECTED)
				.header("surreal-ns", "foo")
				.header("surreal-db", "bar"),
		)
		.await;
	assert_eq!(pair, json!(["foo", "bar"]));
}

#[tokio::test]
async fn conflicting_header_is_refused() {
	let server = TestServer::start().await;
	for (header, value) in [("surreal-ns", "other"), ("surreal-db", "other")] {
		let res = server
			.sql("/ns/foo/db/bar/sql", SELECTED)
			.header(header, value)
			.send()
			.await
			.expect("request");
		assert_eq!(res.status(), StatusCode::BAD_REQUEST, "{header}: {value}");
	}
}

#[tokio::test]
async fn unscoped_routes_are_not_found_under_a_scope() {
	let server = TestServer::start().await;
	for path in [
		"/ns/foo/health",
		"/ns/foo/db/bar/version",
		"/ns/foo/db/bar/signout",
		"/ns/foo/db/bar/ac/users/sql",
		"/ac/admins/sql",
		"/ns/foo/db/bar",
		"/ns/foo",
		"/ns",
		"/ns//sql",
		"/ns/foo/ns/bar/sql",
	] {
		let res = server.sql(path, SELECTED).send().await.expect("request");
		assert_eq!(res.status(), StatusCode::NOT_FOUND, "{path}");
	}
}

#[tokio::test]
async fn rpc_runs_in_the_scope() {
	let server = TestServer::start().await;
	let res = server
		.request(Method::POST, "/ns/foo/db/bar/rpc")
		.basic_auth(USER, Some(PASS))
		.header("Content-Type", "application/json")
		.header("Accept", "application/json")
		.body(json!({"id": 1, "method": "query", "params": [SELECTED]}).to_string())
		.send()
		.await
		.expect("request");
	assert_eq!(res.status(), StatusCode::OK);
	let body: Value = serde_json::from_str(&res.text().await.expect("body")).expect("json");
	assert_eq!(body["result"][0]["result"], json!(["foo", "bar"]), "body: {body}");
}

#[tokio::test]
async fn key_route_parameters_are_unaffected() {
	let server = TestServer::start().await;
	server.selected(server.sql("/sql", "DEFINE NAMESPACE foo")).await;
	server.selected(server.sql("/ns/foo/sql", "DEFINE DATABASE bar")).await;
	server
		.selected(server.sql("/ns/foo/db/bar/sql", "CREATE person:tobie SET name = 'Tobie'"))
		.await;
	let res = server
		.request(Method::GET, "/ns/foo/db/bar/key/person/tobie")
		.basic_auth(USER, Some(PASS))
		.header("Accept", "application/json")
		.send()
		.await
		.expect("request");
	assert_eq!(res.status(), StatusCode::OK);
	let body: Value = serde_json::from_str(&res.text().await.expect("body")).expect("json");
	assert_eq!(body[0]["result"][0]["name"], json!("Tobie"), "body: {body}");
}

#[tokio::test]
async fn api_answers_in_both_forms() {
	let server = TestServer::start().await;
	server
		.selected(server.sql(
			"/ns/foo/db/bar/sql",
			"DEFINE API '/ping' FOR get PERMISSIONS FULL THEN { RETURN { status: 200, body: 'pong' } }",
		))
		.await;
	for path in ["/api/foo/bar/ping", "/ns/foo/db/bar/api/ping"] {
		let res = server
			.request(Method::GET, path)
			.header("Accept", "application/json")
			.send()
			.await
			.expect("request");
		assert_eq!(res.status(), StatusCode::OK, "{path}");
		assert_eq!(res.text().await.expect("body"), "pong", "{path}");
	}
}

/// Defines a bucket `img` on the root, on namespace `foo` and on database
/// `foo`/`bar`, each holding `a.txt` with the name of its level.
async fn define_buckets(server: &TestServer) {
	for (path, level) in
		[("/sql", "ROOT"), ("/ns/foo/sql", "NAMESPACE"), ("/ns/foo/db/bar/sql", "DATABASE")]
	{
		server
			.selected(server.sql(
				path,
				&format!(
					"DEFINE BUCKET img ON {level} BACKEND \"memory\" PERMISSIONS FULL; \
					 f\"img:/a.txt\".put(\"{}\")",
					level.to_lowercase()
				),
			))
			.await;
	}
}

#[tokio::test]
async fn buckets_are_served_at_the_level_the_url_names() {
	let server = TestServer::start().await;
	server.selected(server.sql("/sql", "DEFINE NAMESPACE foo")).await;
	server.selected(server.sql("/ns/foo/sql", "DEFINE DATABASE bar")).await;
	define_buckets(&server).await;
	let get = |path: &str| server.request(Method::GET, path).basic_auth(USER, Some(PASS));
	for (path, body) in [
		("/bucket/img/a.txt", "root"),
		("/ns/foo/bucket/img/a.txt", "namespace"),
		("/ns/foo/db/bar/bucket/img/a.txt", "database"),
	] {
		let res = get(path).send().await.expect("request");
		assert_eq!(res.status(), StatusCode::OK, "{path}");
		assert_eq!(res.text().await.expect("body"), body, "{path}");
	}
	for path in ["/bucket/img/", "/ns/foo/bucket/img/", "/ns/foo/db/bar/bucket/img/"] {
		let res = get(path).header("Accept", "application/json").send().await.expect("request");
		assert_eq!(res.status(), StatusCode::OK, "{path}");
		let body: Value = serde_json::from_str(&res.text().await.expect("body")).expect("json");
		assert!(body.to_string().contains("a.txt"), "{path}: {body}");
	}
	// The headers never move a request to another level's bucket.
	let res = get("/bucket/img/a.txt")
		.header("surreal-ns", "foo")
		.header("surreal-db", "bar")
		.send()
		.await
		.expect("request");
	assert_eq!(res.text().await.expect("body"), "root");
	// A header that names another database than the path is refused.
	let res = get("/ns/foo/db/bar/bucket/img/a.txt")
		.header("surreal-db", "other")
		.send()
		.await
		.expect("request");
	assert_eq!(res.status(), StatusCode::BAD_REQUEST);
	// `/bucket/{ns}/{db}/{name}/…` is not a route: it names root bucket `foo`.
	let res = get("/bucket/foo/bar/img/a.txt").send().await.expect("request");
	assert_ne!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn a_principal_reaches_no_bucket_above_its_level() {
	let server = TestServer::start().await;
	server.selected(server.sql("/sql", "DEFINE NAMESPACE foo")).await;
	server.selected(server.sql("/ns/foo/sql", "DEFINE DATABASE bar")).await;
	define_buckets(&server).await;
	server
		.selected(server.sql(
			"/ns/foo/db/bar/sql",
			"DEFINE USER carol ON DATABASE PASSWORD 'secret' ROLES OWNER",
		))
		.await;
	let get = |path: &str| {
		server
			.request(Method::GET, path)
			.basic_auth("carol", Some("secret"))
			.header("surreal-auth-ns", "foo")
			.header("surreal-auth-db", "bar")
	};
	let res = get("/ns/foo/db/bar/bucket/img/a.txt").send().await.expect("request");
	assert_eq!(res.status(), StatusCode::OK);
	assert_eq!(res.text().await.expect("body"), "database");
	// The namespace scope refuses a database principal outright; the root
	// bucket refuses it in the bucket route.
	for path in ["/ns/foo/bucket/img/a.txt", "/bucket/img/a.txt"] {
		let res = get(path).send().await.expect("request");
		assert!(
			matches!(res.status(), StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN),
			"{path}: {}",
			res.status()
		);
	}
}

#[tokio::test]
async fn a_namespace_principal_reaches_its_namespace_and_below() {
	let server = TestServer::start().await;
	server.selected(server.sql("/sql", "DEFINE NAMESPACE foo")).await;
	server.selected(server.sql("/ns/foo/sql", "DEFINE DATABASE bar")).await;
	define_buckets(&server).await;
	server
		.selected(
			server
				.sql("/ns/foo/sql", "DEFINE USER dave ON NAMESPACE PASSWORD 'secret' ROLES OWNER"),
		)
		.await;
	let get = |path: &str| {
		server
			.request(Method::GET, path)
			.basic_auth("dave", Some("secret"))
			.header("surreal-auth-ns", "foo")
	};
	for (path, body) in
		[("/ns/foo/bucket/img/a.txt", "namespace"), ("/ns/foo/db/bar/bucket/img/a.txt", "database")]
	{
		let res = get(path).send().await.expect("request");
		assert_eq!(res.status(), StatusCode::OK, "{path}");
		assert_eq!(res.text().await.expect("body"), body, "{path}");
	}
	// The root bucket is above its level.
	let res = get("/bucket/img/a.txt").send().await.expect("request");
	assert_eq!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn presigned_urls_name_their_level_and_ignore_ambient_credentials() {
	let server = TestServer::start().await;
	server.selected(server.sql("/sql", "DEFINE NAMESPACE foo")).await;
	server.selected(server.sql("/ns/foo/sql", "DEFINE DATABASE bar")).await;
	server.selected(server.sql("/sql", "DEFINE NAMESPACE other")).await;
	server
		.selected(
			server
				.sql("/ns/other/sql", "DEFINE USER bob ON NAMESPACE PASSWORD 'secret' ROLES OWNER"),
		)
		.await;
	define_buckets(&server).await;
	const PRESIGN: &str = "RETURN file::presign_get(f\"img:/a.txt\").path";
	for (sql, prefix, body) in [
		("/sql", "/bucket/img/a.txt?", "root"),
		("/ns/foo/sql", "/ns/foo/bucket/img/a.txt?", "namespace"),
		("/ns/foo/db/bar/sql", "/ns/foo/db/bar/bucket/img/a.txt?", "database"),
	] {
		let url = server.selected(server.sql(sql, PRESIGN)).await;
		let url = url.as_str().expect("a presigned path").to_owned();
		assert!(url.starts_with(prefix), "{url}");
		// With no credential, with credentials for another namespace, and
		// with a bearer token that does not verify: the signature decides.
		let requests = [
			server.request(Method::GET, &url),
			server
				.request(Method::GET, &url)
				.basic_auth("bob", Some("secret"))
				.header("surreal-auth-ns", "other"),
			server.request(Method::GET, &url).bearer_auth("not-a-token"),
		];
		for request in requests {
			let res = request.send().await.expect("request");
			assert_eq!(res.status(), StatusCode::OK, "{url}");
			assert_eq!(res.text().await.expect("body"), body, "{url}");
		}
	}
	// A URL minted for one level does not verify at another.
	let url = server.selected(server.sql("/ns/foo/db/bar/sql", PRESIGN)).await;
	let url = url.as_str().expect("a presigned path");
	let moved = url.replacen("/ns/foo/db/bar/bucket/", "/ns/foo/bucket/", 1);
	let res = server.request(Method::GET, &moved).send().await.expect("request");
	assert_eq!(res.status(), StatusCode::FORBIDDEN, "{moved}");
}

#[tokio::test]
async fn credentials_for_another_database_are_refused() {
	let server = TestServer::start().await;
	server
		.selected(server.sql(
			"/ns/foo/db/bar/sql",
			"DEFINE USER alice ON DATABASE PASSWORD 'secret' ROLES OWNER",
		))
		.await;
	let res = server
		.request(Method::POST, "/signin")
		.header("Accept", "text/plain")
		.body(json!({"ns": "foo", "db": "bar", "user": "alice", "pass": "secret"}).to_string())
		.send()
		.await
		.expect("signin");
	assert_eq!(res.status(), StatusCode::OK);
	let token = res.text().await.expect("token");
	let query = |path: &str| {
		server
			.request(Method::POST, path)
			.bearer_auth(&token)
			.header("Content-Type", "text/plain")
			.header("Accept", "application/json")
			.body(SELECTED)
	};
	// The token's own database is reachable through the scope.
	let pair = server.selected(query("/ns/foo/db/bar/sql")).await;
	assert_eq!(pair, json!(["foo", "bar"]));
	// A scope naming another database is not redirected to the token's one.
	let res = query("/ns/foo/db/other/sql").send().await.expect("request");
	assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
	let res = query("/ns/other/sql").send().await.expect("request");
	assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn basic_credentials_for_another_namespace_are_refused() {
	let server = TestServer::start().await;
	server.selected(server.sql("/sql", "DEFINE NAMESPACE foo")).await;
	server.selected(server.sql("/sql", "DEFINE NAMESPACE other")).await;
	server
		.selected(
			server
				.sql("/ns/other/sql", "DEFINE USER bob ON NAMESPACE PASSWORD 'secret' ROLES OWNER"),
		)
		.await;
	let query = |path: &str| {
		server
			.request(Method::POST, path)
			.basic_auth("bob", Some("secret"))
			.header("surreal-auth-ns", "other")
			.header("Content-Type", "text/plain")
			.header("Accept", "application/json")
			.body(SELECTED)
	};
	// The principal's own namespace is reachable through the scope.
	let pair = server.selected(query("/ns/other/sql")).await;
	assert_eq!(pair, json!(["other", null]));
	// A namespace-only scope naming another namespace is refused, although Basic
	// authentication leaves the selected namespace as the path set it.
	let res = query("/ns/foo/sql").send().await.expect("request");
	assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// Defines namespace `foo`, database `bar`, a database user `alice` and a
/// record access method `users` on it.
async fn define_tenant(server: &TestServer) {
	server.selected(server.sql("/sql", "DEFINE NAMESPACE foo")).await;
	server.selected(server.sql("/ns/foo/sql", "DEFINE DATABASE bar")).await;
	server
		.selected(server.sql(
			"/ns/foo/db/bar/sql",
			"DEFINE USER alice ON DATABASE PASSWORD 'secret' ROLES OWNER",
		))
		.await;
	server
		.selected(server.sql(
			"/ns/foo/db/bar/sql",
			"DEFINE ACCESS users ON DATABASE TYPE RECORD \
			 SIGNUP (CREATE user SET email = $email) \
			 SIGNIN (SELECT * FROM user WHERE email = $email) \
			 DURATION FOR SESSION 1h",
		))
		.await;
}

/// `POST {path}` with a JSON credentials body, answered with the bare token.
fn credentials(server: &TestServer, path: &str, body: &Value) -> reqwest::RequestBuilder {
	server
		.request(Method::POST, path)
		.header("Content-Type", "application/json")
		.header("Accept", "text/plain")
		.body(body.to_string())
}

#[tokio::test]
async fn scoped_signin_takes_its_scope_from_the_path() {
	let server = TestServer::start().await;
	define_tenant(&server).await;
	let res =
		credentials(&server, "/ns/foo/db/bar/signin", &json!({"user": "alice", "pass": "secret"}))
			.send()
			.await
			.expect("signin");
	assert_eq!(res.status(), StatusCode::OK);
	let token = res.text().await.expect("token");
	// The token is a database user's token for `foo`/`bar`.
	let pair = server
		.selected(
			server
				.request(Method::POST, "/sql")
				.bearer_auth(&token)
				.header("Content-Type", "text/plain")
				.header("Accept", "application/json")
				.body(SELECTED),
		)
		.await;
	assert_eq!(pair, json!(["foo", "bar"]));
}

#[tokio::test]
async fn scoped_signin_refuses_credentials_for_another_scope() {
	let server = TestServer::start().await;
	define_tenant(&server).await;
	for body in [
		json!({"ns": "other", "user": "alice", "pass": "secret"}),
		json!({"DB": "other", "user": "alice", "pass": "secret"}),
	] {
		let res =
			credentials(&server, "/ns/foo/db/bar/signin", &body).send().await.expect("signin");
		assert_eq!(res.status(), StatusCode::BAD_REQUEST, "{body}");
	}
	// Credentials that repeat the scope are accepted.
	let res = credentials(
		&server,
		"/ns/foo/db/bar/signin",
		&json!({"ns": "foo", "db": "bar", "user": "alice", "pass": "secret"}),
	)
	.send()
	.await
	.expect("signin");
	assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn scoped_signup_and_signin_take_the_access_method_from_the_path() {
	let server = TestServer::start().await;
	define_tenant(&server).await;
	let res = credentials(&server, "/ns/foo/db/bar/ac/users/signup", &json!({"email": "a@b.c"}))
		.send()
		.await
		.expect("signup");
	assert_eq!(res.status(), StatusCode::OK, "body: {}", res.text().await.unwrap_or_default());
	let res = credentials(&server, "/ns/foo/db/bar/ac/users/signin", &json!({"email": "a@b.c"}))
		.send()
		.await
		.expect("signin");
	assert_eq!(res.status(), StatusCode::OK, "body: {}", res.text().await.unwrap_or_default());
	let res = credentials(
		&server,
		"/ns/foo/db/bar/ac/users/signin",
		&json!({"ac": "other", "email": "a@b.c"}),
	)
	.send()
	.await
	.expect("signin");
	assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn use_can_leave_the_scope_over_sql() {
	let server = TestServer::start().await;
	let res = server
		.sql(
			"/ns/foo/db/bar/sql",
			"RETURN [session::ns(), session::db()]; USE NS other DB other; RETURN [session::ns(), session::db()]",
		)
		.send()
		.await
		.expect("request");
	assert_eq!(res.status(), StatusCode::OK);
	let body: Value = serde_json::from_str(&res.text().await.expect("body")).expect("json");
	assert_eq!(body[0]["result"], json!(["foo", "bar"]), "body: {body}");
	assert_eq!(body[1]["status"], json!("OK"), "body: {body}");
	assert_eq!(body[2]["result"], json!(["other", "other"]), "body: {body}");
}

#[tokio::test]
async fn use_can_leave_the_scope_over_http_rpc() {
	let server = TestServer::start().await;
	let res = server
		.request(Method::POST, "/ns/foo/db/bar/rpc")
		.basic_auth(USER, Some(PASS))
		.header("Content-Type", "application/json")
		.header("Accept", "application/json")
		.body(json!({"id": 1, "method": "use", "params": ["other", "other"]}).to_string())
		.send()
		.await
		.expect("request");
	let body: Value = serde_json::from_str(&res.text().await.expect("body")).expect("json");
	assert!(body.get("error").is_none(), "body: {body}");
}

/// A raw Json-format WebSocket RPC client.
struct WsClient {
	stream: tokio_tungstenite::WebSocketStream<
		tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
	>,
}

impl WsClient {
	/// Connects to `path` authenticated as root with Basic credentials.
	async fn connect(server: &TestServer, path: &str) -> Self {
		use tokio_tungstenite::tungstenite::client::IntoClientRequest;
		let mut request = format!("ws://{}{path}", server.address)
			.into_client_request()
			.expect("a websocket request");
		let headers = request.headers_mut();
		headers.insert("Sec-WebSocket-Protocol", "json".parse().expect("a protocol header"));
		headers.insert("Authorization", "Basic cm9vdDpyb290".parse().expect("a basic header"));
		let (stream, _) = tokio_tungstenite::connect_async(request).await.expect("connect");
		Self {
			stream,
		}
	}

	/// Sends a request and returns its response, skipping unrelated messages.
	async fn request(&mut self, body: Value) -> Value {
		use futures::{SinkExt, StreamExt};
		use tokio_tungstenite::tungstenite::Message;
		let id = body["id"].clone();
		self.stream.send(Message::Text(body.to_string().into())).await.expect("send");
		loop {
			let msg = tokio::time::timeout(std::time::Duration::from_secs(30), self.stream.next())
				.await
				.expect("a message within the read timeout")
				.expect("an open connection")
				.expect("a websocket message");
			let msg: Value = match msg {
				Message::Text(text) => serde_json::from_str(&text).expect("json"),
				Message::Binary(bytes) => serde_json::from_slice(&bytes).expect("json"),
				Message::Ping(_) | Message::Pong(_) | Message::Close(_) | Message::Frame(_) => {
					continue;
				}
			};
			if msg["id"] == id {
				return msg;
			}
		}
	}
}

#[tokio::test]
async fn websocket_connection_starts_in_its_scope() {
	let server = TestServer::start().await;
	let mut ws = WsClient::connect(&server, "/ns/foo/db/bar/rpc").await;
	let selected = |id: &str| json!({"id": id, "method": "query", "params": [SELECTED]});
	// The connection starts in the namespace and database the URL names.
	let res = ws.request(selected("q1")).await;
	assert_eq!(res["result"][0]["result"], json!(["foo", "bar"]), "{res}");
	// `use` selects another namespace and database for the rest of the connection.
	let res = ws.request(json!({"id": "u1", "method": "use", "params": ["other", "other"]})).await;
	assert!(res.get("error").is_none(), "{res}");
	let res = ws.request(selected("q2")).await;
	assert_eq!(res["result"][0]["result"], json!(["other", "other"]), "{res}");
	// So does SurrealQL `USE`.
	let res = ws
		.request(
			json!({"id": "q3", "method": "query", "params": [format!("USE NS foo DB baz; {SELECTED}")]}),
		)
		.await;
	assert_eq!(res["result"][1]["result"], json!(["foo", "baz"]), "{res}");
}
