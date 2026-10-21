//! End-to-end tests for HttpOnly cookie authentication.
//!
//! Each test serves the real community router on an ephemeral port with cookie
//! authentication on and `http://app.test` as the only allowed origin, and
//! drives it with raw HTTP and WebSocket clients so that every header the
//! origin gate reads is set explicitly.

// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]
#![recursion_limit = "256"]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use reqwest::{Client, Method, Response, StatusCode};
use surrealdb_core::CommunityComposer;
use surrealdb_core::kvs::Datastore;
use surrealdb_rpc::capabilities::Capabilities;
use surrealdb_server::ntw::{RouterOptions, SurrealRouter};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_util::sync::CancellationToken;

const USER: &str = "root";
const PASS: &str = "root";
const APP: &str = "http://app.test";
const EVIL: &str = "http://evil.test";
const COOKIE_NAME: &str = "surreal_token";
const READ_TIMEOUT: Duration = Duration::from_secs(30);

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

async fn datastore()
-> (Arc<Datastore>, surrealdb_core::channel::Receiver<surrealdb_types::Notification>) {
	let (send, recv) = surrealdb_core::channel::bounded(128);
	let datastore = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.with_notify(send)
		.with_auth(true)
		.build_with_path("memory")
		.await
		.expect("datastore");
	datastore.initialise_credentials(USER, PASS).await.expect("root credentials");
	(datastore, recv)
}

impl TestServer {
	async fn start(options: RouterOptions) -> Self {
		let (datastore, recv) = datastore().await;
		let canceller = CancellationToken::new();
		let router = SurrealRouter::build::<CommunityComposer>(
			options,
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

	async fn cookies() -> Self {
		Self::start(cookie_options()).await
	}

	fn url(&self, path: &str) -> String {
		format!("http://{}{path}", self.address)
	}

	fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
		Client::new().request(method, self.url(path))
	}

	/// Signs in as root asking for a cookie, and returns the `name=value` pair.
	async fn cookie(&self) -> String {
		let res = self
			.request(Method::POST, "/signin/cookie")
			.header("Origin", APP)
			.header("Accept", "application/json")
			.body(format!(r#"{{"user":"{USER}","pass":"{PASS}"}}"#))
			.send()
			.await
			.expect("signin");
		assert_eq!(res.status(), StatusCode::OK);
		let set = set_cookie(&res).expect("a Set-Cookie header");
		set.split(';').next().expect("a cookie pair").to_string()
	}

	/// `INFO FOR ROOT` over `POST /sql`, which only root may run.
	fn sql(&self) -> reqwest::RequestBuilder {
		self.request(Method::POST, "/sql")
			.header("Content-Type", "text/plain")
			.header("Accept", "application/json")
			.body("INFO FOR ROOT;")
	}
}

fn cookie_options() -> RouterOptions {
	RouterOptions {
		cookie_auth: true,
		allow_origin: vec![APP.to_string()],
		..RouterOptions::default()
	}
}

fn set_cookie(res: &Response) -> Option<String> {
	res.headers().get("set-cookie").map(|v| v.to_str().expect("ascii cookie").to_string())
}

/// Whether the `/sql` response shows the statement ran with root authority.
async fn ran_as_root(res: Response) -> bool {
	assert_eq!(res.status(), StatusCode::OK, "a refused cookie must not become a 401");
	let body: serde_json::Value = res.json().await.expect("json body");
	body[0]["status"] == "OK"
}

fn max_age(cookie: &str) -> i64 {
	cookie
		.split(';')
		.find_map(|p| p.trim().strip_prefix("Max-Age="))
		.expect("a Max-Age attribute")
		.parse()
		.expect("a numeric Max-Age")
}

fn with_big_stack<F, Fut>(body: F)
where
	F: FnOnce() -> Fut + Send + 'static,
	Fut: std::future::Future<Output = ()>,
{
	std::thread::Builder::new()
		.stack_size(24 * 1024 * 1024)
		.spawn(move || {
			let runtime = tokio::runtime::Builder::new_multi_thread()
				.enable_all()
				.worker_threads(2)
				.thread_stack_size(24 * 1024 * 1024)
				.build()
				.expect("test runtime");
			runtime.block_on(body());
		})
		.expect("spawn test thread")
		.join()
		.expect("test thread");
}

#[test]
fn signin_issues_a_cookie_and_no_token_in_the_body() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		let res = server
			.request(Method::POST, "/signin/cookie")
			.header("Origin", APP)
			.header("Accept", "application/json")
			.body(format!(r#"{{"user":"{USER}","pass":"{PASS}"}}"#))
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::OK);
		let set = set_cookie(&res).expect("a Set-Cookie header");
		assert!(set.starts_with("surreal_token="), "{set}");
		for attr in ["HttpOnly", "Secure", "SameSite=Strict", "Path=/"] {
			assert!(set.split(';').any(|p| p.trim() == attr), "{attr} missing in {set}");
		}
		let token = set.split(';').next().unwrap().trim_start_matches("surreal_token=").to_string();
		let body = res.text().await.unwrap();
		assert!(!body.contains(&token), "the token must not be in the body: {body}");
		let body: serde_json::Value = serde_json::from_str(&body).unwrap();
		assert_eq!(body["code"], 200);
		assert!(body.get("token").is_none());
	});
}

#[test]
fn signin_cookie_max_age_is_the_token_lifetime() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		let cookie = {
			let res = server
				.request(Method::POST, "/signin/cookie")
				.header("Origin", APP)
				.body(format!(r#"{{"user":"{USER}","pass":"{PASS}"}}"#))
				.send()
				.await
				.unwrap();
			set_cookie(&res).unwrap()
		};
		let age = max_age(&cookie);
		assert!((3500..=3600).contains(&age), "Max-Age {age} is not about the token hour");
	});
}

#[test]
fn plain_signin_still_returns_the_token_with_cookie_auth_on() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		let res = server
			.request(Method::POST, "/signin")
			.header("Origin", APP)
			.header("Accept", "application/json")
			.body(format!(r#"{{"user":"{USER}","pass":"{PASS}"}}"#))
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::OK);
		assert!(set_cookie(&res).is_none());
		let body: serde_json::Value = res.json().await.unwrap();
		assert!(body["token"].is_string(), "{body}");
	});
}

#[test]
fn signin_from_a_foreign_origin_is_forbidden() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		let res = server
			.request(Method::POST, "/signin/cookie")
			.header("Origin", EVIL)
			.header("Accept", "application/json")
			.body(format!(r#"{{"user":"{USER}","pass":"{PASS}"}}"#))
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::FORBIDDEN);
		assert!(set_cookie(&res).is_none());
	});
}

#[test]
fn csrf_from_a_foreign_origin_runs_anonymous() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		let cookie = server.cookie().await;
		let res =
			server.sql().header("Cookie", &cookie).header("Origin", EVIL).send().await.unwrap();
		assert!(!ran_as_root(res).await);
	});
}

#[test]
fn csrf_from_a_sibling_subdomain_runs_anonymous() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		let cookie = server.cookie().await;
		let res = server
			.sql()
			.header("Cookie", &cookie)
			.header("Origin", "http://sub.app.test")
			.send()
			.await
			.unwrap();
		assert!(!ran_as_root(res).await);
	});
}

#[test]
fn allowed_origin_runs_authenticated() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		let cookie = server.cookie().await;
		let res =
			server.sql().header("Cookie", &cookie).header("Origin", APP).send().await.unwrap();
		assert!(ran_as_root(res).await);
	});
}

#[test]
fn cookie_without_browser_headers_runs_anonymous() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		let cookie = server.cookie().await;
		let res = server.sql().header("Cookie", &cookie).send().await.unwrap();
		assert!(!ran_as_root(res).await);
	});
}

#[test]
fn same_origin_fetch_metadata_runs_authenticated() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		let cookie = server.cookie().await;
		let res = server
			.sql()
			.header("Cookie", &cookie)
			.header("Sec-Fetch-Site", "same-origin")
			.send()
			.await
			.unwrap();
		assert!(ran_as_root(res).await);
		// Fetch metadata that says cross-site, with no origin, is a browser and denied.
		let res = server
			.sql()
			.header("Cookie", &cookie)
			.header("Sec-Fetch-Site", "cross-site")
			.send()
			.await
			.unwrap();
		assert!(!ran_as_root(res).await);
	});
}

#[test]
fn authorization_header_wins_over_the_cookie() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		let cookie = server.cookie().await;
		// A garbage bearer token is a 401, which a cookie must not rescue or replace.
		let res = server
			.sql()
			.header("Cookie", &cookie)
			.header("Origin", APP)
			.header("Authorization", "Bearer garbage")
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
		// Basic credentials are used even though the cookie would also have worked.
		let res = server
			.sql()
			.header("Cookie", "surreal_token=garbage")
			.header("Origin", APP)
			.basic_auth(USER, Some(PASS))
			.send()
			.await
			.unwrap();
		assert!(ran_as_root(res).await);
	});
}

#[test]
fn dead_cookie_is_anonymous_and_cleared() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		for value in [
			"garbage",
			// {"alg":"HS256","typ":"JWT"}.{"exp":1}.sig: expired
			"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjF9.c2ln",
		] {
			let res = server
				.sql()
				.header("Cookie", format!("{COOKIE_NAME}={value}"))
				.header("Origin", APP)
				.send()
				.await
				.unwrap();
			let set = set_cookie(&res).expect("a clearing Set-Cookie");
			assert!(set.starts_with("surreal_token=;"), "{set}");
			assert_eq!(max_age(&set), 0);
			assert!(!ran_as_root(res).await);
		}
	});
}

#[test]
fn signout_clears_the_cookie() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		// A dead cookie does not block its own removal.
		let res = server
			.request(Method::POST, "/signout")
			.header("Origin", APP)
			.header("Cookie", "surreal_token=garbage")
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::OK);
		let set = set_cookie(&res).expect("a clearing Set-Cookie");
		assert_eq!(set, "surreal_token=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0");
		let res =
			server.request(Method::POST, "/signout").header("Origin", EVIL).send().await.unwrap();
		assert_eq!(res.status(), StatusCode::FORBIDDEN);
		assert!(set_cookie(&res).is_none());
	});
}

#[test]
fn cookie_auth_requires_an_explicit_allowlist() {
	with_big_stack(|| async {
		let (datastore, recv) = datastore().await;
		let result = SurrealRouter::build::<CommunityComposer>(
			RouterOptions {
				cookie_auth: true,
				..RouterOptions::default()
			},
			datastore,
			recv,
			CancellationToken::new(),
			(),
		)
		.await;
		let err = result.err().expect("a startup error");
		assert!(
			err.to_string()
				.contains("cookie authentication requires an explicit --allow-origin list"),
			"{err}"
		);
	});
}

#[test]
fn cookie_is_never_read_when_the_feature_is_off() {
	with_big_stack(|| async {
		let on = TestServer::cookies().await;
		let cookie = on.cookie().await;
		let off = TestServer::start(RouterOptions {
			allow_origin: vec![APP.to_string()],
			..RouterOptions::default()
		})
		.await;
		// The token is a valid root token for the other server only; what matters
		// is that the off server neither reads nor clears it.
		let res = off.sql().header("Cookie", &cookie).header("Origin", APP).send().await.unwrap();
		assert!(set_cookie(&res).is_none());
		assert!(!ran_as_root(res).await);
		let res = off.request(Method::POST, "/signout").header("Origin", APP).send().await.unwrap();
		assert_eq!(res.status(), StatusCode::NOT_FOUND);
		// The cookie sign-in route does not exist either.
		let res = off
			.request(Method::POST, "/signin/cookie")
			.header("Origin", APP)
			.header("Accept", "application/json")
			.body(format!(r#"{{"user":"{USER}","pass":"{PASS}"}}"#))
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::NOT_FOUND);
		assert!(set_cookie(&res).is_none());
	});
}

/// Opens a Json `/rpc` WebSocket carrying `cookie`, from `origin`, and returns
/// the result of `INFO FOR ROOT` as the first reply.
async fn ws_info(server: &TestServer, cookie: &str, origin: &str) -> serde_json::Value {
	let mut ws = connect_ws(server, cookie, origin).await;
	ws_query(&mut ws).await
}

type Ws =
	tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect_ws(server: &TestServer, cookie: &str, origin: &str) -> Ws {
	let mut request =
		format!("ws://{}/rpc", server.address).into_client_request().expect("a websocket request");
	let headers = request.headers_mut();
	headers.insert("Sec-WebSocket-Protocol", "json".parse().expect("header"));
	headers.insert("Cookie", cookie.parse().expect("header"));
	headers.insert("Origin", origin.parse().expect("header"));
	connect_async(request).await.expect("connect").0
}

async fn ws_query(ws: &mut Ws) -> serde_json::Value {
	let body = serde_json::json!({"id": 1, "method": "query", "params": ["INFO FOR ROOT;"]});
	ws.send(Message::Text(body.to_string().into())).await.expect("send");
	loop {
		let msg = tokio::time::timeout(READ_TIMEOUT, ws.next())
			.await
			.expect("a reply in time")
			.expect("an open connection")
			.expect("a websocket message");
		let text = match msg {
			Message::Text(t) => t.to_string(),
			Message::Binary(b) => String::from_utf8(b.to_vec()).expect("utf8"),
			_ => continue,
		};
		return serde_json::from_str(&text).expect("json reply");
	}
}

fn ws_ran_as_root(reply: &serde_json::Value) -> bool {
	reply.get("error").is_none() && reply["result"][0]["status"] == "OK"
}

#[test]
fn websocket_upgrade_honours_the_origin_gate() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		let cookie = server.cookie().await;
		let reply = ws_info(&server, &cookie, EVIL).await;
		assert!(!ws_ran_as_root(&reply), "{reply}");
		let reply = ws_info(&server, &cookie, APP).await;
		assert!(ws_ran_as_root(&reply), "{reply}");
	});
}

#[test]
fn websocket_seeded_by_a_cookie_stops_when_the_session_expires() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		{
			let res = server
				.request(Method::POST, "/sql")
				.header("Content-Type", "text/plain")
				.header("Accept", "application/json")
				.header("Surreal-NS", "t")
				.header("Surreal-DB", "t")
				.basic_auth(USER, Some(PASS))
				.body(
					"DEFINE NAMESPACE t; USE NS t; DEFINE DATABASE t; USE DB t; \
					 DEFINE ACCESS acc ON DATABASE TYPE RECORD \
					 SIGNUP (CREATE user SET email = $email) \
					 DURATION FOR SESSION 2s, FOR TOKEN 1h;",
				)
				.send()
				.await
				.unwrap();
			assert_eq!(res.status(), StatusCode::OK);
		}
		let res = server
			.request(Method::POST, "/signup/cookie")
			.header("Origin", APP)
			.header("Accept", "application/json")
			.body(r#"{"ns":"t","db":"t","ac":"acc","email":"a@b.test"}"#)
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::OK);
		let set = set_cookie(&res).expect("a Set-Cookie header");
		let age = max_age(&set);
		assert!((1..=2).contains(&age), "Max-Age {age} follows the session, which is shorter");
		let cookie = set.split(';').next().unwrap().to_string();
		let mut ws = connect_ws(&server, &cookie, APP).await;
		let reply = ws_query(&mut ws).await;
		assert!(reply["result"][0]["status"] == "ERR", "a record user is not root: {reply}");
		tokio::time::sleep(Duration::from_millis(3200)).await;
		let reply = ws_query(&mut ws).await;
		let text = reply.to_string().to_lowercase();
		assert!(reply.get("error").is_some() && text.contains("expired"), "{reply}");
	});
}

#[test]
fn api_handlers_never_see_the_session_cookie() {
	with_big_stack(|| async {
		let server = TestServer::cookies().await;
		let res = server
			.request(Method::POST, "/sql")
			.basic_auth(USER, Some(PASS))
			.header("Surreal-NS", "t")
			.header("Surreal-DB", "t")
			.header("Accept", "application/json")
			.body(
				"DEFINE API \"/echo\" FOR get PERMISSIONS FULL THEN { \
				 { status: 200, body: <string> $request.headers[\"cookie\"] } };",
			)
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::OK);
		assert!(!res.text().await.unwrap().contains("\"ERR\""));
		let cookie = server.cookie().await;
		let token = cookie.trim_start_matches("surreal_token=").to_string();
		let res = server
			.request(Method::GET, "/api/t/t/echo")
			.header("Origin", APP)
			.header("Accept", "application/json")
			.header("Cookie", format!("theme=dark; {cookie}"))
			.send()
			.await
			.unwrap();
		let status = res.status();
		let body = res.text().await.unwrap();
		assert_eq!(status, StatusCode::OK, "{body}");
		assert!(!body.contains(&token), "the session cookie reached user code: {body}");
		assert!(body.contains("theme=dark"), "other cookies are kept: {body}");
	});
}
