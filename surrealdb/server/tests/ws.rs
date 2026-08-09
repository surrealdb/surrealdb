//! End-to-end tests for the WebSocket streaming protocol (`query_stream`).
//!
//! Each test serves the real community router -- the same tree, middleware
//! and `axum_server` setup the `surreal start` path builds -- on an ephemeral
//! port, and drives it with a raw WebSocket client rather than an SDK. The
//! raw client is the point: what is under test is the wire protocol itself --
//! the frame envelopes, their ordering, their correlation by request id -- as
//! any SDK would observe them, without an SDK's own accumulation in between.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use surrealdb_core::CommunityComposer;
use surrealdb_core::kvs::Datastore;
use surrealdb_rpc::capabilities::Capabilities;
use surrealdb_server::ntw::{RouterOptions, SurrealRouter};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tokio_util::sync::CancellationToken;

const USER: &str = "root";
const PASS: &str = "root";

/// How long any single read may take before the test is called stuck.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// A server serving the community router on an ephemeral port, shut down when
/// dropped.
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
			.with_capabilities(Capabilities::all())
			.with_notify(send)
			.with_auth(true)
			.build_with_path("memory")
			.await
			.expect("datastore");
		datastore.initialise_credentials(USER, PASS).await.expect("root credentials");
		let datastore = Arc::new(datastore);

		let canceller = CancellationToken::new();
		let router = SurrealRouter::build::<CommunityComposer>(
			RouterOptions::default(),
			Arc::clone(&datastore),
			recv,
			canceller.clone(),
			(),
		)
		.await
		.expect("router");
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

	/// Connects a raw Json-format WebSocket client, authenticated as root
	/// against the `test`/`test` namespace and database.
	async fn connect(&self) -> WsClient {
		let mut request = format!("ws://{}/rpc", self.address)
			.into_client_request()
			.expect("a websocket request");
		request
			.headers_mut()
			.insert("Sec-WebSocket-Protocol", "json".parse().expect("a protocol header"));
		let (stream, _) = connect_async(request).await.expect("connect");
		let mut client = WsClient {
			stream,
		};
		client
			.request(serde_json::json!({
				"id": "auth",
				"method": "signin",
				"params": [{ "user": USER, "pass": PASS }],
			}))
			.await;
		client
			.request(serde_json::json!({
				"id": "use",
				"method": "use",
				"params": ["test", "test"],
			}))
			.await;
		client
	}
}

/// A raw WebSocket RPC client speaking the Json format.
struct WsClient {
	stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl WsClient {
	async fn send(&mut self, body: serde_json::Value) {
		self.stream.send(Message::Text(body.to_string().into())).await.expect("send");
	}

	/// The next data message, decoded. Control frames are skipped.
	async fn recv(&mut self) -> serde_json::Value {
		loop {
			let msg = tokio::time::timeout(READ_TIMEOUT, self.stream.next())
				.await
				.expect("a message within the read timeout")
				.expect("an open connection")
				.expect("a websocket message");
			match msg {
				Message::Text(text) => {
					return serde_json::from_str(&text).expect("valid response json");
				}
				Message::Binary(bytes) => {
					return serde_json::from_slice(&bytes).expect("valid response json");
				}
				_ => continue,
			}
		}
	}

	/// Send a request and return its response, skipping unrelated messages.
	async fn request(&mut self, body: serde_json::Value) -> serde_json::Value {
		let id = body["id"].clone();
		self.send(body).await;
		loop {
			let msg = self.recv().await;
			if msg["id"] == id {
				return msg;
			}
		}
	}

	/// Send a `query_stream` request and collect every frame through `end`.
	async fn stream(&mut self, id: &str, sql: &str) -> Vec<serde_json::Value> {
		self.send(serde_json::json!({
			"id": id,
			"method": "query_stream",
			"params": [sql],
		}))
		.await;
		let mut frames = Vec::new();
		loop {
			let msg = self.recv().await;
			if msg["id"] != id {
				continue;
			}
			let done = frame_tag(&msg) == Some("end") || msg.get("error").is_some();
			frames.push(msg);
			if done {
				return frames;
			}
		}
	}
}

/// The frame tag of a message, when it is a stream frame.
fn frame_tag(msg: &serde_json::Value) -> Option<&str> {
	msg.get("result")?.get("stream")?.as_str()
}

/// Run a test body on a dedicated OS thread + multi-threaded runtime with a
/// 24 MiB stack. The server runs in-process here, and the executor and parser
/// carry large stack frames in debug builds that overflow tokio's default
/// 2 MiB worker stack; same pattern as the `rpc::websocket` unit tests.
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

/// Rebuild each statement's value from its frames, as an SDK's accumulator
/// would: rows concatenate into an array, a `single` statement is its bare
/// value, and an errored statement is its error message.
fn reconstruct(frames: &[serde_json::Value]) -> Vec<serde_json::Value> {
	let mut rows: std::collections::BTreeMap<i64, Vec<serde_json::Value>> = Default::default();
	let mut singles: std::collections::BTreeMap<i64, serde_json::Value> = Default::default();
	let mut results: std::collections::BTreeMap<i64, serde_json::Value> = Default::default();
	for msg in frames {
		let frame = &msg["result"];
		let index = || frame["index"].as_i64().expect("a statement index");
		match frame_tag(msg) {
			Some("rows") => rows
				.entry(index())
				.or_default()
				.extend(frame["values"].as_array().expect("a values array").iter().cloned()),
			Some("value") => {
				singles.insert(index(), frame["value"].clone());
			}
			Some("finished") => {
				let index = index();
				let value = if let Some(error) = frame.get("error") {
					rows.remove(&index);
					singles.remove(&index);
					error["message"].clone()
				} else if frame["single"] == true {
					singles.remove(&index).expect("a single statement sent its value")
				} else {
					serde_json::Value::Array(rows.remove(&index).unwrap_or_default())
				};
				results.insert(index, value);
			}
			_ => {}
		}
	}
	results.into_values().collect()
}

/// The streamed answer must be the buffered answer, delivered as frames: the
/// same statements, the same values, in the same order.
#[test]
fn streamed_results_match_the_buffered_response() {
	with_big_stack(|| async {
		let server = TestServer::start().await;
		let mut client = server.connect().await;
		let seeded = client
			.request(serde_json::json!({
				"id": "seed",
				"method": "query",
				"params": ["CREATE |wide:100| SET n = rand::int(0, 9) RETURN NONE"],
			}))
			.await;
		assert!(seeded.get("error").is_none(), "seeding succeeds: {seeded}");

		let sql = "SELECT * FROM wide ORDER BY id; RETURN 42; SELECT count() FROM wide GROUP ALL;";
		let buffered = client
			.request(serde_json::json!({ "id": "q", "method": "query", "params": [sql] }))
			.await;
		let buffered: Vec<serde_json::Value> = buffered["result"]
			.as_array()
			.expect("a buffered result per statement")
			.iter()
			.map(|r| r["result"].clone())
			.collect();

		let frames = client.stream("s", sql).await;
		assert_eq!(frame_tag(&frames[0]), Some("begin"));
		assert_eq!(frames[0]["result"]["statements"], 3);
		let streamed = reconstruct(&frames);

		assert_eq!(streamed, buffered, "the stream carries exactly the buffered answer");
		let end = &frames[frames.len() - 1];
		assert_eq!(frame_tag(end), Some("end"));
		assert_eq!(end["result"]["results"], 3);
	});
}

/// The first rows must arrive while the query is still executing. A trailing
/// `SLEEP` pins the stream open long after the first statement's rows are
/// produced, so the gap between the first rows frame and the end frame proves
/// delivery happened mid-query rather than after it.
#[test]
fn rows_arrive_before_the_query_finishes_over_websocket() {
	with_big_stack(|| async {
		let server = TestServer::start().await;
		let mut client = server.connect().await;
		client
			.request(serde_json::json!({
				"id": "seed",
				"method": "query",
				"params": ["CREATE |wide:50| SET n = 1 RETURN NONE"],
			}))
			.await;

		client
			.send(serde_json::json!({
				"id": "s",
				"method": "query_stream",
				"params": ["SELECT * FROM wide; SLEEP 1s;"],
			}))
			.await;
		let mut first_rows = None;
		let end;
		loop {
			let msg = client.recv().await;
			if msg["id"] != "s" {
				continue;
			}
			match frame_tag(&msg) {
				Some("rows") if first_rows.is_none() => {
					first_rows = Some(tokio::time::Instant::now());
				}
				Some("end") => {
					end = tokio::time::Instant::now();
					break;
				}
				_ => {}
			}
		}
		let first_rows = first_rows.expect("rows were streamed");
		assert!(
			end.duration_since(first_rows) >= Duration::from_millis(800),
			"the first rows must precede the query's completion by the sleep",
		);
	});
}

/// A `LIVE SELECT` registered through a stream delivers notifications over
/// the same connection, exactly as one registered through the buffered path.
#[test]
fn a_streamed_live_select_delivers_notifications() {
	with_big_stack(|| async {
		let server = TestServer::start().await;
		let mut client = server.connect().await;
		client
			.request(serde_json::json!({
				"id": "seed",
				"method": "query",
				"params": ["DEFINE TABLE thing"],
			}))
			.await;

		let frames = client.stream("live", "LIVE SELECT * FROM thing;").await;
		let lqid = frames
			.iter()
			.find(|m| frame_tag(m) == Some("value"))
			.expect("the live query id arrives as a value frame")["result"]["value"]
			.as_str()
			.expect("a live query id")
			.to_string();

		// A write from a second connection produces a notification on this one.
		let mut writer = server.connect().await;
		writer
			.request(serde_json::json!({
				"id": "w",
				"method": "query",
				"params": ["CREATE thing:1 SET x = 1"],
			}))
			.await;

		loop {
			let msg = client.recv().await;
			// Live notifications carry no request id.
			if msg.get("id").is_none()
				&& msg["result"]["action"] == "CREATE"
				&& msg["result"]["id"] == lqid.as_str()
			{
				return;
			}
		}
	});
}

/// `query_cancel` reaches an in-flight stream over the wire and the stream
/// still ends cleanly, well before the query would have finished on its own.
#[test]
fn a_stream_cancelled_over_the_wire_ends_promptly() {
	with_big_stack(|| async {
		let server = TestServer::start().await;
		let mut client = server.connect().await;
		let started = tokio::time::Instant::now();

		client
			.send(serde_json::json!({
				"id": "c",
				"method": "query_stream",
				"params": ["SLEEP 30s; RETURN 1;"],
			}))
			.await;
		loop {
			let msg = client.recv().await;
			if msg["id"] == "c" && frame_tag(&msg) == Some("begin") {
				break;
			}
		}

		let cancelled = client
			.request(serde_json::json!({
				"id": "k",
				"method": "query_cancel",
				"params": ["c"],
			}))
			.await;
		assert!(cancelled.get("error").is_none(), "the cancel succeeds: {cancelled}");

		loop {
			let msg = client.recv().await;
			if msg["id"] == "c" && frame_tag(&msg) == Some("end") {
				break;
			}
		}
		assert!(started.elapsed() < Duration::from_secs(20), "the cancel interrupts the sleep");
	});
}

/// The unknown-method answer is what an SDK's transparent fallback keys on: a
/// server that does not recognise a method must answer that request — id
/// echoed — with the Method-not-found wire code, leaving the connection
/// usable.
#[test]
fn an_unknown_method_answers_with_method_not_found() {
	with_big_stack(|| async {
		let server = TestServer::start().await;
		let mut client = server.connect().await;
		let response = client
			.request(serde_json::json!({
				"id": "probe",
				"method": "query_stream_and_a_suffix_no_server_knows",
				"params": [],
			}))
			.await;
		assert_eq!(response["error"]["code"], -32601, "the wire code SDK fallbacks key on");
		assert_eq!(response["error"]["message"], "Method not found");

		// The connection survives the probe.
		let alive = client
			.request(
				serde_json::json!({ "id": "after", "method": "query", "params": ["RETURN 1"] }),
			)
			.await;
		assert!(alive.get("error").is_none(), "the connection is still usable: {alive}");
	});
}

/// An ordinary buffered `query` and live notifications keep flowing while a
/// stream occupies the connection: frames interleave rather than block.
#[test]
fn a_stream_does_not_block_the_connection() {
	with_big_stack(|| async {
		let server = TestServer::start().await;
		let mut client = server.connect().await;

		client
			.send(serde_json::json!({
				"id": "bg",
				"method": "query_stream",
				"params": ["SLEEP 2s; RETURN 'done';"],
			}))
			.await;
		loop {
			let msg = client.recv().await;
			if msg["id"] == "bg" && frame_tag(&msg) == Some("begin") {
				break;
			}
		}

		// A buffered query on the same connection answers while the stream is
		// mid-SLEEP.
		let response = client
			.request(serde_json::json!({ "id": "fg", "method": "query", "params": ["RETURN 7"] }))
			.await;
		assert!(response.get("error").is_none(), "the interleaved query succeeds: {response}");
		assert_eq!(response["result"][0]["result"], 7);

		// And the stream still completes normally afterwards.
		loop {
			let msg = client.recv().await;
			if msg["id"] == "bg" && frame_tag(&msg) == Some("end") {
				let end = &msg["result"];
				assert!(end.get("error").is_none(), "the stream ends cleanly: {msg}");
				break;
			}
		}
	});
}
