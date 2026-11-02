//! End-to-end tests for the `cbor.2` wire revision and its negotiation over
//! WebSocket (`Sec-WebSocket-Protocol: cbor.2, cbor`) and HTTP
//! (`Accept: application/cbor; v=2` / `Content-Type: application/cbor; v=2`).
//!
//! `CborWsClient` and `CborHttpClient` implement the SDK negotiation contract:
//! - Offer `cbor.2` with `cbor` as the fallback and read the selected revision from the server's
//!   response (`Sec-WebSocket-Protocol` on WebSocket `101`, `Content-Type` on HTTP responses).
//! - Send only frozen `cbor` until the server has answered `cbor.2` on the connection.
//! - Decode responses according to the negotiated revision.

// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]
#![recursion_limit = "256"]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use surrealdb_core::CommunityComposer;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::format::cbor::{decode, decode_v2, encode, encode_v2};
use surrealdb_rpc::capabilities::Capabilities;
use surrealdb_server::ntw::{RouterOptions, SurrealRouter};
use surrealdb_types::{Number, Object, ToSql, Value, f16};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tokio_util::sync::CancellationToken;

const USER: &str = "root";
const PASS: &str = "root";
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
}

/// Reference WebSocket CBOR client implementing the `cbor.2` negotiation rules.
struct CborWsClient {
	stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
	selected_protocol: String,
	negotiated_v2: bool,
}

impl CborWsClient {
	/// Connects offering `protocols` (for example `"cbor.2, cbor"` or `"cbor"`)
	/// and authenticates as root on `test`/`test`.
	async fn connect(address: SocketAddr, protocols: &str) -> Self {
		let mut request =
			format!("ws://{address}/rpc").into_client_request().expect("a websocket request");
		request
			.headers_mut()
			.insert("Sec-WebSocket-Protocol", protocols.parse().expect("a protocol header"));
		let (stream, response) = connect_async(request).await.expect("connect");
		let selected_protocol = response
			.headers()
			.get("Sec-WebSocket-Protocol")
			.and_then(|h| h.to_str().ok())
			.unwrap_or("")
			.to_owned();
		let negotiated_v2 = selected_protocol == "cbor.2";
		let mut client = Self {
			stream,
			selected_protocol,
			negotiated_v2,
		};
		let mut auth = Object::default();
		auth.insert("user".to_owned(), Value::String(USER.to_owned()));
		auth.insert("pass".to_owned(), Value::String(PASS.to_owned()));
		let res = client.rpc("auth", "signin", vec![Value::Object(auth)]).await;
		assert!(obj_get(&res, "error").is_none(), "signin failed: {res:?}");
		let res = client
			.rpc(
				"use",
				"use",
				vec![Value::String("test".to_owned()), Value::String("test".to_owned())],
			)
			.await;
		assert!(obj_get(&res, "error").is_none(), "use failed: {res:?}");
		client
	}

	fn encode_msg(&self, val: Value) -> Vec<u8> {
		if self.negotiated_v2 {
			encode_v2(val).expect("encode_v2")
		} else {
			encode(val).expect("encode")
		}
	}

	fn decode_msg(&self, bytes: &[u8]) -> Value {
		if self.negotiated_v2 {
			decode_v2(bytes, 128).expect("decode_v2")
		} else {
			decode(bytes, 128).expect("decode")
		}
	}

	async fn send_bytes(&mut self, bytes: Vec<u8>) {
		self.stream.send(Message::Binary(bytes.into())).await.expect("send");
	}

	async fn recv_raw(&mut self) -> Vec<u8> {
		loop {
			let msg = tokio::time::timeout(READ_TIMEOUT, self.stream.next())
				.await
				.expect("a message within the read timeout")
				.expect("an open connection")
				.expect("a websocket message");
			match msg {
				Message::Binary(bytes) => return bytes.to_vec(),
				Message::Text(text) => return text.as_bytes().to_vec(),
				Message::Ping(_) | Message::Pong(_) | Message::Close(_) | Message::Frame(_) => {
					continue;
				}
			}
		}
	}

	async fn rpc_raw(&mut self, id: &str, method: &str, params: Vec<Value>) -> Vec<u8> {
		let req = make_rpc_request(id, method, params);
		let bytes = self.encode_msg(req);
		self.send_bytes(bytes).await;
		loop {
			let raw = self.recv_raw().await;
			let msg = self.decode_msg(&raw);
			if obj_get(&msg, "id") == Some(&Value::String(id.to_owned())) {
				return raw;
			}
		}
	}

	async fn rpc(&mut self, id: &str, method: &str, params: Vec<Value>) -> Value {
		let raw = self.rpc_raw(id, method, params).await;
		self.decode_msg(&raw)
	}
}

/// Reference HTTP CBOR client implementing the `cbor.2` negotiation rules:
/// - Sends `Accept: application/cbor; v=2` (or `application/cbor` when `offer_v2` is false).
/// - Sends `Content-Type: application/cbor` until a response on this client has answered
///   `Content-Type: application/cbor; v=2`, after which requests use `Content-Type:
///   application/cbor; v=2` and `cbor.2` encoding.
struct CborHttpClient {
	address: SocketAddr,
	http: reqwest::Client,
	offer_v2: bool,
	negotiated_v2: bool,
	last_response_content_type: String,
}

impl CborHttpClient {
	fn new(address: SocketAddr, offer_v2: bool) -> Self {
		Self {
			address,
			http: reqwest::Client::new(),
			offer_v2,
			negotiated_v2: false,
			last_response_content_type: String::new(),
		}
	}

	async fn rpc(&mut self, id: &str, method: &str, params: Vec<Value>) -> Value {
		let req_val = make_rpc_request(id, method, params);
		let (content_type, body) = if self.negotiated_v2 {
			(surrealdb_rpc::format::CBOR2, encode_v2(req_val).expect("encode_v2"))
		} else {
			(surrealdb_rpc::format::CBOR, encode(req_val).expect("encode"))
		};
		let accept = if self.offer_v2 {
			surrealdb_rpc::format::CBOR2
		} else {
			surrealdb_rpc::format::CBOR
		};
		let response = self
			.http
			.post(format!("http://{}/rpc", self.address))
			.basic_auth(USER, Some(PASS))
			.header("surreal-ns", "test")
			.header("surreal-db", "test")
			.header(reqwest::header::ACCEPT, accept)
			.header(reqwest::header::CONTENT_TYPE, content_type)
			.body(body)
			.send()
			.await
			.expect("http post");
		let resp_ct = response
			.headers()
			.get(reqwest::header::CONTENT_TYPE)
			.and_then(|h| h.to_str().ok())
			.unwrap_or("")
			.to_owned();
		let is_v2 = resp_ct == surrealdb_rpc::format::CBOR2;
		self.negotiated_v2 = is_v2;
		self.last_response_content_type = resp_ct;
		let bytes = response.bytes().await.expect("response bytes");
		if is_v2 {
			decode_v2(&bytes, 128).expect("decode_v2")
		} else {
			decode(&bytes, 128).expect("decode")
		}
	}
}

fn make_rpc_request(id: &str, method: &str, params: Vec<Value>) -> Value {
	let mut map = Object::default();
	map.insert("id".to_owned(), Value::String(id.to_owned()));
	map.insert("method".to_owned(), Value::String(method.to_owned()));
	map.insert("params".to_owned(), Value::Array(params.into()));
	Value::Object(map)
}

fn obj_get<'a>(val: &'a Value, key: &str) -> Option<&'a Value> {
	if let Value::Object(map) = val {
		map.get(key)
	} else {
		None
	}
}

/// Extracts the inner `result` of the first statement from an RPC `query` response.
fn first_query_result(response: &Value) -> &Value {
	assert!(obj_get(response, "error").is_none(), "unexpected RPC error: {response:?}");
	let Some(Value::Array(statements)) = obj_get(response, "result") else {
		panic!("expected result array in RPC response: {response:?}");
	};
	let first = statements.first().expect("at least one statement result");
	assert_eq!(
		obj_get(first, "status"),
		Some(&Value::String("OK".to_owned())),
		"statement error: {first:?}"
	);
	obj_get(first, "result").expect("statement result field")
}

fn sample_sized_numbers() -> Vec<(Number, &'static str)> {
	vec![
		(Number::I8(i8::MIN), "i8"),
		(Number::I8(42), "i8"),
		(Number::U8(u8::MAX), "u8"),
		(Number::I16(i16::MIN), "i16"),
		(Number::U16(u16::MAX), "u16"),
		(Number::I32(i32::MIN), "i32"),
		(Number::U32(u32::MAX), "u32"),
		(Number::U64(u64::MAX), "u64"),
		(Number::F16(f16::from_f32(1.5)), "f16"),
		(Number::F32(3.25_f32), "f32"),
	]
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
fn websocket_negotiates_cbor2_and_preserves_sized_number_widths() {
	with_big_stack(|| async {
		let server = TestServer::start().await;
		let mut client = CborWsClient::connect(server.address, "cbor.2, cbor").await;
		assert_eq!(client.selected_protocol, "cbor.2");
		assert!(client.negotiated_v2);

		// Every sized kind bound as a parameter preserves its width in the engine and in the
		// response.
		for (i, (num, expected_kind)) in sample_sized_numbers().into_iter().enumerate() {
			let mut vars = Object::default();
			vars.insert("x".to_owned(), Value::Number(num.clone()));
			let res = client
				.rpc(
					&format!("q_{i}"),
					"query",
					vec![
						Value::String("RETURN [$x, type::of($x)];".to_owned()),
						Value::Object(vars),
					],
				)
				.await;
			let result = first_query_result(&res);
			assert_eq!(
				result,
				&Value::Array(
					vec![Value::Number(num.clone()), Value::String(expected_kind.to_owned())]
						.into()
				),
				"failed round-trip for {num:?} ({expected_kind})"
			);
		}

		// Streaming (`query_stream`) over `cbor.2` also encodes `rows` frames with `cbor.2` tags.
		let req = make_rpc_request(
			"stream_1",
			"query_stream",
			vec![Value::String(
				"SELECT VALUE v FROM [{ v: 7i8 }, { v: 9u16 }, { v: 1.5f16 }, { v: 2.25f32 }];"
					.to_owned(),
			)],
		);
		let bytes = client.encode_msg(req);
		client.send_bytes(bytes).await;
		let mut collected = Vec::new();
		loop {
			let raw = client.recv_raw().await;
			let msg = client.decode_msg(&raw);
			if obj_get(&msg, "id") != Some(&Value::String("stream_1".to_owned())) {
				continue;
			}
			let Some(frame) = obj_get(&msg, "result") else {
				panic!("unexpected stream error frame: {msg:?}");
			};
			let tag = obj_get(frame, "stream");
			if tag == Some(&Value::String("rows".to_owned()))
				&& let Some(Value::Array(vals)) = obj_get(frame, "values")
			{
				collected.extend(vals.iter().cloned());
			}
			if tag == Some(&Value::String("end".to_owned())) {
				break;
			}
		}
		assert_eq!(
			collected,
			vec![
				Value::Number(Number::I8(7)),
				Value::Number(Number::U16(9)),
				Value::Number(Number::F16(f16::from_f32(1.5))),
				Value::Number(Number::F32(2.25)),
			]
		);
	});
}

#[test]
fn websocket_unnegotiated_cbor_is_frozen_and_refuses_v2_tags() {
	with_big_stack(|| async {
		let server = TestServer::start().await;
		let mut v1_client = CborWsClient::connect(server.address, "cbor").await;
		let mut v2_client = CborWsClient::connect(server.address, "cbor.2, cbor").await;
		assert_eq!(v1_client.selected_protocol, "cbor");
		assert!(!v1_client.negotiated_v2);
		assert_eq!(v2_client.selected_protocol, "cbor.2");

		// 1. Sized numbers in responses over `cbor` widen to untagged Int / Float.
		let res = v1_client
			.rpc(
				"widen",
				"query",
				vec![Value::String(
					"RETURN [5i8, 5u8, 5i16, 5u16, 5i32, 5u32, 5u64, 1.5f16, 2.25f32];".to_owned(),
				)],
			)
			.await;
		let result = first_query_result(&res);
		assert_eq!(
			result,
			&Value::Array(
				vec![
					Value::Number(Number::Int(5)),
					Value::Number(Number::Int(5)),
					Value::Number(Number::Int(5)),
					Value::Number(Number::Int(5)),
					Value::Number(Number::Int(5)),
					Value::Number(Number::Int(5)),
					Value::Number(Number::Int(5)),
					Value::Number(Number::Float(1.5)),
					Value::Number(Number::Float(2.25)),
				]
				.into()
			)
		);

		// 2. Pre-existing value kinds produce identical response bytes on `cbor` and `cbor.2`.
		// We compare the encoded `result` payload of each query so per-execution timing metadata
		// (`time: "12µs"`) does not differ.
		let pre_existing_queries = [
			"RETURN [NONE, NULL, true, false, 42, -7, 3.5f, 12.34dec, 'hello', b'cafe'];",
			"RETURN [d'2025-01-02T03:04:05Z', 1h30m, u'018f4a60-1234-7000-8000-000000000001'];",
			"RETURN [{ a: 1, b: [2, 3] }, person:alice, person:42, 1..=10, (1.0, 2.0)];",
		];
		for (idx, sql) in pre_existing_queries.iter().enumerate() {
			let id = format!("compat_{idx}");
			let v1_msg = v1_client.rpc(&id, "query", vec![Value::String((*sql).to_owned())]).await;
			let v2_msg = v2_client.rpc(&id, "query", vec![Value::String((*sql).to_owned())]).await;
			let v1_val = first_query_result(&v1_msg).clone();
			let v2_val = first_query_result(&v2_msg).clone();
			assert_eq!(v1_val, v2_val);
			assert_eq!(
				encode(v1_val).expect("encode"),
				encode_v2(v2_val).expect("encode_v2"),
				"wire bytes diverged for query {sql}"
			);
		}

		// 3. Sending a `cbor.2`-tagged request on an unnegotiated `cbor` WebSocket is refused,
		// and the error response still echoes the request `id`.
		let mut vars = Object::default();
		vars.insert("x".to_owned(), Value::Number(Number::U8(7)));
		let bad_req = make_rpc_request(
			"bad_v2_on_v1",
			"query",
			vec![Value::String("RETURN $x;".to_owned()), Value::Object(vars)],
		);
		let raw_v2_bytes = encode_v2(bad_req).expect("encode_v2");
		v1_client.send_bytes(raw_v2_bytes).await;
		let raw_err = v1_client.recv_raw().await;
		let err_msg = v1_client.decode_msg(&raw_err);
		assert_eq!(
			obj_get(&err_msg, "id"),
			Some(&Value::String("bad_v2_on_v1".to_owned())),
			"refused request must echo request id: {err_msg:?}"
		);
		assert!(
			obj_get(&err_msg, "error").is_some(),
			"unnegotiated cbor connection must refuse cbor.2 tags: {err_msg:?}"
		);
	});
}

#[test]
fn http_negotiates_cbor2_and_enforces_content_type_rules() {
	with_big_stack(|| async {
		let server = TestServer::start().await;

		// 1. Reference HTTP client starts with `Content-Type: application/cbor` +
		// `Accept: application/cbor; v=2`, observes `Content-Type: application/cbor; v=2`
		// in the first response, and then sends `Content-Type: application/cbor; v=2`
		// with width-preserving tags on subsequent requests.
		let mut client = CborHttpClient::new(server.address, true);
		assert!(!client.negotiated_v2);

		let ver = client.rpc("v1", "version", vec![]).await;
		assert!(obj_get(&ver, "error").is_none(), "version call failed: {ver:?}");
		assert!(client.negotiated_v2, "first response should negotiate application/cbor; v=2");
		assert_eq!(client.last_response_content_type, surrealdb_rpc::format::CBOR2);

		for (i, (num, expected_kind)) in sample_sized_numbers().into_iter().enumerate() {
			let mut vars = Object::default();
			vars.insert("x".to_owned(), Value::Number(num.clone()));
			let res = client
				.rpc(
					&format!("hq_{i}"),
					"query",
					vec![
						Value::String("RETURN [$x, type::of($x)];".to_owned()),
						Value::Object(vars),
					],
				)
				.await;
			assert_eq!(client.last_response_content_type, surrealdb_rpc::format::CBOR2);
			let result = first_query_result(&res);
			assert_eq!(
				result,
				&Value::Array(
					vec![Value::Number(num.clone()), Value::String(expected_kind.to_owned())]
						.into()
				),
				"HTTP round-trip failed for {num:?} ({expected_kind})"
			);
		}

		// 2. Unnegotiated HTTP client (`Accept: application/cbor`) receives
		// `Content-Type: application/cbor` and widened numbers.
		let mut legacy_client = CborHttpClient::new(server.address, false);
		let res = legacy_client
			.rpc(
				"legacy_q",
				"query",
				vec![Value::String("RETURN [5i8, 5u64, 1.5f16, 2.25f32];".to_owned())],
			)
			.await;
		assert!(!legacy_client.negotiated_v2);
		assert_eq!(legacy_client.last_response_content_type, surrealdb_rpc::format::CBOR);
		assert_eq!(
			first_query_result(&res),
			&Value::Array(
				vec![
					Value::Number(Number::Int(5)),
					Value::Number(Number::Int(5)),
					Value::Number(Number::Float(1.5)),
					Value::Number(Number::Float(2.25)),
				]
				.into()
			)
		);

		// 3. Sending a `cbor.2`-tagged body with `Content-Type: application/cbor` (no `; v=2`)
		// is refused even if `Accept: application/cbor; v=2` is set.
		let mut vars = BTreeMap::new();
		vars.insert("x".to_owned(), Value::Number(Number::U8(9)));
		let req_val = make_rpc_request(
			"bad_http",
			"query",
			vec![Value::String("RETURN $x;".to_owned()), Value::Object(vars.into())],
		);
		let raw_v2_body = encode_v2(req_val).expect("encode_v2");
		let http_res = reqwest::Client::new()
			.post(format!("http://{}/rpc", server.address))
			.basic_auth(USER, Some(PASS))
			.header("surreal-ns", "test")
			.header("surreal-db", "test")
			.header(reqwest::header::ACCEPT, surrealdb_rpc::format::CBOR2)
			.header(reqwest::header::CONTENT_TYPE, surrealdb_rpc::format::CBOR)
			.body(raw_v2_body)
			.send()
			.await
			.expect("http post");
		assert_ne!(
			http_res.status(),
			reqwest::StatusCode::OK,
			"server must refuse cbor.2 tags when Content-Type is frozen application/cbor"
		);

		// 4. Sending `Content-Type: application/cbor; v=2` with `Accept: application/cbor`
		// is rejected with 415 Unsupported Media Type.
		let mismatch_body =
			encode_v2(make_rpc_request("mismatch", "version", vec![])).expect("encode_v2");
		let mismatch_res = reqwest::Client::new()
			.post(format!("http://{}/rpc", server.address))
			.basic_auth(USER, Some(PASS))
			.header("surreal-ns", "test")
			.header("surreal-db", "test")
			.header(reqwest::header::ACCEPT, surrealdb_rpc::format::CBOR)
			.header(reqwest::header::CONTENT_TYPE, surrealdb_rpc::format::CBOR2)
			.body(mismatch_body)
			.send()
			.await
			.expect("http post");
		assert_eq!(
			mismatch_res.status(),
			reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE,
			"Content-Type: application/cbor; v=2 with Accept: application/cbor must return 415"
		);
	});
}

#[test]
fn cbor_reference_client_against_cached_released_binaries() {
	// When released server binaries are cached under `language-tests/.binaries/`,
	// verify the reference client (`cbor.2, cbor` on WS and `Accept: application/cbor; v=2`
	// on HTTP) falls back cleanly to frozen `cbor` against every released 3.x server.
	let bin_dir =
		std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../language-tests/.binaries");
	let Ok(entries) = std::fs::read_dir(&bin_dir) else {
		return;
	};
	let mut binaries: Vec<std::path::PathBuf> = entries
		.filter_map(|e| e.ok().map(|e| e.path()))
		.filter(|p| {
			p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with("surreal-v3."))
		})
		.collect();
	binaries.sort();
	if binaries.is_empty() {
		return;
	}

	with_big_stack(move || async move {
		for bin in binaries {
			let port_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral port");
			let addr = port_listener.local_addr().expect("local_addr");
			drop(port_listener);

			let mut child = std::process::Command::new(&bin)
				.args([
					"start",
					"--bind",
					&addr.to_string(),
					"--user",
					USER,
					"--pass",
					PASS,
					"memory",
				])
				.stdout(std::process::Stdio::null())
				.stderr(std::process::Stdio::null())
				.spawn()
				.expect("spawn released binary");

			// Wait for the server to accept TCP connections.
			let mut ready = false;
			for _ in 0..100 {
				if TcpStream::connect(addr).await.is_ok() {
					ready = true;
					break;
				}
				tokio::time::sleep(Duration::from_millis(50)).await;
			}
			assert!(ready, "released binary {bin:?} did not start listening on {addr}");

			// WebSocket: offering `cbor.2, cbor` selects `cbor`, and bound sized values widen
			// cleanly because `negotiated_v2` is false.
			let mut ws = CborWsClient::connect(addr, "cbor.2, cbor").await;
			assert_eq!(ws.selected_protocol, "cbor", "released binary {bin:?}");
			assert!(!ws.negotiated_v2);
			let mut vars = Object::default();
			vars.insert("x".to_owned(), Value::Number(Number::U8(42)));
			let ws_res = ws
				.rpc(
					"rel_ws",
					"query",
					vec![Value::String("RETURN $x;".to_owned()), Value::Object(vars)],
				)
				.await;
			assert_eq!(first_query_result(&ws_res), &Value::Number(Number::Int(42)));

			// HTTP: sending `Accept: application/cbor; v=2` answers `application/cbor` and
			// decodes cleanly.
			let mut http = CborHttpClient::new(addr, true);
			let mut vars = Object::default();
			vars.insert("x".to_owned(), Value::Number(Number::I16(-12)));
			let http_res = http
				.rpc(
					"rel_http",
					"query",
					vec![Value::String("RETURN $x;".to_owned()), Value::Object(vars)],
				)
				.await;
			assert!(
				!http.negotiated_v2,
				"released binary {bin:?}: {}",
				Value::Number(Number::I16(-12)).to_sql()
			);
			assert_eq!(first_query_result(&http_res), &Value::Number(Number::Int(-12)));

			let _ = child.kill();
			let _ = child.wait();
		}
	});
}
