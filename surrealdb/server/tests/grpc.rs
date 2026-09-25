//! End-to-end tests for the gRPC transport.
//!
//! Each test serves the real community router -- the same tree, middleware and
//! `axum_server` setup the `surreal start` path builds -- on an ephemeral
//! port, and drives it with the SDK's own gRPC engine. That combination is
//! what is being tested: the transport is only correct if the client the
//! server ships with can actually talk to it.
//!
//! Serving over plaintext HTTP/2 is itself part of what these cover. Nothing
//! configures a second listener or an HTTP/2-only mode: the server negotiates
//! the protocol from the connection preface, which is what lets `grpc://` and
//! `http://` share a port.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use surrealdb::Surreal;
use surrealdb::engine::remote::grpc::{Client, Grpc};
use surrealdb::opt::auth::Root;
use surrealdb_core::CommunityComposer;
use surrealdb_core::kvs::Datastore;
use surrealdb_protocol::proto::rpc::v1::surreal_db_service_client::SurrealDbServiceClient;
use surrealdb_protocol::proto::rpc::v1::{
	AccessMethod, AttachSessionRequest, NullableString, RequestContext, SigninRequest, UseRequest,
	UserCredentials, access_method, nullable_string,
};
use surrealdb_rpc::capabilities::Capabilities;
use surrealdb_server::ntw::{RouterOptions, SurrealRouter};
use surrealdb_types::{QueryError, Value};
use tokio_util::sync::CancellationToken;

const USER: &str = "root";
const PASS: &str = "root";

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

type EncodingLog = Arc<std::sync::Mutex<Vec<(String, String)>>>;

impl TestServer {
	async fn start() -> Self {
		Self::start_with(None).await
	}

	async fn start_with(record: Option<EncodingLog>) -> Self {
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
		let mut app = router.into_router();
		if let Some(seen) = record {
			app = app.layer(axum::middleware::from_fn(
				move |request: axum::extract::Request, next: axum::middleware::Next| {
					let seen = Arc::clone(&seen);
					async move {
						let path = request.uri().path().to_string();
						let encoding = request
							.headers()
							.get("grpc-encoding")
							.and_then(|v| v.to_str().ok())
							.unwrap_or("identity")
							.to_string();
						seen.lock().expect("encoding log poisoned").push((path, encoding));
						next.run(request).await
					}
				},
			));
		}
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

	/// Starts a server that records the `grpc-encoding` of every request it
	/// receives, so a test can assert what was actually on the wire rather than
	/// what either peer was configured to prefer.
	async fn start_recording() -> (Self, Arc<std::sync::Mutex<Vec<(String, String)>>>) {
		let seen: Arc<std::sync::Mutex<Vec<(String, String)>>> = Arc::default();
		let server = Self::start_with(Some(Arc::clone(&seen))).await;
		(server, seen)
	}

	/// Connects the SDK's gRPC engine to this server.
	async fn connect(&self) -> Surreal<Client> {
		Surreal::new::<Grpc>(self.address.to_string()).await.expect("connect")
	}

	/// Connects, authenticates as root, and selects a namespace and database.
	async fn connect_as_root(&self) -> Surreal<Client> {
		self.connect_to_database("test").await
	}

	async fn connect_to_database(&self, database: &str) -> Surreal<Client> {
		let db = self.connect().await;
		db.signin(Root {
			username: USER.to_string(),
			password: PASS.to_string(),
		})
		.await
		.expect("signin");
		db.use_ns("test").use_db(database).await.expect("use");
		db
	}
}

/// The whole session lifecycle over one connection: authentication takes
/// effect on the session, and the namespace selection persists across
/// requests -- which is the thing a connectionless transport most easily gets
/// wrong.
#[tokio::test]
async fn a_session_carries_auth_and_namespace_across_requests() {
	let server = TestServer::start().await;
	let db = server.connect().await;

	// Before signing in, the session is anonymous and cannot reach root.
	db.query("INFO FOR ROOT").await.expect("send").check().expect_err("anonymous root access");

	db.signin(Root {
		username: USER.to_string(),
		password: PASS.to_string(),
	})
	.await
	.expect("signin");
	// The session is authenticated from here on, on a later request.
	db.query("INFO FOR ROOT").await.expect("send").check().expect("root access after signin");

	let (namespace, database) = db.use_ns("test").use_db("test").await.expect("use");
	assert_eq!(namespace.as_deref(), Some("test"));
	assert_eq!(database.as_deref(), Some("test"));

	// And the selection is still in effect on a request that does not repeat it.
	let mut response = db.query("RETURN $session.ns").await.expect("send");
	let selected: Option<String> = response.take(0).expect("selection");
	assert_eq!(selected.as_deref(), Some("test"));
}

/// A result bigger than one gRPC message still reaches the caller.
///
/// A client decodes 4 MiB per message by default, so a statement whose records
/// are sent as a single batch is refused outright rather than returning its
/// rows. The server splits them, which is what the protocol's batch index and
/// `Batched`/`BatchedFinal` kinds are for.
#[tokio::test]
async fn a_result_larger_than_one_message_still_arrives() {
	let server = TestServer::start().await;
	let db = server.connect_as_root().await;

	// Comfortably past the 4 MiB a gRPC client decodes per message by default,
	// so this only arrives if the server splits the statement across frames.
	const RECORDS: usize = 1200;
	const PADDING: usize = 4096;

	db.query("FOR $i IN array::range(0, $count) { CREATE type::record('big', $i) SET pad = $pad }")
		.bind(("count", RECORDS as i64))
		.bind(("pad", "x".repeat(PADDING)))
		.await
		.expect("send")
		.check()
		.expect("create");

	let all: Vec<Value> = db.select("big").await.expect("a result spanning several frames");
	assert_eq!(all.len(), RECORDS, "every record must survive the split");
	const _: () = assert!(
		RECORDS * PADDING > 4 * 1024 * 1024,
		"the fixture has to exceed one message for this to prove anything"
	);
}

/// Records wide enough that a full batch of them exceeds one message are split
/// on bytes, not just on record count.
///
/// The SDK accepts compressed responses on every connection, and tonic weighs
/// its send limit against the *compressed* payload while the receiving decoder
/// applies its own limit to the decompressed one. A frame bounded only by
/// record count can therefore be several times the limit, compress to almost
/// nothing on the way out, and fail inside the client's decompressor -- where
/// the caller sees a corrupt stream rather than a size. The padding here is a
/// repeated byte precisely so that it compresses away: a fixture that did not
/// compress would be refused on the way out and never reach the case this
/// covers.
#[tokio::test]
async fn a_batch_of_wide_records_arrives_over_a_compressed_stream() {
	let server = TestServer::start().await;
	let db = server.connect_as_root().await;

	// 48 records ramp to batches of 16 then 32; at this width the second batch
	// is past the 4 MiB default, so it only arrives if bytes bound the frame.
	const RECORDS: usize = 48;
	const PADDING: usize = 140 * 1024;
	const _: () = assert!(
		32 * PADDING > 4 * 1024 * 1024,
		"a full batch has to exceed one message for this to prove anything"
	);

	db.query(
		"FOR $i IN array::range(0, $count) { CREATE type::record('wide', $i) SET pad = $pad }",
	)
	.bind(("count", RECORDS as i64))
	.bind(("pad", "x".repeat(PADDING)))
	.await
	.expect("send")
	.check()
	.expect("create");

	let all: Vec<Value> = db.select("wide").await.expect("a result split on bytes");
	assert_eq!(all.len(), RECORDS, "every record must survive the split");
}

/// A frame of data that does not compress still fits.
///
/// tonic weighs the *compressed* payload against its send limit and compresses
/// unconditionally once a client has negotiated an encoding -- it never checks
/// whether compressing helped. Incompressible data therefore grows on the way
/// out, so a frame filled to exactly the advertised limit would be refused by
/// tonic after this server had already accepted it, and the caller would see
/// `out_of_range` on a response that was within the size it was promised. The
/// budget holds back a proportional reserve for that expansion; this fills a
/// frame against the budget with bytes chosen so neither codec can shrink them.
#[tokio::test]
async fn incompressible_records_still_fit_their_frame() {
	let server = TestServer::start().await;
	let db = server.connect_as_root().await;

	// Narrow enough that a frame packs tight against the budget instead of
	// stopping a whole record short of it, and numerous enough to reach a frame
	// the byte budget closes: the batches ramp 16, 32, 64, 128 before one is
	// bounded by bytes rather than by count. A fixture that leaves a record's
	// width of slack never reaches the headroom this is about.
	const PADDING: usize = 16 * 1024;
	const RECORDS: usize = 16 + 32 + 64 + 128 + 256;

	// Every record gets its own bytes. One buffer stored in all of them would
	// be incompressible per record and enormously compressible per frame: both
	// codecs reach back across the records around it -- deflate over 32 KiB and
	// zstd far further -- and a frame of one repeated 16 KiB block encodes down
	// to a few tens of kibibytes, which exercises no expansion at all.
	let mut state = 0x243F6A8885A308D3u64;
	let mut next_pad = || -> surrealdb_types::Bytes {
		surrealdb_types::Bytes::from(
			(0..PADDING)
				.map(|_| {
					state ^= state << 13;
					state ^= state >> 7;
					state ^= state << 17;
					(state >> 33) as u8
				})
				.collect::<Vec<u8>>(),
		)
	};
	let pads: Vec<surrealdb_types::Bytes> = (0..RECORDS).map(|_| next_pad()).collect();

	// Written a chunk at a time because the whole fixture is larger than one
	// message: the limit this test is about bounds requests too.
	for chunk in pads.chunks(64) {
		db.query("FOR $pad IN $pads { CREATE dense SET pad = $pad }")
			.bind(("pads", chunk.to_vec()))
			.await
			.expect("send")
			.check()
			.expect("create");
	}

	let all: Vec<Value> = db.select("dense").await.expect("a result tonic will still emit");
	assert_eq!(all.len(), RECORDS, "every record must survive compression");

	// Guards what the fixture is for: were these to coincide, the frames would
	// compress away and the test would pass without touching the headroom.
	let distinct: std::collections::HashSet<Vec<u8>> = all
		.iter()
		.map(|record| match record {
			Value::Object(fields) => match fields.get("pad") {
				Some(Value::Bytes(bytes)) => bytes.clone().into_inner().to_vec(),
				other => panic!("expected bytes, got {other:?}"),
			},
			other => panic!("expected a record, got {other:?}"),
		})
		.collect();
	assert_eq!(distinct.len(), RECORDS, "each record must carry its own bytes");
}

/// `run` reaches the database as its own RPC, so the function name travels as
/// data rather than being compiled into a query the caller never wrote.
#[tokio::test]
async fn a_function_runs_through_its_own_rpc() {
	let server = TestServer::start().await;
	let db = server.connect_as_root().await;

	db.query("DEFINE FUNCTION fn::greet($name: string) { RETURN 'hello ' + $name }")
		.await
		.expect("send")
		.check()
		.expect("define");

	let greeting: String = db.run("fn::greet").args("world").await.expect("run");
	assert_eq!(greeting, "hello world");

	// A built-in function resolves the same way, and its argument stays an
	// argument rather than becoming syntax.
	let joined: String = db.run("string::join").args(("-", "a", "b")).await.expect("run built-in");
	assert_eq!(joined, "a-b");
}

/// `kill` likewise: ending a live query is nameable on the wire, so a failure
/// reaches the caller as one rather than hiding in a statement result.
#[tokio::test]
async fn killing_a_live_query_ends_its_stream() {
	use futures::StreamExt;

	let server = TestServer::start().await;
	let db = server.connect_as_root().await;
	db.query("DEFINE TABLE watched SCHEMALESS").await.expect("send").check().expect("define");

	let mut stream = db.select("watched").live().await.expect("live");
	db.query("CREATE watched:one SET n = 1").await.expect("send").check().expect("create");
	let first: surrealdb::Notification<Value> =
		tokio::time::timeout(Duration::from_secs(10), stream.next())
			.await
			.expect("a notification within ten seconds")
			.expect("a notification")
			.expect("a successful notification");
	assert_eq!(first.action, surrealdb_types::Action::Create);

	drop(stream);
	// The live query outlives its subscriber, so an explicit kill is what ends
	// it; that it returns at all is the RPC round-tripping.
	db.query("SELECT * FROM watched").await.expect("send").check().expect("still usable");
}

/// The CRUD surface, which is where most of the wire format gets exercised:
/// every builder method compiles to a `Query`, so this covers statement
/// framing, value round-tripping, and the single-versus-list distinction the
/// batch `kind` carries.
#[tokio::test]
async fn records_round_trip_through_the_query_stream() {
	let server = TestServer::start().await;
	let db = server.connect_as_root().await;

	db.query("CREATE person:test SET name = $name, tags = $tags")
		.bind(("name", "Tobie"))
		.bind(("tags", vec!["a", "b"]))
		.await
		.expect("send")
		.check()
		.expect("create");

	let mut response = db.query("SELECT VALUE name FROM ONLY person:test").await.expect("send");
	let name: Option<String> = response.take(0).expect("name");
	assert_eq!(name.as_deref(), Some("Tobie"));

	// A list result and a single result take different paths through the batch
	// frame; both must arrive as what the caller asked for.
	let all: Vec<Value> = db.select("person").await.expect("select");
	assert_eq!(all.len(), 1);
	let one: Option<Value> = db.select(("person", "test")).await.expect("select one");
	assert!(one.is_some());

	let none: Option<Value> = db.select(("person", "absent")).await.expect("select missing");
	assert!(none.is_none(), "a missing record is not an error");
}

/// A statement's own failure travels in its batch, so the statements around it
/// still report their results -- the property the per-statement error frame
/// exists for.
#[tokio::test]
async fn a_failed_statement_does_not_fail_the_others() {
	let server = TestServer::start().await;
	let db = server.connect_as_root().await;

	let mut response = db.query("RETURN 1; THROW 'nope'; RETURN 42").await.expect("send");
	let first: Option<i64> = response.take(0).expect("the statement before the failure");
	assert_eq!(first, Some(1));
	assert!(response.take::<Option<i64>>(1).is_err(), "the THROW is that statement's error");
	let last: Option<i64> = response.take(2).expect("the statement after the failure");
	assert_eq!(last, Some(42));
}

/// A statement's failure reaches the caller as the typed reason the taxonomy
/// names, not merely as a message. The protocol states the message is not part
/// of the contract and will be reworded, so a client that only receives one has
/// nothing durable to branch on -- which is exactly what a retry loop around a
/// transaction conflict depends on.
#[tokio::test]
async fn a_failed_statement_carries_its_typed_reason() {
	let server = TestServer::start().await;
	let db = server.connect_as_root().await;

	// A failure inside a transaction block abandons the statements around it,
	// and the executor reports each of those as `NotExecuted` -- the same
	// per-statement error frame a conflicting `COMMIT` travels in.
	let mut response = db
		.query("BEGIN; CREATE person:one; THROW 'nope'; CREATE person:two; COMMIT")
		.await
		.expect("send");
	let failures = response.take_errors();
	let reasons: Vec<Option<&QueryError>> =
		failures.values().map(surrealdb::Error::query_details).collect();
	assert!(
		reasons.contains(&Some(&QueryError::NotExecuted)),
		"an abandoned statement should name its reason, got {reasons:?}"
	);
}

/// Variables bound by the session outlive the request that set them, and are
/// visible to later queries -- the other half of per-session state.
#[tokio::test]
async fn session_variables_persist_and_can_be_removed() {
	let server = TestServer::start().await;
	let db = server.connect_as_root().await;

	db.set("greeting", "hello").await.expect("set");
	let mut response = db.query("RETURN $greeting").await.expect("send");
	let greeting: Option<String> = response.take(0).expect("variable");
	assert_eq!(greeting.as_deref(), Some("hello"));

	db.unset("greeting").await.expect("unset");
	let mut response = db.query("RETURN $greeting").await.expect("send");
	let greeting: Option<String> = response.take(0).expect("variable");
	assert_eq!(greeting, None);
}

/// An explicit transaction spans several requests, and its writes are only
/// visible once it commits -- so the transaction id in the request context is
/// genuinely selecting the right transaction rather than being ignored.
#[tokio::test]
async fn a_transaction_spans_requests_and_isolates_its_writes() {
	let server = TestServer::start().await;
	let db = server.connect_as_root().await;
	let other = server.connect_as_root().await;
	// Define the table up front so that "no rows" and "no table" are
	// distinguishable: selecting from a table that does not exist is an error,
	// not an empty result.
	db.query("DEFINE TABLE account SCHEMALESS").await.expect("send").check().expect("define");

	let transaction = db.begin().await.expect("begin");
	transaction
		.query("CREATE account:one SET balance = 10")
		.await
		.expect("send")
		.check()
		.expect("create");

	// The write is not visible outside the transaction until it commits.
	let outside: Vec<Value> = other.select("account").await.expect("select");
	assert!(outside.is_empty(), "an uncommitted write must not be visible");

	transaction.commit().await.expect("commit");
	let outside: Vec<Value> = other.select("account").await.expect("select");
	assert_eq!(outside.len(), 1);
}

/// A cancelled transaction leaves nothing behind.
#[tokio::test]
async fn a_cancelled_transaction_discards_its_writes() {
	let server = TestServer::start().await;
	let db = server.connect_as_root().await;
	db.query("DEFINE TABLE account SCHEMALESS").await.expect("send").check().expect("define");

	let transaction = db.begin().await.expect("begin");
	transaction
		.query("CREATE account:two SET balance = 20")
		.await
		.expect("send")
		.check()
		.expect("create");
	// Cancelling hands the client back, so the check runs on the same session.
	let db = transaction.cancel().await.expect("cancel");

	let remaining: Vec<Value> = db.select("account").await.expect("select");
	assert!(remaining.is_empty());
}

/// A live query registered by an ordinary statement, subscribed to over
/// `Subscribe`. This is the full notification path: the executor's
/// registration hook, the subscription registry, and the dispatcher routing to
/// the gRPC transport rather than the WebSocket one.
#[tokio::test]
async fn live_queries_stream_notifications() {
	use futures::StreamExt;

	let server = TestServer::start().await;
	let db = server.connect_as_root().await;
	db.query("DEFINE TABLE thing SCHEMALESS").await.expect("send").check().expect("define");

	let mut stream = db.select("thing").live().await.expect("live");

	db.query("CREATE thing:one SET value = 1").await.expect("send").check().expect("create");
	let notification: surrealdb::Notification<Value> =
		tokio::time::timeout(Duration::from_secs(10), stream.next())
			.await
			.expect("a notification within ten seconds")
			.expect("a notification")
			.expect("a successful notification");
	assert_eq!(notification.action, surrealdb_types::Action::Create);

	// An update on the same table reaches the same subscription.
	db.query("UPDATE thing:one SET value = 2").await.expect("send").check().expect("update");
	let notification: surrealdb::Notification<Value> =
		tokio::time::timeout(Duration::from_secs(10), stream.next())
			.await
			.expect("a notification within ten seconds")
			.expect("a notification")
			.expect("a successful notification");
	assert_eq!(notification.action, surrealdb_types::Action::Update);
}

/// Export and import round-trip a database through the streaming RPCs,
/// including the trailer that marks the byte stream complete -- the client
/// rejects a stream that arrives without one.
#[tokio::test]
async fn a_database_round_trips_through_export_and_import() {
	let server = TestServer::start().await;
	let db = server.connect_as_root().await;
	db.query("CREATE person:exported SET name = 'Tobie'")
		.await
		.expect("send")
		.check()
		.expect("create");

	let file = tempfile::NamedTempFile::new().expect("temp file");
	db.export(file.path()).await.expect("export");
	let exported = std::fs::read_to_string(file.path()).expect("read the export");
	assert!(exported.contains("person:exported"), "the export should contain the record");

	// Import it into a second database and check the record arrives.
	let restored = server.connect_to_database("restored").await;
	restored.import(file.path()).await.expect("import");
	let people: Vec<Value> = restored.select("person").await.expect("select");
	assert_eq!(people.len(), 1);
}

/// An import whose statements fail is reported as a failure, and says that the
/// statements before the failure have been applied.
///
/// The import is not transactional -- each statement commits on its own and
/// execution carries on past one that fails -- so a caller that reads only the
/// outcome has to be told both that it failed and that the database now holds
/// part of the dump. Reporting success here would leave a half-restored
/// database behind a call that said it worked.
#[tokio::test]
async fn an_import_reports_the_statements_it_could_not_apply() {
	let server = TestServer::start().await;
	let restored = server.connect_to_database("partial").await;

	let file = tempfile::NamedTempFile::new().expect("temp file");
	std::fs::write(file.path(), "OPTION IMPORT;\nCREATE applied:1;\nBREAK;\n").expect("write");

	let error = restored.import(file.path()).await.expect_err("the import must not report success");
	let message = error.to_string();
	assert!(
		message.contains("break or continue statement found outside of loop"),
		"the failing statement is not named: {message}"
	);
	assert!(
		message.contains("have been applied"),
		"the report does not say the dump is partially applied: {message}"
	);

	// The statement before the failure really did apply, which is what the
	// message promises and the reason the call cannot report plain success.
	let applied: Vec<Value> = restored.select("applied").await.expect("select");
	assert_eq!(applied.len(), 1, "the statement before the failure should have been applied");
}

/// A complete import still reports success, so the failure above is read from
/// the statements rather than from the call having run at all.
#[tokio::test]
async fn a_complete_import_reports_success() {
	let server = TestServer::start().await;
	let restored = server.connect_to_database("complete").await;

	let file = tempfile::NamedTempFile::new().expect("temp file");
	std::fs::write(file.path(), "OPTION IMPORT;\nCREATE applied:1;\n").expect("write");

	restored.import(file.path()).await.expect("a complete import must report success");
	let applied: Vec<Value> = restored.select("applied").await.expect("select");
	assert_eq!(applied.len(), 1);
}

/// The version reported over gRPC comes from the handshake rather than a round
/// trip, so it has to be the real one.
#[tokio::test]
async fn the_server_reports_its_version() {
	let server = TestServer::start().await;
	let db = server.connect().await;
	let version = db.version().await.expect("version");
	assert!(version.major >= 1, "expected a real version, got {version}");
}

/// Health is answered without a session, which is what a load balancer probe
/// needs.
#[tokio::test]
async fn health_is_answered_without_a_session() {
	let server = TestServer::start().await;
	let db = server.connect().await;
	db.health().await.expect("health");
}

/// Nothing about the server is gRPC-only: the same listener still serves the
/// HTTP routes, which is the whole point of mounting the service on the shared
/// router.
#[tokio::test]
async fn the_same_port_still_serves_http() {
	let server = TestServer::start().await;
	let response = reqwest::Client::new()
		.get(format!("http://{}/health", server.address))
		.send()
		.await
		.expect("request");
	assert!(response.status().is_success());
}

/// Variables bound to a single query are not confused with the session's own.
#[tokio::test]
async fn query_variables_do_not_leak_into_the_session() {
	let server = TestServer::start().await;
	let db = server.connect_as_root().await;

	let mut response = db.query("RETURN $scoped").bind(("scoped", "value")).await.expect("send");
	let scoped: Option<String> = response.take(0).expect("variable");
	assert_eq!(scoped.as_deref(), Some("value"));

	// The next query does not see it.
	let mut response = db.query("RETURN $scoped").await.expect("send");
	let scoped: Option<String> = response.take(0).expect("variable");
	assert_eq!(scoped, None);
}

/// Two connections get independent sessions: one signing in must not
/// authenticate the other.
#[tokio::test]
async fn sessions_are_independent_across_connections() {
	let server = TestServer::start().await;
	let authenticated = server.connect_as_root().await;
	let anonymous = server.connect().await;

	authenticated.query("INFO FOR ROOT").await.expect("send").check().expect("root access");
	anonymous
		.query("INFO FOR ROOT")
		.await
		.expect("send")
		.check()
		.expect_err("a separate connection must not inherit the other's authentication");
}

/// Results reach the client while the query is still running.
///
/// This is the property the streaming path exists for, and the one no other
/// test here can see: the SDK's engine buffers every frame before returning, so
/// a buffered server and a streaming one are indistinguishable through it. The
/// raw client observes the frames themselves.
///
/// The assertion is a generous ratio rather than a latency bound, so it pins
/// the behaviour without depending on how fast the machine is. A server that
/// executed to completion before answering would sit at essentially 100%.
#[tokio::test]
async fn rows_arrive_before_the_query_finishes() {
	use std::time::Instant;

	use surrealdb_protocol::proto::rpc::v1::surreal_db_service_client::SurrealDbServiceClient;
	use surrealdb_protocol::proto::rpc::v1::{
		self as rpc, AccessMethod, AttachSessionRequest, NullableString, RequestContext,
		SigninRequest, UseRequest, UserCredentials, access_method, nullable_string,
	};

	let server = TestServer::start().await;

	// Enough rows that producing them all takes meaningfully longer than
	// producing the first batch.
	const RECORDS: i64 = 10_000;
	let db = server.connect_as_root().await;
	db.query("FOR $i IN array::range(0, $count) { CREATE type::record('wide', $i) SET n = $i }")
		.bind(("count", RECORDS))
		.await
		.expect("send")
		.check()
		.expect("seed");

	let mut client = SurrealDbServiceClient::connect(format!("http://{}", server.address))
		.await
		.expect("a raw gRPC client");

	let session = client
		.attach_session(AttachSessionRequest::default())
		.await
		.expect("attach")
		.into_inner()
		.session
		.expect("the server mints a session id");
	let context = Some(RequestContext {
		session: Some(session),
		..Default::default()
	});
	client
		.signin(SigninRequest {
			context: context.clone(),
			access_method: Some(AccessMethod {
				method: Some(access_method::Method::User(UserCredentials {
					username: USER.to_string(),
					password: PASS.to_string(),
					..Default::default()
				})),
			}),
		})
		.await
		.expect("signin");
	client
		.r#use(UseRequest {
			context: context.clone(),
			namespace: Some(NullableString {
				value: Some(nullable_string::Value::Some("test".to_string())),
			}),
			database: Some(NullableString {
				value: Some(nullable_string::Value::Some("test".to_string())),
			}),
		})
		.await
		.expect("use");

	let mut stream = client
		.query(rpc::QueryRequest {
			context,
			query: "SELECT * FROM wide".to_string(),
			variables: None,
			accepted_encodings: Vec::new(),
			max_batch_records: 0,
		})
		.await
		.expect("query")
		.into_inner();

	// Time from the `Begin` frame, not from the request. `Begin` is sent on
	// accepting the query and before executing it, so it separates what this
	// test is about -- how soon results follow their own production -- from
	// connecting a channel and resolving a session, which happen once and would
	// otherwise dominate a fixture small enough to run in a test suite.
	let mut executing = None;
	let mut first_rows: Option<Duration> = None;
	let mut records = 0usize;
	while let Some(response) = stream.message().await.expect("a frame") {
		match response.frame {
			Some(rpc::query_response::Frame::Begin(_)) => executing = Some(Instant::now()),
			Some(rpc::query_response::Frame::Batch(batch)) => {
				if let Some(rpc::query_batch_frame::Payload::Values(values)) = batch.payload
					&& !values.values.is_empty()
				{
					let executing = executing.expect("`Begin` precedes every batch");
					first_rows.get_or_insert_with(|| executing.elapsed());
					records += values.values.len();
				}
			}
			_ => {}
		}
	}
	let executing = executing.expect("the query began");
	let total = executing.elapsed();
	let first = first_rows.expect("some rows arrived");

	assert_eq!(records, RECORDS as usize, "every record still arrives exactly once");
	assert!(
		first.as_secs_f64() < total.as_secs_f64() * 0.25,
		"the first rows should arrive early in the query, not at the end: \
		 first after {first:?} of {total:?}"
	);
}

/// The SDK's streaming API hands rows to the caller as they arrive, and reports
/// each statement's outcome separately from its rows.
#[tokio::test]
async fn the_sdk_streams_rows_as_they_arrive() {
	use futures::StreamExt;
	use surrealdb::method::StreamItem;

	let server = TestServer::start().await;
	let db = server.connect_as_root().await;

	const RECORDS: i64 = 500;
	db.query("FOR $i IN array::range(0, $count) { CREATE type::record('row', $i) SET n = $i }")
		.bind(("count", RECORDS))
		.await
		.expect("send")
		.check()
		.expect("seed");

	let mut items =
		db.query("SELECT * FROM row").query("RETURN 'done'").stream_items().expect("a stream");

	let mut rows = 0usize;
	let mut ends = Vec::new();
	while let Some(item) = items.next().await {
		match item.expect("no stream-level failure") {
			StreamItem::Row {
				statement,
				..
			} => {
				// Rows arrive before their statement is confirmed, which is the
				// whole point: nothing has told us yet that they are final.
				assert!(
					!ends.contains(&statement),
					"statement {statement} produced a row after it finished"
				);
				rows += 1;
			}
			StreamItem::StatementEnd {
				statement,
				result,
				..
			} => {
				result.expect("both statements succeed");
				ends.push(statement);
			}
		}
	}

	assert_eq!(rows, RECORDS as usize + 1, "every row, plus the second statement's one value");
	assert_eq!(ends, vec![0, 1], "each statement ends exactly once, in order");
}

/// A statement that fails reports it on its own `StatementEnd`, leaving the
/// other statements' results usable -- the streaming equivalent of a per-result
/// error in the buffered API.
#[tokio::test]
async fn a_failed_statement_ends_with_its_error() {
	use futures::StreamExt;
	use surrealdb::method::StreamItem;

	let server = TestServer::start().await;
	let db = server.connect_as_root().await;

	let mut items = db
		.query("RETURN 1")
		.query("THROW 'nope'")
		.query("RETURN 3")
		.stream_items()
		.expect("a stream");

	let mut outcomes = Vec::new();
	while let Some(item) = items.next().await {
		if let StreamItem::StatementEnd {
			statement,
			result,
			..
		} = item.expect("no stream-level failure")
		{
			outcomes.push((statement, result.is_ok()));
		}
	}
	assert_eq!(
		outcomes,
		vec![(0, true), (1, false), (2, true)],
		"only the throwing statement fails, and the ones around it still finish"
	);
}

/// Signs a raw client in and returns the context every later call carries.
async fn authenticate(
	client: &mut SurrealDbServiceClient<tonic::transport::Channel>,
) -> Option<RequestContext> {
	let session = client
		.attach_session(AttachSessionRequest::default())
		.await
		.expect("attach")
		.into_inner()
		.session
		.expect("the server mints a session id");
	let context = Some(RequestContext {
		session: Some(session),
		..Default::default()
	});
	client
		.signin(SigninRequest {
			context: context.clone(),
			access_method: Some(AccessMethod {
				method: Some(access_method::Method::User(UserCredentials {
					username: USER.to_string(),
					password: PASS.to_string(),
					..Default::default()
				})),
			}),
		})
		.await
		.expect("signin");
	client
		.r#use(UseRequest {
			context: context.clone(),
			namespace: Some(NullableString {
				value: Some(nullable_string::Value::Some("test".to_string())),
			}),
			database: Some(NullableString {
				value: Some(nullable_string::Value::Some("test".to_string())),
			}),
		})
		.await
		.expect("use");
	context
}

/// Compression is negotiated per request, not assumed.
///
/// An export is the payload that makes this matter -- it is the one response
/// large enough for the encoding to change what crosses the network -- and the
/// HTTP transport has always gzipped it, so shipping it uncompressed over gRPC
/// was a regression against the transport it stands beside.
///
/// What has to hold is that enabling it did not make an encoding mandatory: a
/// client advertising nothing must still be served. Both halves are asserted
/// from the response's own `grpc-encoding`, which is what the server actually
/// put on the wire rather than what either side was configured to prefer.
#[tokio::test]
async fn export_compression_is_negotiated_per_client() {
	use surrealdb_protocol::proto::rpc::v1 as rpc;
	use tonic::codec::CompressionEncoding;

	let server = TestServer::start().await;
	let db = server.connect_as_root().await;
	db.query("CREATE person:1 SET name = 'a'").await.expect("send").check().expect("seed");

	let endpoint = format!("http://{}", server.address);

	// A client that advertises zstd is served zstd.
	let mut compressing = SurrealDbServiceClient::connect(endpoint.clone())
		.await
		.expect("a raw gRPC client")
		.accept_compressed(CompressionEncoding::Zstd);
	let context = authenticate(&mut compressing).await;
	let response = compressing
		.export_surql(rpc::ExportSurqlRequest {
			context,
			config: None,
		})
		.await
		.expect("export");
	assert_eq!(
		response.metadata().get("grpc-encoding").map(|v| v.to_str().expect("ascii")),
		Some("zstd"),
		"a client advertising zstd must be served zstd"
	);

	// A client that advertises nothing is served identity, and the stream still
	// carries the export.
	let mut plain = SurrealDbServiceClient::connect(endpoint).await.expect("a raw gRPC client");
	let context = authenticate(&mut plain).await;
	let response = plain
		.export_surql(rpc::ExportSurqlRequest {
			context,
			config: None,
		})
		.await
		.expect("export");
	assert!(
		response.metadata().get("grpc-encoding").is_none(),
		"a client advertising no encoding must be served identity"
	);

	let mut body = Vec::new();
	let mut stream = response.into_inner();
	while let Some(frame) = stream.message().await.expect("a frame") {
		if let Some(rpc::export_surql_response::Frame::Chunk(chunk)) = frame.frame {
			body.extend_from_slice(&chunk.data);
		}
	}
	let text = String::from_utf8(body).expect("the export is utf-8");
	assert!(text.contains("person:1"), "the uncompressed export must still carry the record");
}

/// A versioned export is refused as the caller's mistake, before the stream
/// opens.
///
/// A dump carries the current state of each record and has no grammar for a
/// record's history, so no retry of the same request can succeed. The code is
/// what this asserts: `INTERNAL` reads as a server fault and invites a client to
/// try again, where `INVALID_ARGUMENT` tells it to change the request. The
/// refusal has to arrive as a status rather than an error frame, because a
/// stream that opened has already reported success.
#[tokio::test]
async fn a_versioned_export_is_refused_as_an_invalid_argument() {
	use surrealdb_protocol::proto::rpc::v1 as rpc;

	let server = TestServer::start().await;
	let db = server.connect_as_root().await;
	db.query("CREATE person:1 SET name = 'a'").await.expect("send").check().expect("seed");

	let mut client = SurrealDbServiceClient::connect(format!("http://{}", server.address))
		.await
		.expect("a raw gRPC client");
	let context = authenticate(&mut client).await;
	let status = client
		.export_surql(rpc::ExportSurqlRequest {
			context,
			config: Some(rpc::ExportConfig {
				versions: true,
				..Default::default()
			}),
		})
		.await
		.expect_err("a versioned export must be refused");

	assert_eq!(
		status.code(),
		tonic::Code::InvalidArgument,
		"a request the format cannot represent must not report a server fault: {status:?}"
	);
	assert!(
		status.message().contains("Versioned export is not supported"),
		"the refusal must say why: {status:?}"
	);
}

/// An import streams a whole database up, so it is the request worth
/// compressing -- and the only one. Small calls are left alone, because framing
/// and compressing a few hundred bytes costs more than it saves.
///
/// Asserted from what the server received rather than from what the client was
/// configured to do, since the point is what crossed the network.
#[tokio::test]
async fn imports_upload_compressed_and_small_calls_do_not() {
	let (server, seen) = TestServer::start_recording().await;
	let db = server.connect_as_root().await;
	db.query("CREATE person:exported SET name = 'Tobie'")
		.await
		.expect("send")
		.check()
		.expect("create");

	let file = tempfile::NamedTempFile::new().expect("temp file");
	db.export(file.path()).await.expect("export");

	let restored = server.connect_to_database("restored").await;
	restored.import(file.path()).await.expect("import");
	let people: Vec<Value> = restored.select("person").await.expect("select");
	assert_eq!(people.len(), 1, "the import must still land");

	let seen = seen.lock().expect("encoding log poisoned");
	let encoding_of = |suffix: &str| -> Option<&str> {
		seen.iter().find(|(path, _)| path.ends_with(suffix)).map(|(_, encoding)| encoding.as_str())
	};

	// Exactly one attempt, and it compressed: reading the capability is what
	// makes the compressed attempt safe, so a retry here would mean the answer
	// was not trusted or was wrong.
	let imports: Vec<&str> = seen
		.iter()
		.filter(|(path, _)| path.ends_with("/ImportSurql"))
		.map(|(_, encoding)| encoding.as_str())
		.collect();
	assert_eq!(imports, ["zstd"], "one compressed import attempt, saw {seen:?}");
	// The handshake is what negotiated it, and every ordinary call after it
	// stays uncompressed.
	assert_eq!(
		encoding_of("/Query"),
		Some("identity"),
		"a query must not pay to compress a few hundred bytes, saw {seen:?}"
	);
	assert_eq!(
		encoding_of("/Signin"),
		Some("identity"),
		"a signin must not be compressed, saw {seen:?}"
	);
	// The handshake reads the answer out of the capabilities rather than
	// probing for it, so it is asked once and asked uncompressed.
	let handshakes: Vec<&str> = seen
		.iter()
		.filter(|(path, _)| path.ends_with("/GetCapabilities"))
		.map(|(_, encoding)| encoding.as_str())
		.collect();
	assert!(
		handshakes.iter().all(|encoding| *encoding == "identity"),
		"the handshake must not be compressed, saw {handshakes:?}"
	);
	assert_eq!(
		handshakes.len(),
		2,
		"one handshake per connection, and this test opens two, saw {handshakes:?}"
	);
}

/// A frame's size must come from the chunk size the server advertises, not from
/// how wide the exported records happen to be.
///
/// The exporter writes one line per `INSERT` statement, so a line is as wide as
/// the records it carries. Forwarding lines as frames tied the two together, and
/// records of a few KB were enough to push a frame past the very message limit
/// the same handshake reports -- leaving a client that honoured that limit
/// unable to decode its own export.
///
/// The client here is left at its default limits, so it stands in for exactly
/// that client.
#[tokio::test]
async fn export_frames_are_bounded_regardless_of_record_width() {
	use surrealdb_protocol::proto::rpc::v1 as rpc;

	const CHUNK: usize = surrealdb_protocol::DEFAULT_FILE_CHUNK_SIZE;
	// Wide enough that one `INSERT` line runs past the 4 MiB default decode
	// limit: the exporter groups up to 1000 records per line, and these are
	// ~300 KiB each.
	const RECORDS: usize = 20;
	const FIELD_BYTES: usize = 300 * 1024;

	let server = TestServer::start().await;
	let db = server.connect_as_root().await;
	let blob = "x".repeat(FIELD_BYTES);
	for i in 0..RECORDS {
		db.query(format!("CREATE person:{i} SET blob = '{blob}'"))
			.await
			.expect("send")
			.check()
			.expect("seed");
	}

	let mut client = SurrealDbServiceClient::connect(format!("http://{}", server.address))
		.await
		.expect("a raw gRPC client");
	let context = authenticate(&mut client).await;
	let mut stream = client
		.export_surql(rpc::ExportSurqlRequest {
			context,
			config: None,
		})
		.await
		.expect("export")
		.into_inner();

	let mut sizes = Vec::new();
	let mut body = Vec::new();
	let mut trailer = None;
	while let Some(response) = stream.message().await.expect("a frame") {
		match response.frame {
			Some(rpc::export_surql_response::Frame::Chunk(chunk)) => {
				sizes.push(chunk.data.len());
				body.extend_from_slice(&chunk.data);
			}
			Some(rpc::export_surql_response::Frame::Trailer(t)) => trailer = Some(t),
			Some(rpc::export_surql_response::Frame::Error(e)) => panic!("export failed: {e:?}"),
			None => panic!("a frame with no payload"),
		}
	}

	let largest = sizes.iter().copied().max().expect("the export must carry chunks");
	assert!(
		largest <= CHUNK,
		"no frame may exceed the advertised chunk size of {CHUNK}, largest was {largest}"
	);
	// The payload is several times the chunk size, so this really did have to
	// split rather than happening to fit.
	assert!(
		body.len() > 4 * CHUNK,
		"the fixture must produce an export worth splitting, got {} bytes",
		body.len()
	);
	// Framing is not allowed to change the bytes or what the trailer says about
	// them.
	let trailer = trailer.expect("a complete export ends with a trailer");
	assert_eq!(trailer.bytes as usize, body.len(), "the trailer must count what was sent");
	assert_eq!(
		trailer.blake3,
		blake3::hash(&body).to_hex().to_string(),
		"the digest must cover the reassembled bytes"
	);
	let text = String::from_utf8(body).expect("the export is utf-8");
	assert!(text.contains("person:0"), "the export must carry its records");
	assert!(text.contains(&format!("person:{}", RECORDS - 1)));
}

/// An error a query composes has no bound: `THROW` puts the thrown value in the
/// message. A status carries its message as a trailer, where the peer's HTTP/2
/// header list -- not the gRPC message size -- is the limit, and an oversized
/// header block is refused rather than truncated: the caller sees an h2
/// protocol error and loses the error entirely. Trimming is what keeps it an
/// error.
#[tokio::test]
async fn an_unbounded_thrown_message_still_arrives_as_an_error() {
	let server = TestServer::start().await;
	let db = server.connect_as_root().await;

	db.query("DEFINE FUNCTION fn::boom() { THROW string::repeat('q', 1000000) }")
		.await
		.expect("send")
		.check()
		.expect("define");

	let error = db.run::<Value>("fn::boom").await.expect_err("the function throws");
	let rendered = error.to_string();
	assert!(
		rendered.contains("qqq"),
		"the thrown value has to survive as far as it fits: {rendered}"
	);
	assert!(
		!rendered.contains("h2 protocol error"),
		"an oversized trailer costs the caller the error itself: {rendered}"
	);
}
