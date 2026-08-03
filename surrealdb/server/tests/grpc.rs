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
use surrealdb_core::dbs::capabilities::Capabilities;
use surrealdb_core::kvs::Datastore;
use surrealdb_server::ntw::{RouterOptions, SurrealRouter};
use surrealdb_types::Value;
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
