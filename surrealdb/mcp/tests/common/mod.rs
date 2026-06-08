//! Shared helpers for MCP integration tests.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mcp::McpService;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// Build a fresh in-memory datastore with the `test` namespace and database
/// pre-created. Mirrors the bootstrap in `surrealdb/core/tests/helpers/mod.rs`.
pub async fn test_datastore() -> Arc<Datastore> {
	let ds = Arc::new(Datastore::new("memory").await.expect("Failed to create datastore"));
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.expect("bootstrap NS");
	ds.execute("DEFINE DATABASE test;", &Session::owner().with_ns("test"), None)
		.await
		.expect("bootstrap DB");
	ds
}

/// Owner-level session scoped to the `test` namespace and database.
pub fn root_session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

/// Build an initialised `McpService` for direct handler testing (no transport).
pub async fn init_service(ds: Arc<Datastore>) -> McpService {
	let service =
		McpService::new(ds, Some("test".to_string()), Some("test".to_string()), Session::owner());
	service.init_session(root_session()).expect("Failed to init session");
	service
}

/// Concatenate all text fragments of a `CallToolResult` into one string for
/// plain-text assertions.
pub fn content_text(result: &rmcp::model::CallToolResult) -> String {
	result
		.content
		.iter()
		.filter_map(|c| c.raw.as_text())
		.map(|t| t.text.as_str())
		.collect::<Vec<_>>()
		.join("\n")
}

/// Single captured `tracing` event, reduced to its target plus a flat
/// `field -> stringified value` map. We record both the `record_str`
/// path (string fields like `tool`, `subject`, `outcome`) and the
/// `record_debug` fallback so numeric / `Option` fields land too.
#[derive(Debug, Clone)]
pub struct CapturedEvent {
	pub target: String,
	pub fields: HashMap<String, String>,
}

impl CapturedEvent {
	pub fn field(&self, name: &str) -> Option<&str> {
		self.fields.get(name).map(String::as_str)
	}
}

/// Process-global capture buffer for `surrealdb::mcp::audit` events.
///
/// We install the subscriber via [`tracing::dispatcher::set_global_default`]
/// (rmcp's HTTP transport spawns worker tasks whose poll thread is not
/// guaranteed to be the test thread, so a thread-local default would
/// drop the audit emission produced inside the worker future) and
/// serialise audit-capture tests through [`AUDIT_TEST_LOCK`]. The
/// global subscriber is installed exactly once per test binary via
/// [`OnceLock::get_or_init`], and [`install_audit_capture`] drains
/// any leftover events from a previous test before handing the guard
/// to the next caller so each test starts with an empty buffer.
static AUDIT_BUFFER: std::sync::OnceLock<Arc<Mutex<Vec<CapturedEvent>>>> =
	std::sync::OnceLock::new();

/// Mutex held for the duration of a single audit-capture test so two
/// concurrent tests can't observe each other's events when reading
/// from the shared global buffer.
static AUDIT_TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Default, Clone)]
struct GlobalAuditLayer;

impl<S> Layer<S> for GlobalAuditLayer
where
	S: Subscriber + for<'a> LookupSpan<'a>,
{
	fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
		// Filter at the layer to keep the global buffer small —
		// non-audit events from any other test running on the same
		// process never enter the buffer.
		let target = event.metadata().target();
		if target != "surrealdb::mcp::audit" {
			return;
		}
		let mut fields: HashMap<String, String> = HashMap::new();
		let mut visitor = FieldVisitor(&mut fields);
		event.record(&mut visitor);
		let buffer = AUDIT_BUFFER.get().expect("audit buffer initialised before subscriber");
		buffer.lock().expect("audit buffer mutex").push(CapturedEvent {
			target: target.to_string(),
			fields,
		});
	}
}

struct FieldVisitor<'a>(&'a mut HashMap<String, String>);

impl Visit for FieldVisitor<'_> {
	fn record_str(&mut self, field: &Field, value: &str) {
		self.0.insert(field.name().to_string(), value.to_string());
	}
	fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
		self.0.insert(field.name().to_string(), format!("{value:?}"));
	}
	fn record_i64(&mut self, field: &Field, value: i64) {
		self.0.insert(field.name().to_string(), value.to_string());
	}
	fn record_u64(&mut self, field: &Field, value: u64) {
		self.0.insert(field.name().to_string(), value.to_string());
	}
	fn record_bool(&mut self, field: &Field, value: bool) {
		self.0.insert(field.name().to_string(), value.to_string());
	}
	fn record_f64(&mut self, field: &Field, value: f64) {
		self.0.insert(field.name().to_string(), value.to_string());
	}
}

/// Handle returned by [`install_audit_capture`]. Holds the per-binary
/// serialisation mutex (so concurrent audit-capture tests don't see
/// each other's events) and exposes accessors over the shared
/// global buffer.
pub struct AuditCapture<'a> {
	_guard: std::sync::MutexGuard<'a, ()>,
	buffer: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl AuditCapture<'_> {
	/// Snapshot all captured events emitted while this guard is held,
	/// in emission order.
	pub fn audit_events(&self) -> Vec<CapturedEvent> {
		self.buffer.lock().expect("audit buffer mutex").clone()
	}
}

/// Install the process-global audit subscriber if it isn't already,
/// then take exclusive ownership of the capture buffer for the
/// duration of the returned guard. Drains any leftover events from a
/// previous test before handing the guard to the caller so each test
/// starts with an empty buffer.
///
/// Use this *before* spinning up the MCP service so the subscriber is
/// active for the entire test, including any tasks rmcp's transport
/// spawns internally.
pub fn install_audit_capture() -> AuditCapture<'static> {
	let buffer = Arc::clone(AUDIT_BUFFER.get_or_init(|| {
		use tracing::dispatcher::Dispatch;
		use tracing_subscriber::layer::SubscriberExt;
		let buffer: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
		// `set_global_default` can only be called once per
		// process. We stash the buffer first so the layer can
		// always find it, then install the subscriber.
		let layer = GlobalAuditLayer;
		let subscriber = tracing_subscriber::registry().with(layer);
		tracing::dispatcher::set_global_default(Dispatch::new(subscriber))
			.expect("audit subscriber must install once per test binary");
		buffer
	}));
	let guard = AUDIT_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
	buffer.lock().expect("audit buffer mutex").clear();
	AuditCapture {
		_guard: guard,
		buffer,
	}
}
