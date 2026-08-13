//! MCP ServerHandler implementation for SurrealDB.
//!
//! `McpService` is the core MCP server type. One instance is created per MCP
//! session via the factory closure in `StreamableHttpService`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::service::RequestContext;
use rmcp::{ErrorData as McpError, RoleServer, ServerHandler, tool, tool_handler, tool_router};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use tokio::sync::OnceCell;
use web_time::Instant;

use crate::auth::{self, BoundSubject};
use crate::cnf::McpConfig;
use crate::metrics::{McpMetricsRecorder, McpToolOutcome};
use crate::session::McpSession;
use crate::tools::{ToolScope, connection, crud, gql, graphql, query, run as run_tool, schema};
use crate::{audit, completions, prompts, resources};

const LOG: &str = "surrealdb::mcp";

/// Transport label that opts an [`McpService`] out of the strict
/// per-request subject check in [`McpService::verify_request_subject`].
///
/// Stdio is a single trusted process pipe — there is no per-request
/// credential channel and no session-hijack vector. Any other label
/// (set via [`McpService::with_transport_label`] /
/// [`McpServiceConfig::with_transport_label`]) is treated as a
/// networked transport and runs the strict check. Defaulting to
/// `"stdio"` matches the legacy in-process embedders; the HTTP factory
/// in [`http`] explicitly overrides this with `"http"`.
const STDIO_TRANSPORT_LABEL: &str = "stdio";

/// What a legacy `initialize` handshake binds to a service: the long-lived
/// session whose `use` state persists across calls, plus the fingerprint of the
/// subject that opened it.
///
/// The two live in one cell rather than two because
/// [`McpService::verify_request_subject`] authorizes on the *absence* of a
/// handshake: "there is no session to serve this from" and "there is no subject
/// to impersonate" must be a single observation. Two cells could be read in a
/// state where they disagree, and a service that reported itself sessionless
/// while still holding a subject would skip the strict credential check.
struct BoundHandshake {
	session: McpSession,
	/// Used by [`McpService::verify_request_subject`] to reject inbound
	/// requests that present a *different* authenticated identity on the same
	/// MCP session id (the spec's "MUST verify all inbound requests" rule).
	subject: BoundSubject,
}

/// The MCP server handler for SurrealDB.
#[derive(Clone)]
pub struct McpService {
	/// Empty until `initialize` binds a handshake. Emptiness *is* the
	/// definition of a sessionless request in this service; see
	/// [`McpService::is_stateless`].
	handshake: Arc<OnceCell<BoundHandshake>>,
	datastore: Arc<Datastore>,
	default_ns: Option<String>,
	default_db: Option<String>,
	/// Fallback session used when no authenticated session is attached to the
	/// incoming request context (e.g. the STDIO transport). HTTP callers go
	/// through `SurrealAuth` middleware and always supply a session via the
	/// request parts, so this is only consulted for in-process transports.
	base_session: Session,
	/// MCP-side runtime configuration. Cloned into every [`McpSession`] so
	/// handlers read caps off the session rather than process-global
	/// statics. See [`McpConfig`] for the available knobs.
	config: Arc<McpConfig>,
	#[allow(dead_code)] // Read by #[tool_handler] macro-generated code
	tool_router: ToolRouter<Self>,
	/// Optional metrics recorder. When present, every tool dispatch fires
	/// a single `record_tool_invocation` call. The MCP crate has no
	/// dependency on the OpenTelemetry SDK; embedders supply the
	/// recorder.
	metrics_recorder: Option<Arc<dyn McpMetricsRecorder>>,
	/// Static identifier for the wire transport this service is mounted
	/// on. Recorded as the `transport` attribute on every metric so
	/// operators can split stdio vs HTTP MCP traffic.
	transport_label: &'static str,
	/// Drop guard that fires the matching `-1` on the
	/// `surrealdb.mcp.session.active` gauge when the last clone of this
	/// `McpService` is dropped. `McpService` derives `Clone`, and rmcp
	/// may clone the per-session service internally, so the decrement
	/// MUST be tied to the `Arc` reference count rather than to a single
	/// `Drop` impl on `McpService`.
	session_gauge: Arc<SessionGaugeGuard>,
}

/// The session a single tool call runs against.
///
/// Either the long-lived session a legacy handshake bound — reused so `use`
/// keeps working across calls — or one built for this request alone, which is
/// how a stateless request and a scope-bearing legacy call are both served
/// without touching connection state.
///
/// The owned variant is boxed to keep the enum pointer-sized: a handshake
/// session is borrowed, and allocating for the per-request case costs nothing
/// measurable beside the query it is about to run.
enum ResolvedSession<'a> {
	Handshake(&'a McpSession),
	PerRequest(Box<McpSession>),
}

impl ResolvedSession<'_> {
	fn get(&self) -> &McpSession {
		match self {
			Self::Handshake(session) => session,
			Self::PerRequest(session) => session,
		}
	}
}

/// Drop-guard that decrements the `surrealdb.mcp.session.active` gauge
/// when its containing `Arc` is fully released.
///
/// Each [`McpService`] holds its `Arc<SessionGaugeGuard>`. When the last
/// service clone goes away the `Arc` refcount drops to zero and this
/// `Drop` fires once. The decrement is gated on
/// [`Self::incremented`] so services that were constructed but never
/// initialised (the streamable HTTP factory builds idle instances during
/// startup) do not produce a `-1` that has no matching `+1`.
struct SessionGaugeGuard {
	recorder: Option<Arc<dyn McpMetricsRecorder>>,
	transport: &'static str,
	incremented: AtomicBool,
}

impl Drop for SessionGaugeGuard {
	fn drop(&mut self) {
		if !self.incremented.load(Ordering::Acquire) {
			return;
		}
		if let Some(recorder) = self.recorder.as_ref() {
			recorder.adjust_session_active(-1, self.transport);
		}
	}
}

/// Builder-style configuration for [`McpService`].
///
/// The positional `McpService::new(ds, ns, db, session)` signature has two
/// `Option<String>` parameters that are easy to transpose accidentally.
/// Prefer this builder in new code; [`McpService::new`] is kept for
/// compatibility.
#[derive(Clone)]
pub struct McpServiceConfig {
	datastore: Arc<Datastore>,
	default_ns: Option<String>,
	default_db: Option<String>,
	base_session: Session,
	config: Arc<McpConfig>,
	metrics_recorder: Option<Arc<dyn McpMetricsRecorder>>,
	transport_label: &'static str,
}

impl McpServiceConfig {
	/// Start a fresh config for the given datastore.
	///
	/// `base_session` defaults to [`Session::default`]. In-process callers
	/// (e.g. `surreal mcp` stdio) that want pre-authenticated root access
	/// should call [`Self::with_base_session`] with [`Session::owner`].
	///
	/// The MCP runtime configuration is loaded from the `SURREAL_MCP_*`
	/// environment; embedders that already hold a loaded config (or don't
	/// want env auto-loading) should use [`Self::new_with_config`] instead
	/// of paying a redundant env read.
	pub fn new(datastore: Arc<Datastore>) -> Self {
		Self::new_with_config(datastore, McpConfig::from_env())
	}

	/// Like [`Self::new`] but takes an explicit [`McpConfig`], skipping the
	/// `SURREAL_MCP_*` environment read.
	///
	/// The in-tree HTTP factory uses this so the environment is read once at
	/// service construction and a single `Arc<McpConfig>` is shared across
	/// every per-session service, rather than re-read (and immediately
	/// discarded) on each new session.
	pub fn new_with_config(datastore: Arc<Datastore>, config: Arc<McpConfig>) -> Self {
		Self {
			datastore,
			default_ns: None,
			default_db: None,
			base_session: Session::default(),
			config,
			metrics_recorder: None,
			// Default transport label for in-process / stdio embedders.
			// HTTP embedders override via [`Self::with_transport_label`].
			transport_label: STDIO_TRANSPORT_LABEL,
		}
	}

	/// Attach a metrics recorder. Pass an [`Arc`]'d implementation from
	/// the embedding crate; the MCP service will fire one
	/// `record_tool_invocation` per dispatch.
	pub fn with_metrics_recorder(mut self, recorder: Arc<dyn McpMetricsRecorder>) -> Self {
		self.metrics_recorder = Some(recorder);
		self
	}

	/// Override the static transport label.
	///
	/// The label has two roles:
	///
	/// 1. It is recorded as the `transport` attribute on every emitted metric so operators can
	///    split stdio vs HTTP MCP traffic.
	/// 2. It is the discriminator consulted by [`McpService::verify_request_subject`] to decide
	///    whether to run the strict per-request subject check. Only the literal value `"stdio"`
	///    opts out of that check; any other label (HTTP, or a future custom networked transport)
	///    runs it.
	///
	/// Defaults to `"stdio"`. HTTP embedders MUST pass `"http"` (or any
	/// other non-`"stdio"` label) so the strict check fires; the
	/// in-tree HTTP factory does this automatically.
	pub fn with_transport_label(mut self, label: &'static str) -> Self {
		self.transport_label = label;
		self
	}

	/// Set the default namespace applied to any session that doesn't
	/// already carry one.
	pub fn with_default_namespace(mut self, ns: impl Into<String>) -> Self {
		self.default_ns = Some(ns.into());
		self
	}

	/// Set the default database applied to any session that doesn't
	/// already carry one.
	pub fn with_default_database(mut self, db: impl Into<String>) -> Self {
		self.default_db = Some(db.into());
		self
	}

	/// Override the fallback [`Session`] used when no HTTP auth context is
	/// attached to the request (e.g. the STDIO transport).
	pub fn with_base_session(mut self, session: Session) -> Self {
		self.base_session = session;
		self
	}

	/// Override the MCP runtime configuration. Use this when an embedder
	/// has its own configuration source and shouldn't be reading
	/// `SURREAL_MCP_*` from the process environment.
	pub fn with_config(mut self, config: Arc<McpConfig>) -> Self {
		self.config = config;
		self
	}

	/// Consume the builder and construct an [`McpService`].
	pub fn build(self) -> McpService {
		let svc = McpService::new_with_config(
			self.datastore,
			self.default_ns,
			self.default_db,
			self.base_session,
			self.config,
		);
		// Route the recorder / transport through the builder methods so
		// the session-gauge guard is rebuilt with the right
		// configuration before any [`McpService::init_session`] call.
		let svc = svc.with_transport_label(self.transport_label);
		match self.metrics_recorder {
			Some(rec) => svc.with_metrics_recorder(rec),
			None => svc,
		}
	}
}

impl McpService {
	/// Construct a new `McpService`.
	///
	/// Prefer [`McpServiceConfig`] for new call sites -- it avoids the
	/// positional `Option<String>, Option<String>` footgun. This
	/// constructor is retained for backwards-compatibility with existing
	/// callers.
	///
	/// `base_session` is used as the session when no HTTP auth context is
	/// present on the request (the STDIO transport case). Callers exposing a
	/// network surface should pass `Session::default()` and rely on the
	/// HTTP auth middleware to attach an authenticated session; in-process
	/// callers (e.g. `surreal mcp` stdio) should pass `Session::owner()`.
	pub fn new(
		datastore: Arc<Datastore>,
		default_ns: Option<String>,
		default_db: Option<String>,
		base_session: Session,
	) -> Self {
		// Default constructor loads MCP configuration from the
		// `SURREAL_MCP_*` environment, matching the behaviour of every
		// public binary (`surreal mcp`, the HTTP `/mcp` route).
		Self::new_with_config(
			datastore,
			default_ns,
			default_db,
			base_session,
			McpConfig::from_env(),
		)
	}

	/// Construct a new `McpService` with an explicit [`McpConfig`].
	///
	/// Used by [`McpServiceConfig::build`] and by tests / embedders that
	/// want to override the cap defaults without going via the
	/// `SURREAL_MCP_*` environment.
	pub fn new_with_config(
		datastore: Arc<Datastore>,
		default_ns: Option<String>,
		default_db: Option<String>,
		base_session: Session,
		config: Arc<McpConfig>,
	) -> Self {
		let mut tool_router = Self::tool_router();
		crate::tools::output_schemas::attach(&mut tool_router);
		Self {
			handshake: Arc::new(OnceCell::new()),
			datastore,
			default_ns,
			default_db,
			base_session,
			config,
			tool_router,
			metrics_recorder: None,
			transport_label: STDIO_TRANSPORT_LABEL,
			session_gauge: Arc::new(SessionGaugeGuard {
				recorder: None,
				transport: STDIO_TRANSPORT_LABEL,
				incremented: AtomicBool::new(false),
			}),
		}
	}

	/// Attach an [`McpMetricsRecorder`] to an existing service. The MCP
	/// crate has no compile-time dependency on a metrics SDK; embedders
	/// supply the recorder.
	///
	/// Must be called before [`Self::init_session`]: rebuilds the
	/// internal session-gauge drop-guard so the recorder seen at session
	/// teardown matches the one that observed the bump.
	pub fn with_metrics_recorder(mut self, recorder: Arc<dyn McpMetricsRecorder>) -> Self {
		self.metrics_recorder = Some(recorder);
		self.rebuild_session_gauge();
		self
	}

	/// Override the static transport label.
	///
	/// The label has two roles:
	///
	/// 1. It is recorded as the `transport` attribute on every emitted metric so operators can
	///    split stdio vs HTTP MCP traffic.
	/// 2. It is the discriminator consulted by [`Self::verify_request_subject`] to decide whether
	///    to run the strict per-request subject check. Only the literal value `"stdio"` opts out of
	///    that check; any other label (HTTP, or a future custom networked transport) runs it.
	///
	/// Defaults to `"stdio"`. HTTP embedders MUST pass `"http"` (or any
	/// other non-`"stdio"` label) so the strict check fires; the
	/// in-tree HTTP factory does this automatically.
	///
	/// Must be called before [`Self::init_session`]: rebuilds the
	/// internal session-gauge drop-guard so the transport label seen at
	/// session teardown matches the one used at the bump.
	pub fn with_transport_label(mut self, label: &'static str) -> Self {
		self.transport_label = label;
		self.rebuild_session_gauge();
		self
	}

	/// Recreate the session-gauge guard with the current recorder /
	/// transport. Called from the builder methods so the guard reflects
	/// the configuration that will actually be in force at
	/// [`Self::init_session`] time.
	fn rebuild_session_gauge(&mut self) {
		self.session_gauge = Arc::new(SessionGaugeGuard {
			recorder: self.metrics_recorder.clone(),
			transport: self.transport_label,
			incremented: AtomicBool::new(false),
		});
	}

	fn session(&self) -> Result<&McpSession, McpError> {
		self.handshake
			.get()
			.map(|bound| &bound.session)
			.ok_or_else(|| McpError::internal_error("MCP session not initialized", None))
	}

	/// Get a reference to the inner session, if initialized.
	pub fn session_ref(&self) -> Result<&McpSession, McpError> {
		self.session()
	}

	/// Initialize the session. Called during MCP handshake.
	///
	/// Records the [`BoundSubject`] fingerprint of `session` so subsequent
	/// inbound requests on the same MCP session id can be verified against
	/// it. Calling `init_session` more than once on the same `McpService`
	/// is a protocol error.
	pub fn init_session(&self, session: Session) -> Result<(), McpError> {
		let subject = BoundSubject::from_session(&session);
		let mcp_session =
			McpSession::with_config(Arc::clone(&self.datastore), session, Arc::clone(&self.config));
		// One `set` publishes the session and the subject together, so no
		// reader can observe a service that holds one without the other.
		self.handshake
			.set(BoundHandshake {
				session: mcp_session,
				subject,
			})
			.map_err(|_| McpError::internal_error("Session already initialized", None))?;
		// Bump the active-session gauge now that the session is bound.
		// The matching `-1` lives in `SessionGaugeGuard::drop`, fired
		// when the last clone of this `McpService` is released so the
		// gauge tracks live MCP sessions rather than per-request
		// service clones. `compare_exchange` keeps the bump idempotent
		// if `init_session` is somehow re-entered (it normally errors
		// above on the second call).
		if let Some(recorder) = self.metrics_recorder.as_ref()
			&& self
				.session_gauge
				.incremented
				.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
				.is_ok()
		{
			recorder.adjust_session_active(1, self.transport_label);
		}
		Ok(())
	}

	/// Whether this service runs over the stdio transport.
	///
	/// Stdio is a single trusted process pipe — there is no per-request
	/// credential channel to verify against and no session-hijack vector
	/// of the kind described in the MCP security best-practices document.
	/// Networked transports (HTTP, custom embedders) MUST run the strict
	/// subject check in [`Self::verify_request_subject`].
	fn is_stdio_transport(&self) -> bool {
		self.transport_label == STDIO_TRANSPORT_LABEL
	}

	/// Reject an inbound request that presents missing, anonymous, or
	/// disagreeing credentials when the MCP session was bound to an
	/// authenticated subject at `initialize` time.
	///
	/// The strict subject-match rule closes the session-hijack vector
	/// described in the MCP security best-practices document: possession
	/// of an `mcp-session-id` alone must not let an attacker drop
	/// credentials (or present anonymous ones) and have the underlying
	/// authenticated session keep serving the call.
	///
	/// The check is networked-transport only. Stdio transports do not
	/// have a per-request credential channel and have no session-hijack
	/// vector — there is a single trusted process driving the pipe and
	/// the bound subject captured at handshake is authoritative. The
	/// discriminator is [`Self::is_stdio_transport`], driven by the
	/// `transport_label` set by the embedder at construction time
	/// (HTTP factories call [`Self::with_transport_label`] with
	/// `"http"`; the stdio default is `"stdio"`).
	///
	/// Outcomes (see [`crate::auth::check_subject`]):
	///
	/// - Sessionless request, which by definition has no bound subject: allowed; it carries its own
	///   credentials and is authenticated before reaching this handler.
	/// - Stdio transport: allowed without re-checking incoming credentials.
	/// - HTTP, no or anonymous credentials on a non-anonymous bound session: rejected with
	///   `invalid_params`.
	/// - HTTP, same authenticated subject as binding: allowed.
	/// - HTTP, different authenticated subject than binding: rejected with `invalid_params`.
	fn verify_request_subject(&self, ctx: &RequestContext<RoleServer>) -> Result<(), McpError> {
		let Some(bound) = self.handshake.get() else {
			// No handshake, so there is no subject to impersonate and nothing
			// for a replayed session id to reach. A sessionless request
			// legitimately has none: it carries its own credentials and is
			// authenticated before it gets here.
			//
			// The bypass keys on the handshake cell, never on the request's
			// protocol version alone: a bound session can report the
			// sessionless revision, so a version-only test would return `Ok`
			// here and run the call under the bound subject's credentials
			// without matching them. See [`Self::is_stateless`].
			if self.is_stateless(ctx) {
				return Ok(());
			}
			// No handshake and not a sessionless request: return a
			// protocol-level error so the caller knows to send `initialize`
			// first.
			return Err(McpError::internal_error(
				"MCP session not initialized: send `initialize` first",
				None,
			));
		};
		// Stdio is single-tenant: no per-request credential channel and
		// no session-hijack vector. The bound subject captured at
		// handshake is authoritative. Networked transports MUST run the
		// strict check; see [`Self::is_stdio_transport`] for the
		// discriminator contract.
		if self.is_stdio_transport() {
			return Ok(());
		}
		let incoming = auth::incoming_subject(ctx);
		auth::check_subject(&bound.subject, incoming)
	}

	/// Audit label for whoever is making this request.
	///
	/// A handshake-bound subject wins, so legacy sessions keep logging the
	/// identity captured at `initialize`. Without one — the stateless case —
	/// the label is derived from the credentials on the request itself, which
	/// is the only identity that exists there.
	fn request_subject_label(&self, ctx: &RequestContext<RoleServer>) -> String {
		if let Some(bound) = self.handshake.get() {
			return bound.subject.audit_label();
		}
		auth::incoming_subject(ctx)
			.map(|subject| subject.audit_label())
			.unwrap_or_else(|| "anonymous".into())
	}

	/// Whether this request is genuinely sessionless: no handshake bound it, so
	/// its credentials and its scope come from the request itself.
	///
	/// The handshake half is what makes the answer authoritative, and it must
	/// come first. `ctx.protocol_version()` reports the version in the
	/// request's own `_meta`, falling back to the one *negotiated* at handshake
	/// when the request omits it — and that negotiated value is not the
	/// client's choice: rmcp answers a version outside
	/// [`SUPPORTED_PROTOCOL_VERSIONS`] with [`ADVERTISED_PROTOCOL_VERSION`] and
	/// records it as the peer's version. Since the advertised revision is the
	/// sessionless one, a legacy session opened with any unrecognised version
	/// string reports the sessionless revision on every later request, while
	/// still holding a session and a bound subject. So the version alone
	/// answers "which protocol era are we speaking", never "is there a
	/// handshake here" — only the cell answers that, and only that question is
	/// safe to authorize against.
	///
	/// The version half remains load-bearing for the reverse case: a caller on
	/// an older revision that never sent `initialize` has no handshake either,
	/// and must be told to send one rather than served anonymously.
	fn is_stateless(&self, ctx: &RequestContext<RoleServer>) -> bool {
		self.handshake.get().is_none() && Self::claims_sessionless_revision(ctx)
	}

	/// Whether the request speaks a protocol revision that has no sessions.
	///
	/// This is the request's *claim* about its era, which on its own says
	/// nothing about whether a handshake exists; see [`Self::is_stateless`] for
	/// why the two questions must not be conflated.
	fn claims_sessionless_revision(ctx: &RequestContext<RoleServer>) -> bool {
		ctx.protocol_version().is_some_and(|version| version >= ProtocolVersion::V_2026_07_28)
	}

	/// Namespace and database carried by the request's `surreal-ns` /
	/// `surreal-db` headers, if any. Empty values are treated as absent so a
	/// blank header cannot blank out a configured default.
	fn header_scope(ctx: &RequestContext<RoleServer>) -> (Option<String>, Option<String>) {
		let Some(parts) = ctx.extensions.get::<http::request::Parts>() else {
			return (None, None);
		};
		let read = |name: &str| {
			parts
				.headers
				.get(name)
				.and_then(|value| value.to_str().ok())
				.map(str::to_string)
				.filter(|value| !value.is_empty())
		};
		(read("surreal-ns"), read("surreal-db"))
	}

	/// Resolve the session a tool call runs against.
	///
	/// Scope precedence, highest first: the call's own `namespace` /
	/// `database` arguments, the `surreal-ns` / `surreal-db` request headers,
	/// the handshake session's current `use` state, then the server's
	/// configured defaults. Each layer fills only the halves the layer above
	/// left unset, so a call may override the database while inheriting the
	/// namespace.
	///
	/// A handshake session is reused as-is when the call names no scope of its
	/// own, which is what keeps `use` meaningful for legacy clients. As soon as
	/// a call does name a scope, it runs against a derived session so the
	/// override cannot leak into the connection's `use` state and affect later
	/// calls.
	///
	/// Without a handshake the request must be a stateless one; anything else
	/// is a legacy client that skipped `initialize`, and still gets told so.
	async fn resolve_session(
		&self,
		ctx: &RequestContext<RoleServer>,
		scope: &ToolScope,
	) -> Result<ResolvedSession<'_>, McpError> {
		scope.validate()?;
		let (header_ns, header_db) = Self::header_scope(ctx);
		let ns = scope.namespace.clone().or(header_ns);
		let db = scope.database.clone().or(header_db);

		// The arms below have already established whether a handshake exists, so
		// they test the request's era with `claims_sessionless_revision` rather
		// than re-reading the cell through `is_stateless`.
		match self.handshake.get().map(|bound| &bound.session) {
			Some(session) if ns.is_none() && db.is_none() => {
				Ok(ResolvedSession::Handshake(session))
			}
			Some(session) => {
				Ok(ResolvedSession::PerRequest(Box::new(session.derive_scoped(ns, db).await)))
			}
			None if Self::claims_sessionless_revision(ctx) => {
				// No handshake ever happened, so both the caller's identity and
				// their scope come from this request. Networked transports get
				// the session the auth middleware attached; in-process ones
				// fall back to the configured base session.
				let base = ctx
					.extensions
					.get::<http::request::Parts>()
					.and_then(auth::extract_session_from_parts)
					.unwrap_or_else(|| self.base_session.clone());
				Ok(ResolvedSession::PerRequest(Box::new(McpSession::from_request(
					Arc::clone(&self.datastore),
					base,
					ns.or_else(|| self.default_ns.clone()),
					db.or_else(|| self.default_db.clone()),
					Arc::clone(&self.config),
				))))
			}
			None => Err(McpError::internal_error(
				"MCP session not initialized: send `initialize` first",
				None,
			)),
		}
	}

	/// Verify the request, run `handler`, and emit the canonical audit
	/// log line for the invocation. Centralises the boilerplate every
	/// `#[tool]` would otherwise repeat (verify → time → audit) so the
	/// individual handlers stay focused on the SurrealQL they execute.
	///
	/// The verification step is run *inside* the timed inner block so
	/// credential-mismatch rejections and "session not initialized"
	/// failures still emit one canonical audit record per attempt,
	/// classified as [`audit::Outcome::ProtocolError`]. This is the
	/// detection surface operators forward to a SIEM to spot session
	/// hijack attempts; silently dropping the rejection from the audit
	/// feed would defeat the spec-mandated subject-binding defence.
	///
	/// Uses [`AsyncFnOnce`] so callers can pass a normal `async |s| { ... }`
	/// closure that borrows `s: &McpSession` for the duration of the
	/// returned future without needing explicit `BoxFuture` machinery.
	async fn dispatch_tool<F>(
		&self,
		tool: &'static str,
		ctx: &RequestContext<RoleServer>,
		scope: &ToolScope,
		handler: F,
	) -> Result<CallToolResult, McpError>
	where
		F: AsyncFnOnce(&McpSession) -> Result<CallToolResult, McpError>,
	{
		let subject = self.request_subject_label(ctx);
		let started = Instant::now();
		// Verification and scope resolution run inside the timed span so a
		// rejected request still produces exactly one audit record.
		let resolved = async {
			self.verify_request_subject(ctx)?;
			self.resolve_session(ctx, scope).await
		}
		.await;
		// Log the scope the call actually ran against, which for a
		// scope-bearing call is not the connection's `use` state. A request
		// rejected before resolution has no scope to report.
		let (ns, db) = match &resolved {
			Ok(session) => session.get().current_ns_db().await,
			Err(_) => (None, None),
		};
		let outcome: Result<CallToolResult, McpError> = match resolved {
			Ok(session) => handler(session.get()).await,
			Err(err) => Err(err),
		};
		let elapsed = started.elapsed();
		let (kind, kind_str) = audit::classify(&outcome);
		audit::record(tool, &subject, ns.as_deref(), db.as_deref(), kind, &kind_str, elapsed);
		// Optional metrics dispatch. Mirrors the audit classification so
		// `surrealdb.mcp.tool.invocation{outcome="error"}` and the audit
		// log agree on every dispatch.
		if let Some(recorder) = self.metrics_recorder.as_ref() {
			let metric_outcome = match kind {
				audit::Outcome::Ok => McpToolOutcome::Success,
				audit::Outcome::ToolError => McpToolOutcome::ToolError,
				audit::Outcome::ProtocolError => McpToolOutcome::ProtocolError,
			};
			recorder.record_tool_invocation(tool, self.transport_label, metric_outcome, elapsed);
		}
		outcome
	}
}

/// Protocol revision advertised to a client that does not request a specific
/// supported one, and reported by the `surrealdb://info` resource.
///
/// Pinned explicitly rather than inherited from `ProtocolVersion::LATEST`: the
/// SDK constant tracks the newest revision the SDK models, which is not the
/// same claim as the newest revision this server implements. Binding to it
/// would let a dependency bump silently change the protocol SurrealDB
/// advertises.
pub const ADVERTISED_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::V_2026_07_28;

/// Every protocol revision this server implements, newest first.
///
/// This list bounds three things at once: what `server/discover` advertises,
/// what `initialize` negotiation may agree to, and which per-request protocol
/// versions are accepted. A revision absent here is answered by downgrading to
/// [`ADVERTISED_PROTOCOL_VERSION`] rather than by an error.
///
/// Both protocol eras are served on one endpoint. Under `2026-07-28` there is
/// no handshake and no session: the caller's credentials arrive with every
/// request and the namespace/database come from the call's own arguments,
/// headers, or the server's defaults. Under the older revisions the
/// `initialize` handshake still binds a session whose `use` state persists
/// across calls. [`McpService::resolve_session`] is the single point where
/// that difference is decided.
pub const SUPPORTED_PROTOCOL_VERSIONS: &[ProtocolVersion] = &[
	ProtocolVersion::V_2026_07_28,
	ProtocolVersion::V_2025_11_25,
	ProtocolVersion::V_2025_06_18,
	ProtocolVersion::V_2025_03_26,
	ProtocolVersion::V_2024_11_05,
];

// ---------------------------------------------------------------------------
// Tool implementations -- use types from tools/ modules directly
// ---------------------------------------------------------------------------

// Tool annotations follow the MCP hint spec as of 2025-11-25:
// - `read_only_hint = true` for tools that never write.
// - `destructive_hint = true` for tools that may mutate or remove data.
// - `idempotent_hint = true` for tools where repeated calls with the same arguments produce the
//   same result with no additional side effects.
// - `open_world_hint = false` everywhere because no MCP tool reaches the network on its own — every
//   effect is bounded by the local datastore and its capability rules.
#[tool_router]
impl McpService {
	#[tool(
		description = "Execute a SurrealQL query with optional parameterized inputs. Use $param syntax for placeholders and provide bindings in the parameters object.",
		annotations(
			title = "Run SurrealQL",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = false
		)
	)]
	async fn query(
		&self,
		Parameters(p): Parameters<query::QueryParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = p.scope.clone();
		self.dispatch_tool("query", &ctx, &scope, async |s| query::execute(s, p).await).await
	}

	#[tool(
		description = "SELECT records with optional filtering, sorting, and pagination. `fields`, `where_clause`, `order_clause`, `group_clause`, and `split_clause` are raw SurrealQL expression fragments -- use the `query` tool with $param bindings for dynamic values.",
		annotations(
			title = "Select records",
			read_only_hint = true,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = false
		)
	)]
	async fn select(
		&self,
		Parameters(p): Parameters<crud::SelectParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = p.scope.clone();
		self.dispatch_tool("select", &ctx, &scope, async |s| crud::select(s, p).await).await
	}

	#[tool(
		description = "CREATE a new record with optional content data. Data is bound as a typed variable.",
		annotations(
			title = "Create record",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = false
		)
	)]
	async fn create(
		&self,
		Parameters(p): Parameters<crud::CreateParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = p.scope.clone();
		self.dispatch_tool("create", &ctx, &scope, async |s| crud::create(s, p).await).await
	}

	#[tool(
		description = "INSERT records into a table. Data is bound as a typed variable. Supports IGNORE and RELATION flags.",
		annotations(
			title = "Insert records",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = false
		)
	)]
	async fn insert(
		&self,
		Parameters(p): Parameters<crud::InsertParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = p.scope.clone();
		self.dispatch_tool("insert", &ctx, &scope, async |s| crud::insert(s, p).await).await
	}

	#[tool(
		description = "UPSERT records with CONTENT, MERGE, or PATCH mode. Data is bound as a typed variable. `where_clause` is a SurrealQL expression fragment -- use the `query` tool with $param bindings for dynamic values.",
		annotations(
			title = "Upsert records",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = false
		)
	)]
	async fn upsert(
		&self,
		Parameters(p): Parameters<crud::UpsertParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = p.scope.clone();
		self.dispatch_tool("upsert", &ctx, &scope, async |s| crud::upsert(s, p).await).await
	}

	#[tool(
		description = "UPDATE existing records with CONTENT, MERGE, or PATCH mode. Data is bound as a typed variable. `where_clause` is a SurrealQL expression fragment -- use the `query` tool with $param bindings for dynamic values.",
		annotations(
			title = "Update records",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = false
		)
	)]
	async fn update(
		&self,
		Parameters(p): Parameters<crud::UpdateParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = p.scope.clone();
		self.dispatch_tool("update", &ctx, &scope, async |s| crud::update(s, p).await).await
	}

	#[tool(
		description = "DELETE records with an optional WHERE clause. `where_clause` is a SurrealQL expression fragment -- use the `query` tool with $param bindings for dynamic values.",
		annotations(
			title = "Delete records",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = true,
			open_world_hint = false
		)
	)]
	async fn delete(
		&self,
		Parameters(p): Parameters<crud::DeleteParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = p.scope.clone();
		self.dispatch_tool("delete", &ctx, &scope, async |s| crud::delete(s, p).await).await
	}

	#[tool(
		description = "RELATE records to create graph edges (from->table->to). Optional content is bound as a typed variable.",
		annotations(
			title = "Relate records",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = false
		)
	)]
	async fn relate(
		&self,
		Parameters(p): Parameters<crud::RelateParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = p.scope.clone();
		self.dispatch_tool("relate", &ctx, &scope, async |s| crud::relate(s, p).await).await
	}

	#[tool(
		description = "Dump full schema information for a scope. Target: 'root', 'ns', 'db', or a table name. Defaults to the most specific current context. Use `list` when you only need entities of one kind.",
		annotations(
			title = "Inspect schema",
			read_only_hint = true,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = false
		)
	)]
	async fn info(
		&self,
		Parameters(p): Parameters<schema::InfoParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = p.scope.clone();
		self.dispatch_tool("info", &ctx, &scope, async |s| schema::info(s, p).await).await
	}

	#[tool(
		description = "Enumerate schema entities of a single kind. `kind` is one of: namespaces, nodes, databases, tables, functions, analyzers, params, apis, buckets, models, modules, sequences, configs, users, accesses, fields, indexes, events. Set `table` for fields/indexes/events. Set `scope` (root|ns|db) for users/accesses.",
		annotations(
			title = "List schema entities",
			read_only_hint = true,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = false
		)
	)]
	async fn list(
		&self,
		Parameters(p): Parameters<schema::ListParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = p.tool_scope.clone();
		self.dispatch_tool("list", &ctx, &scope, async |s| schema::list(s, p).await).await
	}

	#[tool(
		name = "use",
		description = "Switch the active namespace and/or database. At least one of `namespace` or `database` must be provided; both can be set in a single call. Returns the resolved context.",
		annotations(
			title = "Switch namespace/database",
			read_only_hint = false,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = false
		)
	)]
	async fn use_context(
		&self,
		Parameters(p): Parameters<connection::UseParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = ToolScope::default();
		// Dispatch even when the call cannot succeed, so the rejection is
		// audited and counted like any other invocation rather than vanishing.
		// The test runs inside the closure so it observes the same handshake
		// state `dispatch_tool` resolved `s` from; reading it beforehand would
		// let a handshake bound in between refuse a `use` that has somewhere to
		// apply.
		self.dispatch_tool("use", &ctx, &scope, async |s| {
			if self.is_stateless(&ctx) {
				return Ok(connection::use_unsupported_when_stateless());
			}
			connection::r#use(s, p).await
		})
		.await
	}

	#[tool(
		description = "Invoke a SurrealQL function (e.g. `math::sum`, `string::concat`, `fn::my_function`) with typed argument bindings. Arguments are bound natively; the function name is restricted to `identifier(::identifier)*`. Permissions and capabilities are enforced by SurrealDB.",
		annotations(
			title = "Run function",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = false
		)
	)]
	async fn run(
		&self,
		Parameters(p): Parameters<run_tool::RunParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = p.scope.clone();
		self.dispatch_tool("run", &ctx, &scope, async |s| run_tool::run(s, p).await).await
	}

	#[tool(
		description = "Execute a GQL (ISO/IEC 39075) query or mutation with optional parameter bindings, e.g. `MATCH (p:person) RETURN p.name AS name`. Mutation statements (INSERT, SET, REMOVE, DELETE) are supported and may modify or delete data.",
		annotations(
			title = "Run GQL",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = false
		)
	)]
	async fn gql(
		&self,
		Parameters(p): Parameters<gql::GqlParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = p.scope.clone();
		self.dispatch_tool("gql", &ctx, &scope, async |s| gql::execute(s, p).await).await
	}

	#[tool(
		description = "Execute a GraphQL query or mutation against the active namespace and database. The database must have a `DEFINE CONFIG GRAPHQL` statement. Provide GraphQL variables as a JSON object; returns the GraphQL `{ data, errors }` response envelope (GraphQL execution errors appear in `errors`, not as a tool error).",
		annotations(
			title = "Run GraphQL",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = false
		)
	)]
	async fn graphql(
		&self,
		Parameters(p): Parameters<graphql::GraphqlParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<CallToolResult, McpError> {
		let scope = p.scope.clone();
		self.dispatch_tool("graphql", &ctx, &scope, async |s| graphql::execute(s, p).await).await
	}
}

// ---------------------------------------------------------------------------
// ServerHandler -- wires tools, resources, prompts, completions
// ---------------------------------------------------------------------------

#[tool_handler]
impl ServerHandler for McpService {
	fn get_info(&self) -> ServerInfo {
		ServerInfo::new(
			ServerCapabilities::builder()
				.enable_tools()
				.enable_resources()
				.enable_prompts()
				.enable_completions()
				.build(),
		)
		.with_protocol_version(ADVERTISED_PROTOCOL_VERSION)
		// Named explicitly rather than via `Implementation::from_build_env()`,
		// which resolves `CARGO_CRATE_NAME` inside rmcp and so reports the SDK
		// as the server. MCP clients surface this name to users.
		.with_server_info(
			Implementation::new("surrealdb", env!("CARGO_PKG_VERSION")).with_title("SurrealDB"),
		)
		.with_instructions(resources::instructions::get_instructions().to_string())
	}

	fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
		std::borrow::Cow::Borrowed(SUPPORTED_PROTOCOL_VERSIONS)
	}

	#[tracing::instrument(skip_all, target = "surrealdb::mcp")]
	async fn initialize(
		&self,
		_request: InitializeRequestParams,
		ctx: RequestContext<RoleServer>,
	) -> Result<InitializeResult, McpError> {
		let mut session = ctx
			.extensions
			.get::<http::request::Parts>()
			.and_then(crate::auth::extract_session_from_parts)
			.unwrap_or_else(|| {
				tracing::debug!(
					target: LOG,
					"No session in request context, using configured base session"
				);
				self.base_session.clone()
			});

		if session.ns.is_none()
			&& let Some(ns) = &self.default_ns
		{
			session.ns = Some(ns.clone());
		}
		if session.db.is_none()
			&& let Some(db) = &self.default_db
		{
			session.db = Some(db.clone());
		}

		self.init_session(session)?;
		tracing::info!(target: LOG, "MCP session initialized");
		Ok(self.get_info())
	}

	/// Written out rather than left to `#[tool_handler]` so `tools/list` runs
	/// the same subject check as every sibling method. The generated version
	/// answers from the router without consulting the binding, which makes
	/// possession of an `mcp-session-id` alone enough to confirm the id is live —
	/// the exact inference the binding exists to deny.
	///
	/// Everything below the check reproduces the generated body exactly,
	/// including the cache hints the sessionless revision added and the
	/// `Self::tool_router()` call. That call builds a *fresh* router, which is
	/// not the same value as [`Self::tool_router`]: the field additionally
	/// carries the output schemas [`crate::tools::output_schemas::attach`]
	/// applies, and those schemas do not currently describe what the tools
	/// return. Reading the field here would start advertising them and make
	/// every SDK client reject the responses, so this must keep calling the
	/// associated function until the schemas are corrected.
	async fn list_tools(
		&self,
		_: Option<PaginatedRequestParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<ListToolsResult, McpError> {
		self.verify_request_subject(&ctx)?;
		let supports_cache_hints = Self::claims_sessionless_revision(&ctx);
		Ok(ListToolsResult {
			result_type: Some(ResultType::COMPLETE),
			tools: Self::tool_router().list_all(),
			meta: None,
			next_cursor: None,
			ttl_ms: supports_cache_hints.then_some(0),
			cache_scope: supports_cache_hints.then_some(CacheScope::Public),
		})
	}

	async fn list_resources(
		&self,
		_: Option<PaginatedRequestParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<ListResourcesResult, McpError> {
		self.verify_request_subject(&ctx)?;
		Ok(ListResourcesResult::with_all_items(resources::list_resources()))
	}

	async fn list_resource_templates(
		&self,
		_: Option<PaginatedRequestParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<ListResourceTemplatesResult, McpError> {
		self.verify_request_subject(&ctx)?;
		Ok(ListResourceTemplatesResult::with_all_items(resources::list_resource_templates()))
	}

	async fn read_resource(
		&self,
		request: ReadResourceRequestParams,
		ctx: RequestContext<RoleServer>,
	) -> Result<ReadResourceResponse, McpError> {
		self.verify_request_subject(&ctx)?;
		Ok(resources::read_resource(self.session()?, &request.uri).await?.into())
	}

	async fn list_prompts(
		&self,
		_: Option<PaginatedRequestParams>,
		ctx: RequestContext<RoleServer>,
	) -> Result<ListPromptsResult, McpError> {
		self.verify_request_subject(&ctx)?;
		Ok(ListPromptsResult::with_all_items(prompts::list_prompts()))
	}

	async fn get_prompt(
		&self,
		request: GetPromptRequestParams,
		ctx: RequestContext<RoleServer>,
	) -> Result<GetPromptResponse, McpError> {
		self.verify_request_subject(&ctx)?;
		// `request.arguments` is already a `serde_json::Map<String, Value>`,
		// so we can wrap it directly into a `Value::Object` without a
		// fallible `to_value` round-trip. When absent, downstream prompt
		// handlers treat `Value::Null` as "no arguments".
		let args = request
			.arguments
			.as_ref()
			.map(|m| serde_json::Value::Object(m.clone()))
			.unwrap_or(serde_json::Value::Null);
		prompts::get_prompt(&request.name, &args).map(Into::into).ok_or_else(|| {
			McpError::invalid_params(format!("Unknown prompt: {}", request.name), None)
		})
	}

	async fn complete(
		&self,
		request: CompleteRequestParams,
		ctx: RequestContext<RoleServer>,
	) -> Result<CompleteResult, McpError> {
		self.verify_request_subject(&ctx)?;
		Ok(completions::handle_completion(self.session()?, &request).await)
	}
}

// ---------------------------------------------------------------------------
// Stdio transport
// ---------------------------------------------------------------------------

#[cfg(feature = "transport-io")]
pub use stdio_service::*;

#[cfg(feature = "transport-io")]
mod stdio_service {
	use super::*;

	/// Serve the MCP server over stdio (stdin/stdout).
	pub async fn serve_stdio(service: McpService) -> Result<(), anyhow::Error> {
		let stdin = tokio::io::stdin();
		let stdout = tokio::io::stdout();
		rmcp::ServiceExt::serve(service, (stdin, stdout))
			.await
			.map_err(|e| anyhow::anyhow!("MCP stdio error: {e}"))?
			.waiting()
			.await
			.map_err(|e| anyhow::anyhow!("MCP stdio error: {e}"))?;
		Ok(())
	}
}

// ---------------------------------------------------------------------------
// HTTP service factory
// ---------------------------------------------------------------------------

#[cfg(feature = "server-http")]
pub use http_service::*;

#[cfg(feature = "server-http")]
mod http_service {
	use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
	use rmcp::transport::streamable_http_server::{
		StreamableHttpServerConfig, StreamableHttpService,
	};

	use super::*;

	/// The fully-typed MCP HTTP service.
	pub type McpHttpService = StreamableHttpService<McpService, LocalSessionManager>;

	/// Create a `StreamableHttpService` backed by the given datastore.
	///
	/// The HTTP transport always runs behind the `SurrealAuth` middleware,
	/// which attaches an authenticated `Session` to the request extensions.
	/// If that extraction fails for any reason we fall back to the anonymous
	/// `Session::default()` -- the datastore's capability rules then decide
	/// whether guest access is allowed.
	pub fn create_http_service(ds: Arc<Datastore>) -> McpHttpService {
		create_http_service_with_metrics(ds, None)
	}

	/// Variant of [`create_http_service`] that wires an
	/// [`McpMetricsRecorder`] into every per-session [`McpService`] so
	/// embedders running their own metric pipeline (the SurrealDB server)
	/// can record `surrealdb.mcp.tool.*` instruments.
	pub fn create_http_service_with_metrics(
		ds: Arc<Datastore>,
		metrics_recorder: Option<Arc<dyn McpMetricsRecorder>>,
	) -> McpHttpService {
		create_http_service_with_config(ds, metrics_recorder, McpConfig::from_env())
	}

	/// Variant of [`create_http_service_with_metrics`] that takes an
	/// explicit [`McpConfig`] instead of reading the `SURREAL_MCP_*`
	/// environment. Embedders with their own configuration source -- and
	/// tests that need to exercise the `Host`-header allowlist without
	/// mutating process-global env state -- construct the service this way.
	///
	/// The same config drives both the transport-level DNS-rebinding host
	/// guard (via [`apply_host_policy`]) and every per-session
	/// [`McpService`]'s runtime caps.
	pub fn create_http_service_with_config(
		ds: Arc<Datastore>,
		metrics_recorder: Option<Arc<dyn McpMetricsRecorder>>,
		mcp_config: Arc<McpConfig>,
	) -> McpHttpService {
		let mut config = StreamableHttpServerConfig::default();
		config.legacy_session_mode = true;
		apply_host_policy(&mut config, &mcp_config);
		StreamableHttpService::new(
			move || {
				let svc =
					McpServiceConfig::new_with_config(Arc::clone(&ds), Arc::clone(&mcp_config))
						.with_transport_label("http");
				let svc = match metrics_recorder.clone() {
					Some(rec) => svc.with_metrics_recorder(rec),
					None => svc,
				};
				Ok(svc.build())
			},
			Arc::new(LocalSessionManager::default()),
			config,
		)
	}

	/// Map SurrealDB's configured host policy onto rmcp's transport config.
	///
	/// rmcp defaults `allowed_hosts` to the loopback set (`localhost`,
	/// `127.0.0.1`, `::1`) to prevent DNS-rebinding; that default rejects
	/// every request carrying a public `Host` with
	/// `403 Forbidden: Host header is not allowed`, before any SurrealDB
	/// logic runs. The mapping:
	///
	/// - `allow_all_hosts` -> clear the list. rmcp treats an empty list as "allow any `Host`"; this
	///   is the opt-in escape hatch and it wins over `allowed_hosts`.
	/// - non-empty `allowed_hosts` -> replace the loopback default with the operator's exact
	///   hostnames.
	/// - otherwise -> leave rmcp's safe loopback default in place.
	fn apply_host_policy(config: &mut StreamableHttpServerConfig, mcp: &McpConfig) {
		if mcp.allow_all_hosts {
			config.allowed_hosts.clear();
		} else if !mcp.allowed_hosts.is_empty() {
			config.allowed_hosts.clone_from(&mcp.allowed_hosts);
		}
	}

	#[cfg(test)]
	mod host_policy_tests {
		use super::*;

		fn mk_cfg(allowed_hosts: &[&str], allow_all_hosts: bool) -> McpConfig {
			McpConfig {
				allowed_hosts: allowed_hosts.iter().map(|s| s.to_string()).collect(),
				allow_all_hosts,
				..McpConfig::default()
			}
		}

		#[test]
		fn default_leaves_rmcp_loopback_allowlist_untouched() {
			let mut c = StreamableHttpServerConfig::default();
			let before = c.allowed_hosts.clone();
			apply_host_policy(&mut c, &mk_cfg(&[], false));
			// Unset config is a no-op: rmcp's (non-empty, restrictive)
			// loopback default is exactly what we must preserve.
			assert_eq!(c.allowed_hosts, before);
			assert!(!c.allowed_hosts.is_empty());
		}

		#[test]
		fn explicit_list_replaces_loopback() {
			let mut c = StreamableHttpServerConfig::default();
			apply_host_policy(&mut c, &mk_cfg(&["a.example.com"], false));
			assert_eq!(c.allowed_hosts, vec!["a.example.com".to_string()]);
		}

		#[test]
		fn allow_all_clears_list_and_wins_over_list() {
			let mut c = StreamableHttpServerConfig::default();
			apply_host_policy(&mut c, &mk_cfg(&["a.example.com"], true));
			assert!(c.allowed_hosts.is_empty());
		}
	}
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
	use super::*;

	async fn fresh_datastore() -> Arc<Datastore> {
		Datastore::new("memory").await.expect("memory datastore")
	}

	/// Locks in the construction-time invariant that
	/// [`McpService::verify_request_subject`]'s stdio bypass depends on:
	/// new services default to the stdio transport label and report
	/// themselves as stdio.
	#[tokio::test]
	async fn new_service_defaults_to_stdio_transport() {
		let ds = fresh_datastore().await;
		let svc = McpService::new(ds, None, None, Session::default());
		assert!(svc.is_stdio_transport(), "default transport must be stdio");
		assert_eq!(svc.transport_label, STDIO_TRANSPORT_LABEL);
	}

	/// Locks in the contract that any non-`"stdio"` label opts out of
	/// the stdio bypass and into the strict subject check. This is the
	/// invariant the HTTP factory relies on.
	#[tokio::test]
	async fn with_transport_label_http_opts_into_strict_check() {
		let ds = fresh_datastore().await;
		let svc = McpService::new(ds, None, None, Session::default()).with_transport_label("http");
		assert!(!svc.is_stdio_transport(), "http transport must NOT bypass the strict check");
		assert_eq!(svc.transport_label, "http");
	}

	/// Same contract via the [`McpServiceConfig`] builder, exercised by
	/// the in-tree HTTP factory.
	#[tokio::test]
	async fn config_with_transport_label_http_opts_into_strict_check() {
		let ds = fresh_datastore().await;
		let svc = McpServiceConfig::new(ds).with_transport_label("http").build();
		assert!(!svc.is_stdio_transport());
		assert_eq!(svc.transport_label, "http");
	}

	/// Defensive: an unknown / custom label must default to "treat as
	/// networked transport" so a future embedder that forgets to wire
	/// the strict check is secure-by-default, not silently bypassed.
	#[tokio::test]
	async fn custom_transport_label_is_not_treated_as_stdio() {
		let ds = fresh_datastore().await;
		let svc =
			McpService::new(ds, None, None, Session::default()).with_transport_label("custom-bus");
		assert!(!svc.is_stdio_transport());
	}

	/// `initialize` publishes the session and the subject in one write, so no
	/// reader can observe a service holding a session without the subject that
	/// opened it — the state in which
	/// [`McpService::verify_request_subject`] would take its sessionless bypass
	/// while a subject was still there to impersonate. The single
	/// [`BoundHandshake`] cell is what makes that unrepresentable; this pins the
	/// observable half, including that an authenticated handshake binds a
	/// *non-anonymous* subject — an anonymous binding makes
	/// [`crate::auth::check_subject`] accept every replay.
	#[tokio::test]
	async fn init_session_publishes_handshake_atomically() {
		let ds = fresh_datastore().await;
		let svc = McpService::new(ds, None, None, Session::default()).with_transport_label("http");
		assert!(svc.handshake.get().is_none(), "a fresh service holds no handshake");
		assert!(svc.session().is_err(), "a fresh service has no session to serve from");

		svc.init_session(Session::owner()).expect("init_session");
		let bound = svc.handshake.get().expect("init_session must bind the handshake");
		assert!(svc.session().is_ok(), "the bound session must be servable");
		assert!(
			!bound.subject.is_anonymous(),
			"an owner handshake must bind a non-anonymous subject"
		);
		assert!(
			svc.init_session(Session::owner()).is_err(),
			"a second handshake on one service is a protocol error"
		);
	}
}
