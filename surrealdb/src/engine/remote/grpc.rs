//! The gRPC engine.
//!
//! This engine implements [`SurrealEngine`] directly against the generated
//! `SurrealDBService` client: each method is its own RPC, and nothing here
//! constructs a [`Command`](surrealdb_engine_api::Command) or a
//! [`Route`](surrealdb_engine_api::Route) -- those belong to the engines that
//! are driven by a route channel, which this one is not.
//!
//! HTTP/2 multiplexes concurrent requests over one connection, so there is no
//! single consumer task marshalling requests: every call dispatches on its
//! own clone of the client, which is cheap and safe to use concurrently.
//!
//! Session lifetime is the one thing that does need ordering, because a
//! caller can issue commands on a cloned session before that clone has been
//! established server-side. [`session_task`] consumes those events in order,
//! and every request waits for its session to be ready before dispatching --
//! the same happens-before the WebSocket engine gets by draining its session
//! channel before each route.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use surrealdb_engine_api::{
	DbExportConfig, EngineContext, EngineFuture, MlExportConfig, SurrealEngine,
};
use surrealdb_protocol::proto::rpc::v1 as rpc;
use surrealdb_protocol::proto::rpc::v1::surreal_db_service_client::SurrealDbServiceClient;
use surrealdb_protocol::proto::v1 as proto;
use surrealdb_rpc::{QueryResult, QueryType, Token};
use tokio::sync::Notify;
use tonic::transport::Channel;
use uuid::Uuid;

use crate::types::{Action, Array, Notification, Object, SerializationError, Value, Variables};
use crate::{Error, ExtraFeatures, SessionId};

type EngineResult<T> = crate::Result<T>;

/// The gRPC scheme used to connect to `grpc://` endpoints.
#[derive(Debug)]
pub struct Grpc;

/// The gRPCS scheme used to connect to `grpcs://` endpoints.
#[derive(Debug)]
pub struct Grpcs;

/// A gRPC client for communicating with the server over `SurrealDBService`.
#[derive(Debug, Clone)]
pub struct Client(());

impl crate::Connection for Client {}
impl crate::conn::Sealed for Client {
	#[allow(private_interfaces)]
	fn connect(
		address: crate::opt::Endpoint,
		_capacity: usize,
		session_clone: Option<crate::SessionClone>,
	) -> crate::method::BoxFuture<'static, crate::Result<crate::Surreal<Self>>> {
		Box::pin(async move {
			let session_clone = session_clone.unwrap_or_else(crate::SessionClone::new);
			let (engine, features) =
				connect_engine(&address, session_clone.receiver.clone()).await?;
			let router = crate::conn::Router::from_engine(engine, features, address.config);
			let waiter = tokio::sync::watch::channel(Some(crate::opt::WaitFor::Connection));
			Ok((router, waiter, session_clone).into())
		})
	}
}

/// Opens a gRPC connection and starts the task that keeps its sessions in
/// step with the SDK's.
///
/// Shared by this engine's own `Sealed::connect` and by the `Any` engine.
pub(crate) async fn connect_engine(
	address: &crate::opt::Endpoint,
	session_rx: async_channel::Receiver<SessionId>,
) -> crate::Result<(Arc<dyn SurrealEngine>, std::collections::HashSet<ExtraFeatures>)> {
	// tonic speaks HTTP/2 and does not recognise the `grpc`/`grpcs` schemes
	// this SDK exposes to callers.
	//
	// The scheme is swapped textually rather than with `Url::set_scheme`,
	// which refuses to move a URL between a non-special scheme (`grpc`) and a
	// special one (`http`).
	let is_tls = address.url.scheme() == "grpcs";
	let scheme = if is_tls {
		"https"
	} else {
		"http"
	};
	let dst = match address.url.as_str().split_once("://") {
		Some((_, rest)) => format!("{scheme}://{rest}"),
		None => {
			return Err(Error::configuration(
				format!("Invalid gRPC endpoint: {}", address.url),
				None,
			));
		}
	};

	#[cfg_attr(not(feature = "rustls"), expect(unused_mut))]
	let mut builder = tonic::transport::Endpoint::from_shared(dst)
		.map_err(|e| Error::configuration(e.to_string(), None))?;

	// A caller-supplied TLS configuration cannot be honoured here: tonic builds
	// its own client configuration and offers no way to adopt a `rustls`
	// one, so a private certificate authority, a client certificate or a custom
	// verifier would all be silently ignored -- and a connection the caller
	// expected to be pinned would instead trust the public roots. Refusing is
	// the only answer that does not weaken what was asked for.
	#[cfg(any(feature = "native-tls", feature = "rustls"))]
	if address.config.tls_config.is_some() {
		return Err(Error::configuration(
			"A custom TLS configuration is not supported over `grpcs://`; \
			 the connection would fall back to the public roots instead"
				.to_string(),
			None,
		));
	}

	if is_tls {
		#[cfg(feature = "rustls")]
		{
			builder = builder
				.tls_config(tonic::transport::ClientTlsConfig::new().with_webpki_roots())
				.map_err(|e| Error::configuration(e.to_string(), None))?;
		}
		#[cfg(not(feature = "rustls"))]
		return Err(Error::configuration(
			"Connecting over `grpcs://` requires the `rustls` feature".to_string(),
			None,
		));
	}

	let channel = builder.connect().await.map_err(|e| {
		Error::connection(e.to_string(), crate::types::ConnectionError::ConnectionFailed)
	})?;
	let mut client = SurrealDbServiceClient::new(channel);
	let capabilities = fetch_capabilities(&mut client).await?;
	// Take the message size from the server rather than leaving tonic's 4 MiB
	// default in place: the server splits a query's records across frames, but
	// a single record larger than the default would still be undecodable, and
	// the limit the operator configured is the one that applies.
	if let Some(limit) = capabilities.limits.as_ref().map(|limits| limits.max_message_bytes)
		&& let Ok(limit) = usize::try_from(limit)
		&& limit > 0
	{
		client = client.max_decoding_message_size(limit).max_encoding_message_size(limit);
	}

	let features = extra_features(&capabilities);
	let engine = Arc::new(GrpcEngine {
		client,
		server_version: capabilities.server_version,
		sessions: SessionRegistry::default(),
		query_timeout: address
			.config
			.query_timeout
			.map(proto::Duration::try_from)
			.transpose()
			.map_err(|e| Error::configuration(format!("Invalid query timeout: {e}"), None))?,
	});
	tokio::spawn(session_task(Arc::clone(&engine), session_rx));
	Ok((engine, features))
}

/// Translates the server's reported capabilities into what the SDK gates on.
///
/// A server that reports no capability names at all predates them being
/// populated, and is assumed to support both rather than having features
/// silently disabled.
fn extra_features(
	capabilities: &rpc::ServerCapabilities,
) -> std::collections::HashSet<ExtraFeatures> {
	const EXPORT: &str = "surrealdb.protocol.rpc.v1.SurrealDBService/ExportSurql";
	let denied = |method: &str| capabilities.denied_methods.iter().any(|m| m == method);
	let mut features = std::collections::HashSet::new();
	if !denied(EXPORT) {
		// SurrealQL export and import are core RPCs rather than a named
		// capability, so they are assumed unless the operator denies them.
		features.insert(ExtraFeatures::Backup);
	}
	let reports_any = !capabilities.capabilities.is_empty();
	if !reports_any || capabilities.capabilities.iter().any(|c| c == "LIVE_QUERIES") {
		features.insert(ExtraFeatures::LiveQueries);
	}
	features
}

async fn fetch_capabilities(
	client: &mut SurrealDbServiceClient<Channel>,
) -> crate::Result<rpc::ServerCapabilities> {
	let request = rpc::GetCapabilitiesRequest {
		context: None,
		client: Some(rpc::ClientInfo {
			name: "surrealdb-rust".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			platform: "rust".to_string(),
			metadata: Vec::new(),
		}),
	};
	let capabilities = client
		.get_capabilities(request)
		.await
		.map_err(|status| {
			Error::connection(
				status.message().to_string(),
				crate::types::ConnectionError::ConnectionFailed,
			)
		})?
		.into_inner()
		.capabilities;
	capabilities.ok_or_else(|| {
		Error::connection(
			"Server did not report its capabilities".to_string(),
			crate::types::ConnectionError::ConnectionFailed,
		)
	})
}

/// An operation that must be re-applied to a cloned session.
///
/// The protocol has no "clone this session" call, so a clone is established
/// by attaching a fresh session and replaying the operations that shaped the
/// original. This mirrors which commands the WebSocket engine treats as
/// replayable (`RemoteCommand::replayable`).
#[derive(Debug, Clone)]
enum Replayable {
	Use {
		namespace: Option<String>,
		database: Option<String>,
	},
	Set {
		key: String,
		value: Value,
	},
	Unset {
		key: String,
	},
	Signin(Object),
	Signup(Object),
	Authenticate(Token),
	Invalidate,
}

impl Replayable {
	/// Whether this operation leaves `prev` with nothing left to contribute,
	/// so the log can carry this one in `prev`'s place.
	///
	/// A `Use` carries `None` for "leave unchanged", so it only subsumes an
	/// earlier one when it sets every field that one set. A bind subsumes an
	/// earlier bind or removal of the same variable, whichever way round.
	fn supersedes(&self, prev: &Self) -> bool {
		match (prev, self) {
			(
				Replayable::Use {
					namespace: pn,
					database: pd,
				},
				Replayable::Use {
					namespace: nn,
					database: nd,
				},
			) => (pn.is_none() || nn.is_some()) && (pd.is_none() || nd.is_some()),
			(
				Replayable::Set {
					key: previous,
					..
				}
				| Replayable::Unset {
					key: previous,
				},
				Replayable::Set {
					key: next,
					..
				}
				| Replayable::Unset {
					key: next,
				},
			) => previous == next,
			_ => false,
		}
	}

	/// Whether this operation may change what an earlier one set, which stops
	/// the search for something to coalesce with.
	///
	/// Authenticating, or dropping an authentication, re-establishes the
	/// session: what it leaves selected and bound is its business. Coalescing
	/// across one of these would move a later operation in front of it.
	fn resets_session_state(&self) -> bool {
		matches!(
			self,
			Replayable::Signin(_)
				| Replayable::Signup(_)
				| Replayable::Authenticate(_)
				| Replayable::Invalidate
		)
	}
}

/// A session's replay log and its readiness signal.
#[derive(Default)]
struct SessionEntry {
	ready: Notify,
	/// The session the server allocated for this one, or the failure that
	/// stopped it being established. `None` until `AttachSession` answers.
	///
	/// Session ids are the server's to mint -- it refuses to create one a
	/// client named -- so this is what every request for this session must
	/// carry, and the SDK's own session id is only a local handle onto it.
	server: Mutex<Option<EngineResult<Uuid>>>,
	replay: Mutex<Vec<Replayable>>,
}

impl SessionEntry {
	/// Waits until this session exists server-side, answering with the id the
	/// server gave it.
	///
	/// Follows `Notify`'s race-free pattern: the notified future is created
	/// *before* the readiness check, so a `notify_waiters()` landing between
	/// the check and the await is not missed. The loop covers a spurious
	/// wake-up, which would otherwise return before the session existed.
	async fn wait_ready(&self) -> EngineResult<Uuid> {
		loop {
			let notified = self.ready.notified();
			if let Some(result) = self.outcome() {
				return result;
			}
			notified.await;
		}
	}

	fn established(&self) -> std::sync::MutexGuard<'_, Option<EngineResult<Uuid>>> {
		self.server.lock().expect("session registry poisoned")
	}

	/// How establishing this session turned out, or `None` if it has not been
	/// attempted yet. Unlike [`wait_ready`](Self::wait_ready), this does not
	/// wait for the answer.
	fn outcome(&self) -> Option<EngineResult<Uuid>> {
		self.established().clone()
	}

	/// Publishes the outcome of establishing this session.
	///
	/// A failure is published rather than swallowed so that waiters fail with
	/// the reason the session was never established, instead of hanging or
	/// silently running against no session at all.
	fn mark_ready(&self, outcome: EngineResult<Uuid>) {
		*self.established() = Some(outcome);
		self.ready.notify_waiters();
	}

	/// Appends an operation to the replay log, coalescing it into the entry it
	/// supersedes.
	///
	/// The log is what a clone is rebuilt from, so it only has to describe the
	/// state the session reached -- not how many times it was set. Without
	/// this a session that re-selects a database or rebinds a variable per
	/// request grows its log for the connection's lifetime, and the first
	/// clone then replays every redundant entry as its own round trip.
	fn record(&self, op: Replayable) {
		let mut replay = self.replay.lock().expect("session registry poisoned");
		for previous in replay.iter_mut().rev() {
			// Stop before anything that re-establishes the session: replacing
			// an operation on the far side of one would move this operation in
			// front of it.
			if previous.resets_session_state() {
				break;
			}
			if op.supersedes(previous) {
				*previous = op;
				return;
			}
		}
		replay.push(op);
	}

	fn log(&self) -> Vec<Replayable> {
		self.replay.lock().expect("session registry poisoned").clone()
	}
}

#[derive(Default)]
struct SessionRegistry(Mutex<HashMap<Uuid, Arc<SessionEntry>>>);

impl SessionRegistry {
	fn entry(&self, id: Uuid) -> Arc<SessionEntry> {
		Arc::clone(self.0.lock().expect("session registry poisoned").entry(id).or_default())
	}

	fn remove(&self, id: Uuid) {
		self.0.lock().expect("session registry poisoned").remove(&id);
	}
}

struct GrpcEngine {
	client: SurrealDbServiceClient<Channel>,
	/// Reported once at connect; `version` answers from it rather than
	/// spending a round trip, as the protocol intends.
	server_version: String,
	sessions: SessionRegistry,
	/// Converted once at connect: the wire counts seconds in an `i64`, so the
	/// conversion is fallible, and a per-request context has nowhere to report
	/// a failure that the configuration caused.
	query_timeout: Option<proto::Duration>,
}

// Generated proto messages do not derive `Debug`, so this cannot either.
impl std::fmt::Debug for GrpcEngine {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("GrpcEngine").field("server_version", &self.server_version).finish()
	}
}

impl GrpcEngine {
	fn client(&self) -> SurrealDbServiceClient<Channel> {
		self.client.clone()
	}

	fn context(&self, session: Uuid, transaction: Option<Uuid>) -> rpc::RequestContext {
		rpc::RequestContext {
			session: Some(proto::Uuid::from_uuid(session)),
			transaction: transaction.map(proto::Uuid::from_uuid),
			timeout: self.query_timeout,
		}
	}

	/// Waits for a session to exist server-side, and builds the context a
	/// request on it carries.
	async fn ready(&self, ctx: EngineContext) -> EngineResult<rpc::RequestContext> {
		Ok(self.ready_session(ctx).await?.0)
	}

	/// As [`ready`](Self::ready), also handing back the session's registry
	/// entry.
	///
	/// An operation that shapes the session has to record itself in the same
	/// entry afterwards. Returning it here is what keeps that to one pass over
	/// the registry, whose lock every concurrent request contends for.
	async fn ready_session(
		&self,
		ctx: EngineContext,
	) -> EngineResult<(rpc::RequestContext, Arc<SessionEntry>)> {
		let entry = self.sessions.entry(ctx.session);
		let session = entry.wait_ready().await?;
		Ok((self.context(session, ctx.transaction), entry))
	}

	/// Attaches a session and replays `log` onto it, so the clone is a copy of
	/// the session it was made from.
	///
	/// A replay that fails fails the session: the alternative is a clone that
	/// runs unauthenticated, or in the wrong namespace, and reports errors
	/// naming neither. Only the operations that did apply are recorded, so the
	/// clone's own log describes the state it actually reached.
	async fn establish_clone(
		&self,
		entry: &SessionEntry,
		log: Vec<Replayable>,
	) -> EngineResult<Uuid> {
		let session = self.attach().await?;
		for op in log {
			self.apply(self.context(session, None), op.clone()).await?;
			entry.record(op);
		}
		Ok(session)
	}

	/// Publishes how establishing a session turned out.
	///
	/// Published either way: waiters must not hang, and a failure has to reach
	/// the request that goes on to need the session.
	fn publish(&self, entry: &SessionEntry, id: Uuid, established: EngineResult<Uuid>) {
		if let Err(error) = established.as_ref() {
			trace!("failed to establish session {id}: {error}");
		}
		entry.mark_ready(established);
	}

	async fn apply(&self, context: rpc::RequestContext, op: Replayable) -> EngineResult<()> {
		match op {
			Replayable::Use {
				namespace,
				database,
			} => {
				self.use_ns_db_inner(context, namespace, database).await?;
			}
			Replayable::Set {
				key,
				value,
			} => self.set_inner(context, key, value).await?,
			Replayable::Unset {
				key,
			} => self.unset_inner(context, key).await?,
			Replayable::Signin(credentials) => {
				self.signin_inner(context, credentials).await?;
			}
			Replayable::Signup(credentials) => {
				self.signup_inner(context, credentials).await?;
			}
			Replayable::Authenticate(token) => {
				self.authenticate_inner(context, token).await?;
			}
			Replayable::Invalidate => self.invalidate_inner(context).await?,
		}
		Ok(())
	}

	/// Asks the server for a session, which it allocates and names.
	///
	/// The request carries no session of its own: a server refuses to create
	/// a session under an id a client chose, so that an id cannot be squatted
	/// or pre-created. The id it answers with is what every subsequent request
	/// on this session carries.
	async fn attach(&self) -> EngineResult<Uuid> {
		let response = self
			.client()
			.attach_session(rpc::AttachSessionRequest {
				context: None,
			})
			.await
			.map_err(status_to_error)?
			.into_inner();
		response
			.session
			.ok_or_else(|| Error::internal("Server did not allocate a session".to_string()))?
			.to_uuid()
			.map_err(|e| Error::internal(e.to_string()))
	}

	async fn detach(&self, session: Uuid) -> EngineResult<()> {
		self.client()
			.detach_session(rpc::DetachSessionRequest {
				context: Some(self.context(session, None)),
			})
			.await
			.map_err(status_to_error)?;
		Ok(())
	}

	// The `_inner` methods carry the actual RPC so both the trait method and
	// the replay path can call them without recursing through `ready()`.

	async fn use_ns_db_inner(
		&self,
		context: rpc::RequestContext,
		namespace: Option<String>,
		database: Option<String>,
	) -> EngineResult<(Option<String>, Option<String>)> {
		let response = self
			.client()
			.r#use(rpc::UseRequest {
				context: Some(context),
				namespace: namespace.map(nullable),
				database: database.map(nullable),
			})
			.await
			.map_err(status_to_error)?
			.into_inner();
		// The protocol reports an empty string for "nothing selected".
		let opt = |s: String| (!s.is_empty()).then_some(s);
		Ok((opt(response.namespace), opt(response.database)))
	}

	async fn set_inner(
		&self,
		context: rpc::RequestContext,
		key: String,
		value: Value,
	) -> EngineResult<()> {
		self.client()
			.set_variable(rpc::SetVariableRequest {
				context: Some(context),
				name: key,
				value: Some(to_proto_value(value)?),
			})
			.await
			.map_err(status_to_error)?;
		Ok(())
	}

	async fn unset_inner(&self, context: rpc::RequestContext, key: String) -> EngineResult<()> {
		self.client()
			.unset_variable(rpc::UnsetVariableRequest {
				context: Some(context),
				name: key,
			})
			.await
			.map_err(status_to_error)?;
		Ok(())
	}

	async fn signin_inner(
		&self,
		context: rpc::RequestContext,
		credentials: Object,
	) -> EngineResult<Token> {
		let response = self
			.client()
			.signin(rpc::SigninRequest {
				context: Some(context),
				access_method: Some(access_method(credentials)?),
			})
			.await
			.map_err(status_to_error)?
			.into_inner();
		tokens_to_token(response.tokens)
	}

	async fn signup_inner(
		&self,
		context: rpc::RequestContext,
		credentials: Object,
	) -> EngineResult<Token> {
		let response = self
			.client()
			.signup(rpc::SignupRequest {
				context: Some(context),
				credentials: Some(record_credentials(credentials)?),
			})
			.await
			.map_err(status_to_error)?
			.into_inner();
		tokens_to_token(response.tokens)
	}

	async fn authenticate_inner(
		&self,
		context: rpc::RequestContext,
		token: Token,
	) -> EngineResult<Token> {
		let access = match &token {
			Token::Access(access) => access.clone(),
			Token::WithRefresh {
				access,
				..
			} => access.clone(),
		};
		let response = self
			.client()
			.authenticate(rpc::AuthenticateRequest {
				context: Some(context),
				token: access,
			})
			.await
			.map_err(status_to_error)?
			.into_inner();
		// A server may authenticate the session with a token other than the one
		// presented, so what it reports wins. Only a server that reports none
		// leaves the supplied token as the one in effect.
		match response.tokens {
			Some(_) => tokens_to_token(response.tokens),
			None => Ok(token),
		}
	}

	async fn invalidate_inner(&self, context: rpc::RequestContext) -> EngineResult<()> {
		self.client()
			.invalidate(rpc::InvalidateRequest {
				context: Some(context),
			})
			.await
			.map_err(status_to_error)?;
		Ok(())
	}

	async fn query_inner(
		&self,
		context: rpc::RequestContext,
		query: String,
		variables: Variables,
	) -> EngineResult<Vec<QueryResult>> {
		let mut stream = self
			.client()
			.query(rpc::QueryRequest {
				context: Some(context),
				query,
				variables: Some(to_proto_variables(variables)?),
				// Empty means row-oriented values only, which is all this
				// engine can decode.
				accepted_encodings: Vec::new(),
			})
			.await
			.map_err(status_to_error)?
			.into_inner();

		let mut statements: Vec<Statement> = Vec::new();
		let mut ended = false;
		while let Some(response) = stream.message().await.map_err(status_to_error)? {
			match response.frame {
				Some(rpc::query_response::Frame::Begin(begin)) => {
					statements = Vec::new();
					grow_statements(&mut statements, begin.result_count as usize)?;
				}
				Some(rpc::query_response::Frame::Batch(batch)) => {
					let index = batch.query_index as usize;
					// A server may report more statements than `Begin`
					// announced; grow rather than drop their results.
					if index >= statements.len() {
						grow_statements(&mut statements, index + 1)?;
					}
					statements[index].push(batch);
				}
				Some(rpc::query_response::Frame::End(_)) => {
					ended = true;
					break;
				}
				// A stream-level error is not attributable to one statement,
				// so it fails the whole query.
				Some(rpc::query_response::Frame::Error(error)) => {
					return Err(proto_error(error));
				}
				None => {
					return Err(Error::internal(
						"Query stream carried an unrecognised frame".to_string(),
					));
				}
			}
		}
		// `End` is what marks a query stream complete. A stream that stops
		// before it must be reported, not answered: the statements collected
		// so far would otherwise read as the whole result, and a `SELECT`
		// truncated part-way would look like a table with fewer rows in it.
		if !ended {
			return Err(Error::connection(
				"The query ended before it was complete".to_string(),
				crate::types::ConnectionError::ConnectionFailed,
			));
		}
		Ok(statements.into_iter().map(Statement::finish).collect())
	}
}

/// Reports a value that arrived in a shape this build cannot decode.
fn deserialization_error(error: impl std::fmt::Display) -> Error {
	Error::serialization(error.to_string(), SerializationError::Deserialization)
}

/// The most statements one query stream may report.
///
/// The statement count and the per-batch statement index both arrive from the
/// peer, and both size an allocation. A query is a `;`-separated script, so
/// this is far above anything a caller can write while still refusing a count
/// that would size an allocation by itself.
const MAX_STATEMENTS: usize = 1 << 20;

/// Grows the per-statement accumulators to `len`, refusing a length no query
/// could legitimately produce.
fn grow_statements(statements: &mut Vec<Statement>, len: usize) -> EngineResult<()> {
	if len > MAX_STATEMENTS {
		return Err(Error::internal(format!(
			"The server reported {len} statement results, more than the {MAX_STATEMENTS} a query may have"
		)));
	}
	statements.resize_with(len, Statement::default);
	Ok(())
}

/// Consumes session lifecycle events in order: attach a new session, attach
/// and replay a cloned one, detach a dropped one.
async fn session_task(engine: Arc<GrpcEngine>, session_rx: async_channel::Receiver<SessionId>) {
	while let Ok(event) = session_rx.recv().await {
		match event {
			SessionId::Initial(id) => {
				let entry = engine.sessions.entry(id);
				engine.publish(&entry, id, engine.attach().await);
			}
			SessionId::Clone {
				old,
				new,
			} => {
				let log = engine.sessions.entry(old).log();
				let entry = engine.sessions.entry(new);
				let established = engine.establish_clone(&entry, log).await;
				engine.publish(&entry, new, established);
			}
			SessionId::Drop(id) => {
				// Only a session that was established has anything to release.
				// Read the outcome rather than waiting for it: the event that
				// establishes a session is consumed from this same channel
				// first, so it is already there -- and waiting here would stall
				// every later session event if it somehow were not.
				match engine.sessions.entry(id).outcome() {
					Some(Ok(session)) => {
						if let Err(error) = engine.detach(session).await {
							trace!("failed to detach session {id}: {error}");
						}
					}
					Some(Err(error)) => trace!("session {id} was never attached: {error}"),
					None => trace!("session {id} was dropped before it was established"),
				}
				engine.sessions.remove(id);
			}
		}
	}
}

impl SurrealEngine for GrpcEngine {
	fn query(
		&self,
		ctx: EngineContext,
		query: std::borrow::Cow<'static, str>,
		variables: Variables,
	) -> EngineFuture<'_, Vec<QueryResult>> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			self.query_inner(context, query.into_owned(), variables).await
		})
	}

	fn run(
		&self,
		ctx: EngineContext,
		name: String,
		version: Option<String>,
		args: Array,
	) -> EngineFuture<'_, Value> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			let args = args.into_iter().map(to_proto_value).collect::<EngineResult<Vec<_>>>()?;
			let response = self
				.client()
				.run(rpc::RunRequest {
					context: Some(context),
					name,
					// The wire spells "no version" as an empty string.
					version: version.unwrap_or_default(),
					args,
				})
				.await
				.map_err(status_to_error)?
				.into_inner();
			match response.result {
				Some(value) => Value::try_from(value).map_err(deserialization_error),
				None => Ok(Value::None),
			}
		})
	}

	fn use_ns_db(
		&self,
		ctx: EngineContext,
		namespace: Option<String>,
		database: Option<String>,
	) -> EngineFuture<'_, (Option<String>, Option<String>)> {
		Box::pin(async move {
			let (context, session) = self.ready_session(ctx).await?;
			let selection =
				self.use_ns_db_inner(context, namespace.clone(), database.clone()).await?;
			session.record(Replayable::Use {
				namespace,
				database,
			});
			Ok(selection)
		})
	}

	fn set(&self, ctx: EngineContext, key: String, value: Value) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let (context, session) = self.ready_session(ctx).await?;
			self.set_inner(context, key.clone(), value.clone()).await?;
			session.record(Replayable::Set {
				key,
				value,
			});
			Ok(())
		})
	}

	fn unset(&self, ctx: EngineContext, key: String) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let (context, session) = self.ready_session(ctx).await?;
			self.unset_inner(context, key.clone()).await?;
			session.record(Replayable::Unset {
				key,
			});
			Ok(())
		})
	}

	fn signup(&self, ctx: EngineContext, credentials: Object) -> EngineFuture<'_, Token> {
		Box::pin(async move {
			let (context, session) = self.ready_session(ctx).await?;
			let token = self.signup_inner(context, credentials.clone()).await?;
			session.record(Replayable::Signup(credentials));
			Ok(token)
		})
	}

	fn signin(&self, ctx: EngineContext, credentials: Object) -> EngineFuture<'_, Token> {
		Box::pin(async move {
			let (context, session) = self.ready_session(ctx).await?;
			let token = self.signin_inner(context, credentials.clone()).await?;
			session.record(Replayable::Signin(credentials));
			Ok(token)
		})
	}

	fn authenticate(&self, ctx: EngineContext, token: Token) -> EngineFuture<'_, Token> {
		Box::pin(async move {
			let (context, session) = self.ready_session(ctx).await?;
			let token = self.authenticate_inner(context, token).await?;
			session.record(Replayable::Authenticate(token.clone()));
			Ok(token)
		})
	}

	fn refresh(&self, ctx: EngineContext, token: Token) -> EngineFuture<'_, Token> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			let Token::WithRefresh {
				access,
				refresh,
			} = token
			else {
				return Err(Error::validation(
					"This token carries no refresh token".to_string(),
					None,
				));
			};
			// Both halves travel: the exchange reads the expired access token's
			// claims to recover the namespace, database and access method the
			// new pair is minted for.
			let response = self
				.client()
				.refresh_tokens(rpc::RefreshTokensRequest {
					context: Some(context),
					access,
					refresh,
				})
				.await
				.map_err(status_to_error)?
				.into_inner();
			tokens_to_token(response.tokens)
		})
	}

	fn revoke(&self, ctx: EngineContext, token: Token) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			let (access, refresh) = match token {
				Token::Access(access) => (access, String::new()),
				Token::WithRefresh {
					access,
					refresh,
				} => (access, refresh),
			};
			self.client()
				.revoke_tokens(rpc::RevokeTokensRequest {
					context: Some(context),
					access,
					refresh,
				})
				.await
				.map_err(status_to_error)?;
			Ok(())
		})
	}

	fn invalidate(&self, ctx: EngineContext) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let (context, session) = self.ready_session(ctx).await?;
			self.invalidate_inner(context).await?;
			session.record(Replayable::Invalidate);
			Ok(())
		})
	}

	fn begin(&self, ctx: EngineContext) -> EngineFuture<'_, Uuid> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			let response = self
				.client()
				.begin_transaction(rpc::BeginTransactionRequest {
					context: Some(context),
				})
				.await
				.map_err(status_to_error)?
				.into_inner();
			response
				.transaction
				.ok_or_else(|| {
					Error::internal("Server did not return a transaction id".to_string())
				})?
				.to_uuid()
				.map_err(|e| Error::internal(e.to_string()))
		})
	}

	fn commit(&self, ctx: EngineContext, txn: Uuid) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			// The transaction to finish is named by the request context, not
			// by a parameter, so it is threaded in here.
			let context =
				self.ready(EngineContext::with_transaction(ctx.session, Some(txn))).await?;
			self.client()
				.commit_transaction(rpc::CommitTransactionRequest {
					context: Some(context),
				})
				.await
				.map_err(status_to_error)?;
			Ok(())
		})
	}

	fn rollback(&self, ctx: EngineContext, txn: Uuid) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			// The transaction to finish is named by the request context, not
			// by a parameter, so it is threaded in here.
			let context =
				self.ready(EngineContext::with_transaction(ctx.session, Some(txn))).await?;
			self.client()
				.cancel_transaction(rpc::CancelTransactionRequest {
					context: Some(context),
				})
				.await
				.map_err(status_to_error)?;
			Ok(())
		})
	}

	fn health(&self, ctx: EngineContext) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			self.client()
				.health(rpc::HealthRequest {
					context: Some(context),
				})
				.await
				.map_err(status_to_error)?;
			Ok(())
		})
	}

	fn version(&self, _ctx: EngineContext) -> EngineFuture<'_, String> {
		Box::pin(async move { Ok(self.server_version.clone()) })
	}

	fn kill(&self, ctx: EngineContext, uuid: Uuid) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			self.client()
				.kill(rpc::KillRequest {
					context: Some(context),
					live_query_id: Some(proto::Uuid::from_uuid(uuid)),
				})
				.await
				.map_err(status_to_error)?;
			Ok(())
		})
	}

	fn subscribe_live(
		&self,
		ctx: EngineContext,
		uuid: Uuid,
		notifications: async_channel::Sender<Result<Notification, Error>>,
	) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			// The live query already exists -- `LIVE SELECT` ran as an
			// ordinary statement and returned this id -- so this attaches to
			// it rather than registering a new one. Ending this stream
			// therefore leaves the live query running, which is what `kill`
			// is for.
			let stream = self
				.client()
				.subscribe(rpc::SubscribeRequest {
					context: Some(context),
					resume_from: None,
					subscribe_to: Some(rpc::subscribe_request::SubscribeTo::LiveQueryId(
						proto::Uuid::from_uuid(uuid),
					)),
				})
				.await
				.map_err(status_to_error)?
				.into_inner();
			tokio::spawn(pump_notifications(stream, notifications, uuid));
			Ok(())
		})
	}

	fn export_file(
		&self,
		ctx: EngineContext,
		path: PathBuf,
		config: Option<DbExportConfig>,
	) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			let stream = self.export_surql(context, config).await?;
			write_export_to_file(stream, path).await
		})
	}

	fn export_bytes(
		&self,
		ctx: EngineContext,
		bytes: async_channel::Sender<Result<Vec<u8>, Error>>,
		config: Option<DbExportConfig>,
	) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			let stream = self.export_surql(context, config).await?;
			// The caller reads from the channel while the export streams, so
			// this must not block on draining it here.
			tokio::spawn(pump_export_to_channel(stream, bytes));
			Ok(())
		})
	}

	fn export_ml_file(
		&self,
		ctx: EngineContext,
		path: PathBuf,
		config: MlExportConfig,
	) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			let stream = self.export_ml(context, config).await?;
			write_export_to_file(stream, path).await
		})
	}

	fn export_ml_bytes(
		&self,
		ctx: EngineContext,
		bytes: async_channel::Sender<Result<Vec<u8>, Error>>,
		config: MlExportConfig,
	) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			let stream = self.export_ml(context, config).await?;
			tokio::spawn(pump_export_to_channel(stream, bytes));
			Ok(())
		})
	}

	fn import_file(&self, ctx: EngineContext, path: PathBuf) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			let file = tokio::fs::File::open(&path)
				.await
				.map_err(|e| Error::internal(format!("Failed to open {}: {e}", path.display())))?;
			self.client()
				.import_surql(import_stream(context, file))
				.await
				.map_err(status_to_error)?;
			Ok(())
		})
	}

	fn import_ml_file(&self, ctx: EngineContext, path: PathBuf) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let context = self.ready(ctx).await?;
			let file = tokio::fs::File::open(&path)
				.await
				.map_err(|e| Error::internal(format!("Failed to open {}: {e}", path.display())))?;
			// The model's name and version live in its own header, which the
			// server reads; `Begin` leaves them empty rather than guessing them
			// from the file's path.
			self.client()
				.import_ml_model(ml_import_stream(context, file))
				.await
				.map_err(status_to_error)?;
			Ok(())
		})
	}
}

/// The chunks of an export, which SurrealQL and ML-model exports share the
/// shape of.
///
/// Carries the `Bytes` the wire decoded into rather than a copy: the stream
/// ends when the export's trailer says it is complete, so a consumer that runs
/// the stream to its end has the whole export and one that stops early has an
/// error explaining why.
type ExportChunks =
	std::pin::Pin<Box<dyn futures::Stream<Item = Result<tonic::codegen::Bytes, Error>> + Send>>;

impl GrpcEngine {
	/// Starts a SurrealQL export, normalising its frames into chunks.
	async fn export_surql(
		&self,
		context: rpc::RequestContext,
		config: Option<DbExportConfig>,
	) -> EngineResult<ExportChunks> {
		let config = config.map(export_config).transpose()?;
		let stream = self
			.client()
			.export_surql(rpc::ExportSurqlRequest {
				context: Some(context),
				config,
			})
			.await
			.map_err(status_to_error)?
			.into_inner();
		Ok(export_chunks(stream))
	}

	/// Starts a machine-learning model export, normalising its frames into
	/// chunks.
	async fn export_ml(
		&self,
		context: rpc::RequestContext,
		config: MlExportConfig,
	) -> EngineResult<ExportChunks> {
		let stream = self
			.client()
			.export_ml_model(rpc::ExportMlModelRequest {
				context: Some(context),
				name: config.name,
				version: config.version,
			})
			.await
			.map_err(status_to_error)?
			.into_inner();
		Ok(export_chunks(stream))
	}
}

/// One frame of an export stream, with its wrapper message peeled off.
///
/// The SurrealQL and ML-model exports carry the same three frames in two
/// different wrappers, so unwrapping them is all that differs between them --
/// which is what [`ExportResponse`] answers.
enum ExportFrame {
	Chunk(tonic::codegen::Bytes),
	Trailer(rpc::DataTrailer),
	Error(proto::SurrealError),
}

/// A message an export streams, whichever export it is.
///
/// Implemented for each wrapper the protocol defines, so the framing below is
/// written once rather than per export RPC.
trait ExportResponse: Send + 'static {
	/// The frame this message carries, or `None` for one this build does not
	/// recognise.
	fn into_frame(self) -> Option<ExportFrame>;
}

/// Implements [`ExportResponse`] for one export's response message, whose
/// generated frame enum differs only in its module path.
macro_rules! export_response {
	($response:ty, $frame:path) => {
		impl ExportResponse for $response {
			fn into_frame(self) -> Option<ExportFrame> {
				use $frame as Frame;
				match self.frame? {
					Frame::Chunk(chunk) => Some(ExportFrame::Chunk(chunk.data)),
					Frame::Trailer(trailer) => Some(ExportFrame::Trailer(trailer)),
					Frame::Error(error) => Some(ExportFrame::Error(error)),
				}
			}
		}
	};
}

export_response!(rpc::ExportSurqlResponse, rpc::export_surql_response::Frame);
export_response!(rpc::ExportMlModelResponse, rpc::export_ml_model_response::Frame);

/// Normalises an export stream into the chunks it carries, failing the
/// transfer if it does not complete.
///
/// The trailer is what marks a byte stream complete, so a stream that ends
/// without one is a truncated transfer and must be reported as a failure: a
/// connection dropped part-way through an export otherwise produces a short
/// file that looks like a whole one. The trailer's declared length is checked
/// against what actually arrived.
///
/// The trailer's optional BLAKE3 digest is not verified. Doing so would mean a
/// hash implementation in the SDK, and the length check already catches the
/// failure the transport can produce -- a truncated stream -- while HTTP/2's
/// own framing covers corruption in transit.
fn export_chunks<T: ExportResponse>(stream: tonic::Streaming<T>) -> ExportChunks {
	/// How much of the export has arrived, and whether it is over.
	struct State<T> {
		stream: tonic::Streaming<T>,
		streamed: u64,
		/// Set once the stream has yielded its last item, so a consumer that
		/// polls on past the end gets nothing rather than a second error.
		finished: bool,
	}

	impl<T> State<T> {
		/// Ends the stream, reporting `error` as its last item.
		fn fail(mut self, error: Error) -> Option<(Result<tonic::codegen::Bytes, Error>, Self)> {
			self.finished = true;
			Some((Err(error), self))
		}

		fn truncated(
			self,
			message: String,
		) -> Option<(Result<tonic::codegen::Bytes, Error>, Self)> {
			self.fail(Error::connection(message, crate::types::ConnectionError::ConnectionFailed))
		}
	}

	Box::pin(futures::stream::unfold(
		State {
			stream,
			streamed: 0,
			finished: false,
		},
		|mut state| async move {
			if state.finished {
				return None;
			}
			let frame = match state.stream.message().await {
				Ok(Some(message)) => message.into_frame(),
				Ok(None) => {
					return state.truncated("The export ended before it was complete".to_string());
				}
				Err(status) => return state.fail(status_to_error(status)),
			};
			match frame {
				Some(ExportFrame::Chunk(chunk)) => {
					state.streamed += chunk.len() as u64;
					Some((Ok(chunk), state))
				}
				// The trailer is what marks the export complete, so the stream
				// ends here -- a consumer that reached this point has all of it.
				Some(ExportFrame::Trailer(trailer)) if trailer.bytes == state.streamed => None,
				Some(ExportFrame::Trailer(trailer)) => {
					let message = format!(
						"The export declared {} bytes but {} arrived",
						trailer.bytes, state.streamed
					);
					state.truncated(message)
				}
				Some(ExportFrame::Error(error)) => state.fail(proto_error(error)),
				None => state
					.fail(Error::internal("The export carried an unrecognised frame".to_string())),
			}
		},
	))
}

/// A message an import streams, whichever import it is.
///
/// Implemented for each wrapper the protocol defines, so the framing below is
/// written once rather than per import RPC. `Begin` is supplied by the caller
/// because only it knows what that import's opening frame carries.
trait ImportRequest: Send + 'static {
	fn chunk(data: tonic::codegen::Bytes) -> Self;
	fn trailer(trailer: rpc::DataTrailer) -> Self;
}

impl ImportRequest for rpc::ImportSurqlRequest {
	fn chunk(data: tonic::codegen::Bytes) -> Self {
		Self {
			frame: Some(rpc::import_surql_request::Frame::Chunk(rpc::DataChunk {
				data,
			})),
		}
	}

	fn trailer(trailer: rpc::DataTrailer) -> Self {
		Self {
			frame: Some(rpc::import_surql_request::Frame::Trailer(trailer)),
		}
	}
}

impl ImportRequest for rpc::ImportMlModelRequest {
	fn chunk(data: tonic::codegen::Bytes) -> Self {
		Self {
			frame: Some(rpc::import_ml_model_request::Frame::Chunk(rpc::DataChunk {
				data,
			})),
		}
	}

	fn trailer(trailer: rpc::DataTrailer) -> Self {
		Self {
			frame: Some(rpc::import_ml_model_request::Frame::Trailer(trailer)),
		}
	}
}

/// Frames a SurrealQL import: the opening frame carrying the context, then the
/// file's chunks, then a trailer stating what was sent.
fn import_stream(
	context: rpc::RequestContext,
	file: tokio::fs::File,
) -> impl futures::Stream<Item = rpc::ImportSurqlRequest> + Send + 'static {
	let begin = rpc::ImportSurqlRequest {
		frame: Some(rpc::import_surql_request::Frame::Begin(rpc::ImportSurqlBegin {
			context: Some(context),
		})),
	};
	frame_import(begin, file)
}

/// Frames a SurrealML import.
///
/// The opening frame names no model: the name and version a model is stored
/// under come from its own header, which the server reads, so guessing them
/// from the file's path here would only invite a disagreement.
fn ml_import_stream(
	context: rpc::RequestContext,
	file: tokio::fs::File,
) -> impl futures::Stream<Item = rpc::ImportMlModelRequest> + Send + 'static {
	let begin = rpc::ImportMlModelRequest {
		frame: Some(rpc::import_ml_model_request::Frame::Begin(rpc::ImportMlModelBegin {
			context: Some(context),
			name: String::new(),
			version: String::new(),
		})),
	};
	frame_import(begin, file)
}

/// Streams `begin`, then the file in chunks, then the trailer that completes it.
fn frame_import<T: ImportRequest>(
	begin: T,
	file: tokio::fs::File,
) -> impl futures::Stream<Item = T> + Send + 'static {
	use tokio::io::AsyncReadExt;

	enum Stage<T> {
		Begin(T, tokio::fs::File),
		Chunks(tokio::fs::File, u64),
		Done,
	}

	futures::stream::unfold(Stage::Begin(begin, file), |stage| async move {
		match stage {
			Stage::Begin(begin, file) => Some((begin, Stage::Chunks(file, 0))),
			Stage::Chunks(mut file, sent) => {
				// `read_buf` fills the spare capacity and sets the length, so
				// the buffer is never zeroed only to be overwritten by the
				// read, and needs no truncating afterwards.
				let mut buffer = Vec::with_capacity(surrealdb_protocol::DEFAULT_FILE_CHUNK_SIZE);
				match file.read_buf(&mut buffer).await {
					Ok(0) => Some((
						T::trailer(rpc::DataTrailer {
							bytes: sent,
							// The trailer's checksum is optional, and hashing
							// would mean a second pass over the file.
							blake3: String::new(),
						}),
						Stage::Done,
					)),
					Ok(read) => {
						Some((T::chunk(buffer.into()), Stage::Chunks(file, sent + read as u64)))
					}
					// The stream cannot report an error, so it stops early;
					// the server sees a truncated import and rejects it
					// rather than committing a partial file.
					Err(_) => None,
				}
			}
			Stage::Done => None,
		}
	})
}

/// Writes an export stream to a file, replacing anything already there.
async fn write_export_to_file(mut stream: ExportChunks, path: PathBuf) -> EngineResult<()> {
	use futures::StreamExt;
	use tokio::io::AsyncWriteExt;

	let mut file = tokio::fs::File::create(&path)
		.await
		.map_err(|e| Error::internal(format!("Failed to create {}: {e}", path.display())))?;
	while let Some(chunk) = stream.next().await {
		let chunk = chunk?;
		file.write_all(&chunk)
			.await
			.map_err(|e| Error::internal(format!("Failed to write {}: {e}", path.display())))?;
	}
	file.flush()
		.await
		.map_err(|e| Error::internal(format!("Failed to flush {}: {e}", path.display())))?;
	Ok(())
}

/// Forwards an export stream to the channel the caller reads from, passing on
/// a failure so the reader sees it rather than a silently short export.
async fn pump_export_to_channel(
	mut stream: ExportChunks,
	bytes: async_channel::Sender<Result<Vec<u8>, Error>>,
) {
	use futures::StreamExt;

	while let Some(chunk) = stream.next().await {
		let chunk = match chunk {
			Ok(chunk) => chunk,
			Err(error) => {
				bytes.send(Err(error)).await.ok();
				return;
			}
		};
		// The engine interface hands the caller owned bytes, so this is the
		// one place an export chunk has to be copied.
		if bytes.send(Ok(chunk.to_vec())).await.is_err() {
			// The reader is gone; stop pulling the export.
			return;
		}
	}
	bytes.close();
}

/// Forwards a subscription's notifications to the channel the caller holds.
async fn pump_notifications(
	mut stream: tonic::Streaming<rpc::SubscribeResponse>,
	notifications: async_channel::Sender<Result<Notification, Error>>,
	live_query_id: Uuid,
) {
	loop {
		let message = match stream.message().await {
			Ok(Some(message)) => message,
			Ok(None) => break,
			Err(status) => {
				notifications.send(Err(status_to_error(status))).await.ok();
				break;
			}
		};
		match message.frame {
			// `Begin` only names the subscription, which the caller already
			// knows by its live query id.
			Some(rpc::subscribe_response::Frame::Begin(_)) => {}
			Some(rpc::subscribe_response::Frame::Notification(notification)) => {
				match convert_notification(notification, live_query_id) {
					Ok(notification) => {
						if notifications.send(Ok(notification)).await.is_err() {
							return;
						}
					}
					Err(error) => {
						notifications.send(Err(error)).await.ok();
						return;
					}
				}
			}
			Some(rpc::subscribe_response::Frame::End(_)) => break,
			Some(rpc::subscribe_response::Frame::Error(error)) => {
				notifications.send(Err(proto_error(error))).await.ok();
				break;
			}
			None => break,
		}
	}
	notifications.close();
}

fn convert_notification(
	notification: rpc::Notification,
	live_query_id: Uuid,
) -> EngineResult<Notification> {
	let action = match notification.action() {
		rpc::Action::Created => Action::Create,
		rpc::Action::Updated => Action::Update,
		rpc::Action::Deleted => Action::Delete,
		rpc::Action::Unspecified => {
			return Err(Error::internal("Notification carried an unrecognised action".to_string()));
		}
	};
	let record = match notification.record_id {
		Some(record_id) => Value::RecordId(
			record_id.try_into().map_err(|e: anyhow::Error| deserialization_error(e))?,
		),
		None => Value::None,
	};
	let result = match notification.value {
		Some(value) => Value::try_from(value).map_err(deserialization_error)?,
		None => Value::None,
	};
	// The server echoes the live query id; fall back to the one subscribed
	// with if it is absent, since the caller matches on it.
	let id = notification.live_query_id.and_then(|id| id.to_uuid().ok()).unwrap_or(live_query_id);
	Ok(Notification::new(id.into(), None, action, record, result))
}

/// Maps the SDK's export configuration onto the wire's.
fn export_config(config: DbExportConfig) -> EngineResult<rpc::ExportConfig> {
	use surrealdb_rpc::export::TableConfig;

	let tables = match config.tables {
		TableConfig::All => rpc::export_config::Tables::from(true),
		TableConfig::None => rpc::export_config::Tables::from(false),
		TableConfig::Some(tables) => rpc::export_config::Tables {
			selection: Some(rpc::export_config::tables::Selection::Selected(
				rpc::export_config::SelectedTables {
					tables,
				},
			)),
		},
		// The wire format can name the tables to include but not the ones to
		// leave out. Exporting everything instead would be the opposite of
		// what the caller asked for, so this fails.
		TableConfig::Exclude(excluded) => rpc::export_config::Tables {
			selection: Some(rpc::export_config::tables::Selection::Excluded(
				rpc::export_config::ExcludedTables {
					tables: excluded.exclude,
				},
			)),
		},
	};
	Ok(rpc::ExportConfig {
		users: config.users,
		accesses: config.accesses,
		params: config.params,
		functions: config.functions,
		analyzers: config.analyzers,
		tables: Some(tables),
		versions: config.versions,
		records: config.records,
		sequences: config.sequences,
		apis: config.apis,
		buckets: config.buckets,
		modules: config.modules,
		configs: config.configs,
	})
}

/// One statement's batches, accumulated into a single [`QueryResult`].
#[derive(Default)]
struct Statement {
	values: Vec<Value>,
	/// A `SINGLE` statement yields one value rather than a list.
	single: bool,
	stats: Option<rpc::QueryStats>,
	statement_kind: i32,
	error: Option<Error>,
}

impl Statement {
	fn push(&mut self, batch: rpc::QueryBatchFrame) {
		self.statement_kind = batch.statement_kind;
		if batch.kind == rpc::QueryResponseKind::Single as i32 {
			self.single = true;
		}
		if batch.stats.is_some() {
			self.stats = batch.stats;
		}
		if let Some(error) = batch.error {
			self.error = Some(proto_error(error));
			return;
		}
		match batch.payload {
			Some(rpc::query_batch_frame::Payload::Values(values)) => {
				// The batch carries its whole payload, so the accumulator is
				// sized once rather than grown a row at a time.
				self.values.reserve(values.values.len());
				for value in values.values {
					match Value::try_from(value) {
						Ok(value) => self.values.push(value),
						Err(e) => {
							self.error = Some(deserialization_error(e));
							return;
						}
					}
				}
			}
			Some(rpc::query_batch_frame::Payload::Arrow(_)) => {
				self.error = Some(Error::internal(
					"Server sent a columnar batch, which was not requested".to_string(),
				));
			}
			None => {}
		}
	}

	fn finish(self) -> QueryResult {
		let time = self
			.stats
			.and_then(|stats| stats.execution_duration)
			.and_then(|duration| StdDuration::try_from(duration).ok())
			.unwrap_or_default();
		let query_type = if self.statement_kind == rpc::QueryStatementKind::Live as i32 {
			QueryType::Live
		} else if self.statement_kind == rpc::QueryStatementKind::Kill as i32 {
			QueryType::Kill
		} else {
			QueryType::Other
		};
		// A statement's own failure is carried in its result, so the rest of
		// the query's results still reach the caller.
		let result = match self.error {
			Some(error) => Err(error),
			None if self.single => Ok(self.values.into_iter().next().unwrap_or(Value::None)),
			None => Ok(Value::Array(Array::from(self.values))),
		};
		QueryResult {
			time,
			result,
			query_type,
		}
	}
}

fn nullable(value: String) -> rpc::NullableString {
	rpc::NullableString {
		value: Some(rpc::nullable_string::Value::Some(value)),
	}
}

fn to_proto_variables(variables: Variables) -> EngineResult<proto::Variables> {
	to_variables(Object::from(variables).into_inner())
}

fn tokens_to_token(tokens: Option<rpc::Tokens>) -> EngineResult<Token> {
	let tokens =
		tokens.ok_or_else(|| Error::internal("Server did not return any tokens".to_string()))?;
	Ok(if tokens.refresh.is_empty() {
		Token::Access(tokens.access)
	} else {
		Token::WithRefresh {
			access: tokens.access,
			refresh: tokens.refresh,
		}
	})
}

/// Classifies credentials by the fields they carry, matching how
/// [`Root`](crate::opt::auth::Root), [`Namespace`](crate::opt::auth::Namespace),
/// [`Database`](crate::opt::auth::Database) and
/// [`Record`](crate::opt::auth::Record) each render themselves.
fn access_method(credentials: Object) -> EngineResult<rpc::AccessMethod> {
	let mut fields = credentials.into_inner();
	// `ac` is tested first because that is what the database itself dispatches
	// on: signin routes to an access method whenever `ac` is present, and only
	// falls back to `user`/`pass` when it is not. Probing for `user` first
	// would misread record credentials whose SIGNIN clause takes a `user`
	// parameter -- a common shape -- as system-user credentials, and those
	// carry no variables, so every other parameter would be dropped.
	let method = if let Some(access) = take_string(&mut fields, "ac")? {
		let namespace = take_string(&mut fields, "ns")?.unwrap_or_default();
		let database = take_string(&mut fields, "db")?.unwrap_or_default();
		match take_string(&mut fields, "key")? {
			// A bearer grant is an access method plus the key redeeming it.
			Some(key) => rpc::access_method::Method::Bearer(rpc::BearerCredentials {
				namespace,
				database,
				access,
				key,
			}),
			// Anything else naming an access method is record access, and
			// whatever is left is the variables its SIGNIN clause receives.
			None => rpc::access_method::Method::Record(rpc::RecordCredentials {
				namespace,
				database,
				access,
				variables: Some(to_variables(fields)?),
			}),
		}
	} else if fields.contains_key("user") {
		rpc::access_method::Method::User(rpc::UserCredentials {
			namespace: take_string(&mut fields, "ns")?.unwrap_or_default(),
			database: take_string(&mut fields, "db")?.unwrap_or_default(),
			username: take_string(&mut fields, "user")?.unwrap_or_default(),
			password: take_string(&mut fields, "pass")?.unwrap_or_default(),
			// System-user credentials name no access method: `ac` is what
			// selects the branch above.
			access: String::new(),
		})
	} else {
		return Err(Error::validation(
			"Unrecognised credentials: expected a `user`, `ac`, or `key` field".to_string(),
			None,
		));
	};
	Ok(rpc::AccessMethod {
		method: Some(method),
	})
}

/// Signup always registers a record user, so its credentials are always
/// record-access credentials.
fn record_credentials(credentials: Object) -> EngineResult<rpc::RecordCredentials> {
	let mut fields = credentials.into_inner();
	let namespace = take_string(&mut fields, "ns")?.unwrap_or_default();
	let database = take_string(&mut fields, "db")?.unwrap_or_default();
	let access = take_string(&mut fields, "ac")?.ok_or_else(|| {
		Error::validation("Missing `ac` field in signup credentials".to_string(), None)
	})?;
	Ok(rpc::RecordCredentials {
		namespace,
		database,
		access,
		variables: Some(to_variables(fields)?),
	})
}

/// Removes a credential field that the wire carries as a bare string.
///
/// An absent field reads as `None`, which each caller turns into the empty
/// string the protocol spells "not given" as. A field that is present but is
/// not a string is rejected rather than defaulted: sending an empty password
/// for a mistyped one would report the mistake as bad credentials.
fn take_string(
	fields: &mut std::collections::BTreeMap<String, Value>,
	key: &str,
) -> EngineResult<Option<String>> {
	match fields.remove(key) {
		None => Ok(None),
		Some(Value::String(value)) => Ok(Some(value)),
		Some(_) => {
			Err(Error::validation(format!("The `{key}` credential field must be a string"), None))
		}
	}
}

/// Binds a set of named values as the wire's variables.
///
/// `proto::Variables` collects from `(String, proto::Value)` pairs itself, and
/// that impl is what puts the keys in the ascending order the schema wants, so
/// there is nothing to do here but convert each value.
fn to_variables(
	fields: std::collections::BTreeMap<String, Value>,
) -> EngineResult<proto::Variables> {
	fields
		.into_iter()
		.map(|(key, value)| Ok((key, to_proto_value(value)?)))
		.collect::<EngineResult<Vec<_>>>()
		.map(|pairs| pairs.into_iter().collect())
}

/// Converts a value for the wire, reporting one it cannot carry.
///
/// The only value that fails is a duration past the signed seconds the wire
/// counts in, which no real duration reaches.
fn to_proto_value(value: Value) -> EngineResult<proto::Value> {
	proto::Value::try_from(value).map_err(|e| {
		Error::serialization(e.to_string(), crate::types::SerializationError::Serialization)
	})
}

/// Maps a transport failure, which carries no structured error, from its
/// gRPC status code.
#[allow(clippy::needless_pass_by_value)] // By value so it reads as `.map_err(status_to_error)`; every caller owns its `Status`.
fn status_to_error(status: tonic::Status) -> Error {
	use tonic::Code;
	let message = status.message().to_string();
	match status.code() {
		Code::InvalidArgument | Code::OutOfRange => Error::validation(message, None),
		Code::Unimplemented => Error::configuration(message, None),
		Code::DeadlineExceeded | Code::Aborted => Error::query(message, None),
		Code::PermissionDenied | Code::Unauthenticated => Error::not_allowed(message, None),
		Code::NotFound => Error::not_found(message, None),
		Code::AlreadyExists => Error::already_exists(message, None),
		Code::Unavailable | Code::Cancelled | Code::Unknown => Error::connection(message, None),
		_ => Error::internal(message),
	}
}

/// Maps a structured error carried on the wire, preserving its kind, message
/// and cause chain.
///
/// The finer-grained `details.kind` is not rebuilt into the matching typed
/// detail: callers get the right `is_*` classification and message, but not
/// the structured sub-reason.
fn proto_error(error: proto::SurrealError) -> Error {
	use proto::ErrorKind;
	let kind = error.kind_or_internal();
	let cause = error.cause.map(|cause| proto_error(*cause));
	let message = error.message;
	let mapped = match kind {
		ErrorKind::Validation => Error::validation(message, None),
		ErrorKind::Configuration => Error::configuration(message, None),
		ErrorKind::Query => Error::query(message, None),
		ErrorKind::Serialization => Error::serialization(message, None),
		ErrorKind::NotAllowed => Error::not_allowed(message, None),
		ErrorKind::NotFound => Error::not_found(message, None),
		ErrorKind::AlreadyExists => Error::already_exists(message, None),
		ErrorKind::Connection => Error::connection(message, None),
		ErrorKind::Thrown => Error::thrown(message),
		ErrorKind::Internal | ErrorKind::Context | ErrorKind::Unspecified => {
			Error::internal(message)
		}
	};
	match cause {
		Some(cause) => mapped.with_cause(cause),
		None => mapped,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::types::Number;

	fn object(pairs: &[(&str, &str)]) -> Object {
		let mut object = Object::new();
		for (key, value) in pairs {
			object.insert((*key).to_string(), Value::String((*value).to_string()));
		}
		object
	}

	/// Root/namespace/database credentials all render `user`/`pass`, so they
	/// are one access method with the scope fields left empty.
	#[test]
	fn user_credentials_are_classified_by_their_user_field() {
		let method = access_method(object(&[("user", "root"), ("pass", "secret")]))
			.expect("root credentials should classify");
		let Some(rpc::access_method::Method::User(user)) = method.method else {
			panic!("expected user credentials");
		};
		assert_eq!(user.username, "root");
		assert_eq!(user.password, "secret");
		assert_eq!(user.namespace, "");
		assert_eq!(user.database, "");
	}

	#[test]
	fn database_user_credentials_carry_their_scope() {
		let method = access_method(object(&[
			("ns", "test-ns"),
			("db", "test-db"),
			("user", "alice"),
			("pass", "secret"),
		]))
		.expect("database credentials should classify");
		let Some(rpc::access_method::Method::User(user)) = method.method else {
			panic!("expected user credentials");
		};
		assert_eq!((user.namespace.as_str(), user.database.as_str()), ("test-ns", "test-db"));
	}

	/// Everything left after the access fields is a variable for the access
	/// method's SIGNIN clause.
	#[test]
	fn record_credentials_pass_their_extra_fields_as_variables() {
		let method =
			access_method(object(&[("ns", "n"), ("db", "d"), ("ac", "user"), ("email", "a@b.c")]))
				.expect("record credentials should classify");
		let Some(rpc::access_method::Method::Record(record)) = method.method else {
			panic!("expected record credentials");
		};
		assert_eq!(record.access, "user");
		let variables = record.variables.expect("variables should be set").variables;
		assert_eq!(variables.len(), 1);
		assert_eq!(variables[0].key, "email");
	}

	#[test]
	fn bearer_credentials_are_classified_by_their_key_field() {
		let method = access_method(object(&[("ac", "api"), ("key", "secret-key")]))
			.expect("bearer credentials should classify");
		assert!(matches!(method.method, Some(rpc::access_method::Method::Bearer(_))));
	}

	/// An access method is what the database dispatches signin on, so it wins
	/// over a `user` field -- which a record access method's own SIGNIN clause
	/// is free to take as a parameter.
	#[test]
	fn record_credentials_keep_their_user_parameter() {
		let method = access_method(object(&[
			("ns", "n"),
			("db", "d"),
			("ac", "account"),
			("user", "tobie"),
			("tenant", "acme"),
		]))
		.expect("record credentials should classify");
		let Some(rpc::access_method::Method::Record(record)) = method.method else {
			panic!("expected record credentials");
		};
		assert_eq!(record.access, "account");
		let variables = record.variables.expect("variables should be set").variables;
		let keys: Vec<&str> = variables.iter().map(|kv| kv.key.as_str()).collect();
		assert_eq!(keys, ["tenant", "user"]);
	}

	/// A credential field that is not a string is refused rather than sent as
	/// an empty one, which would report a mistyped password as a bad password.
	#[test]
	fn non_string_credential_fields_are_rejected() {
		let mut credentials = Object::new();
		credentials.insert("user".to_string(), Value::String("root".to_string()));
		credentials.insert("pass".to_string(), Value::Number(Number::Int(1234)));
		let error =
			access_method(credentials).expect_err("a non-string password should be rejected");
		assert!(error.is_validation(), "expected a validation error, got {error:?}");
	}

	/// Re-selecting the same database, or rebinding the same variable, leaves
	/// the log describing the state the session reached rather than every step
	/// it took to get there.
	#[test]
	fn the_replay_log_coalesces_repeated_operations() {
		let entry = SessionEntry::default();
		for _ in 0..10 {
			entry.record(Replayable::Use {
				namespace: Some("ns".to_string()),
				database: Some("db".to_string()),
			});
			entry.record(Replayable::Set {
				key: "tenant".to_string(),
				value: Value::String("acme".to_string()),
			});
		}
		let log = entry.log();
		assert_eq!(log.len(), 2, "expected one `Use` and one `Set`, got {log:?}");
		assert!(matches!(log[0], Replayable::Use { .. }));
		assert!(matches!(&log[1], Replayable::Set { key, .. } if key == "tenant"));
	}

	/// Coalescing stops at an operation that re-establishes the session, so a
	/// clone reaches the same state in the same order the original did.
	#[test]
	fn the_replay_log_does_not_coalesce_across_a_sign_in() {
		let entry = SessionEntry::default();
		let use_ns_db = || Replayable::Use {
			namespace: Some("ns".to_string()),
			database: Some("db".to_string()),
		};
		entry.record(use_ns_db());
		entry.record(Replayable::Signin(Object::new()));
		entry.record(use_ns_db());
		let log = entry.log();
		assert_eq!(log.len(), 3, "the `Use` before the sign-in must survive, got {log:?}");
	}

	#[test]
	fn unrecognised_credentials_are_rejected() {
		let error = access_method(object(&[("nonsense", "value")]))
			.expect_err("unrecognised credentials should be rejected");
		assert!(error.is_validation(), "expected a validation error, got {error:?}");
	}

	#[test]
	fn signup_requires_an_access_method() {
		let error = record_credentials(object(&[("ns", "n"), ("db", "d")]))
			.expect_err("signup without `ac` should be rejected");
		assert!(error.is_validation(), "expected a validation error, got {error:?}");
	}

	/// The wire error's kind selects the SDK error's kind, and its cause chain
	/// is preserved rather than flattened into the message.
	#[test]
	fn wire_errors_keep_their_kind_and_cause() {
		let inner = proto::SurrealError {
			kind: proto::ErrorKind::NotFound as i32,
			message: "no such table".to_string(),
			..Default::default()
		};
		let outer = proto::SurrealError {
			kind: proto::ErrorKind::Query as i32,
			message: "query failed".to_string(),
			cause: Some(Box::new(inner)),
			..Default::default()
		};
		let error = proto_error(outer);
		assert!(error.is_query(), "expected a query error, got {error:?}");
		assert_eq!(error.message(), "query failed");
		let cause = error.cause().expect("the cause should be preserved");
		assert!(cause.is_not_found(), "expected a not-found cause, got {cause:?}");
	}

	/// A kind this build does not know about must behave as `Internal` rather
	/// than as "no error kind".
	#[test]
	fn unknown_wire_error_kinds_degrade_to_internal() {
		let error = proto_error(proto::SurrealError {
			kind: 9999,
			message: "from the future".to_string(),
			..Default::default()
		});
		assert!(error.is_internal(), "expected an internal error, got {error:?}");
	}

	fn batch(values: Vec<Value>, kind: rpc::QueryResponseKind) -> rpc::QueryBatchFrame {
		rpc::QueryBatchFrame {
			query_index: 0,
			batch_index: 0,
			kind: kind as i32,
			statement_kind: rpc::QueryStatementKind::Other as i32,
			stats: None,
			error: None,
			payload: Some(rpc::query_batch_frame::Payload::Values(rpc::ValueBatch {
				values: values
					.into_iter()
					.map(|v| proto::Value::try_from(v).expect("encodable"))
					.collect(),
			})),
		}
	}

	/// A `SINGLE` statement yields its one value; anything else yields a list,
	/// even when only one value arrived.
	#[test]
	fn single_statements_are_not_wrapped_in_an_array() {
		let mut statement = Statement::default();
		statement.push(batch(vec![Value::Number(1.into())], rpc::QueryResponseKind::Single));
		assert_eq!(statement.finish().result.unwrap(), Value::Number(1.into()));

		let mut statement = Statement::default();
		statement.push(batch(vec![Value::Number(1.into())], rpc::QueryResponseKind::BatchedFinal));
		assert_eq!(
			statement.finish().result.unwrap(),
			Value::Array(Array::from(vec![Value::Number(1.into())]))
		);
	}

	#[test]
	fn batches_accumulate_across_frames() {
		let mut statement = Statement::default();
		statement.push(batch(vec![Value::Number(1.into())], rpc::QueryResponseKind::Batched));
		statement.push(batch(vec![Value::Number(2.into())], rpc::QueryResponseKind::BatchedFinal));
		assert_eq!(
			statement.finish().result.unwrap(),
			Value::Array(Array::from(vec![Value::Number(1.into()), Value::Number(2.into())]))
		);
	}

	/// A statement's own failure travels in its result, so the other
	/// Excluding tables is carried on the wire rather than refused: the
	/// protocol names the tables to leave out, so the caller gets what it
	/// asked for instead of an error naming a limitation that is gone.
	#[test]
	fn excluding_tables_from_an_export_is_carried() {
		use rpc::export_config::tables::Selection;
		use surrealdb_rpc::export::{ExcludedTables, TableConfig};

		let config = DbExportConfig {
			tables: TableConfig::Exclude(ExcludedTables {
				exclude: vec!["secrets".to_string()],
			}),
			..Default::default()
		};
		let wire = export_config(config).expect("excluding tables should be carried");
		let Some(Selection::Excluded(excluded)) = wire.tables.and_then(|t| t.selection) else {
			panic!("expected an excluded selection");
		};
		assert_eq!(excluded.tables, ["secrets"]);
	}

	#[test]
	fn export_table_selection_maps_onto_the_wire() {
		use rpc::export_config::tables::Selection;
		use surrealdb_rpc::export::TableConfig;

		let selection = |tables| {
			export_config(DbExportConfig {
				tables,
				..Default::default()
			})
			.expect("selection should convert")
			.tables
			.expect("tables should be set")
			.selection
			.expect("a selection should be set")
		};
		assert!(matches!(selection(TableConfig::All), Selection::All(_)));
		assert!(matches!(selection(TableConfig::None), Selection::None(_)));
		let Selection::Selected(selected) =
			selection(TableConfig::Some(vec!["person".to_string()]))
		else {
			panic!("expected a selected-tables selection");
		};
		assert_eq!(selected.tables, vec!["person".to_string()]);
	}

	/// A server that reports no capability names predates them being
	/// populated, so features are assumed rather than silently switched off.
	#[test]
	fn a_silent_server_is_assumed_to_support_everything() {
		let features = extra_features(&rpc::ServerCapabilities::default());
		assert!(features.contains(&ExtraFeatures::Backup));
		assert!(features.contains(&ExtraFeatures::LiveQueries));
	}

	#[test]
	fn live_queries_are_gated_on_the_reported_capability() {
		let without = extra_features(&rpc::ServerCapabilities {
			capabilities: vec!["TRANSACTIONS".to_string()],
			..Default::default()
		});
		assert!(!without.contains(&ExtraFeatures::LiveQueries));

		let with = extra_features(&rpc::ServerCapabilities {
			capabilities: vec!["LIVE_QUERIES".to_string()],
			..Default::default()
		});
		assert!(with.contains(&ExtraFeatures::LiveQueries));
	}

	#[test]
	fn a_denied_export_method_withdraws_the_backup_feature() {
		let features = extra_features(&rpc::ServerCapabilities {
			capabilities: vec!["LIVE_QUERIES".to_string()],
			denied_methods: vec![
				"surrealdb.protocol.rpc.v1.SurrealDBService/ExportSurql".to_string(),
			],
			..Default::default()
		});
		assert!(!features.contains(&ExtraFeatures::Backup));
	}

	/// The record id and value a notification carries are decoded, and an
	/// action this build does not recognise is an error rather than a guess.
	#[test]
	fn notifications_decode_their_record_and_value() {
		let id = Uuid::from_u128(7);
		let notification = convert_notification(
			rpc::Notification {
				live_query_id: Some(proto::Uuid::from_uuid(id)),
				action: rpc::Action::Updated as i32,
				record_id: None,
				value: Some(
					proto::Value::try_from(Value::String("hello".to_string())).expect("encodable"),
				),
				cursor: None,
			},
			id,
		)
		.expect("the notification should convert");
		assert_eq!(notification.id, id.into());
		assert_eq!(notification.action, Action::Update);
		assert_eq!(notification.result, Value::String("hello".to_string()));
	}

	#[test]
	fn notifications_with_an_unrecognised_action_are_rejected() {
		let error = convert_notification(
			rpc::Notification {
				live_query_id: None,
				action: rpc::Action::Unspecified as i32,
				record_id: None,
				value: None,
				cursor: None,
			},
			Uuid::from_u128(1),
		)
		.expect_err("an unspecified action should be rejected");
		assert!(error.is_internal(), "expected an internal error, got {error:?}");
	}
}
