//! HTTP engine for connecting to SurrealDB over HTTP/HTTPS.
//!
//! This module provides HTTP-based connectivity to SurrealDB servers. While HTTP is
//! traditionally a stateless protocol, this implementation supports stateful sessions
//! by maintaining server-side session state and using session IDs in requests.
//!
//! # Security
//!
//! Every HTTP request carries its own authentication material (Basic or
//! Bearer). Server-side, an attached HTTP session is bound to the
//! authenticating principal the first time an authentication command
//! (`signin`, `signup`, or `authenticate`) runs against it. Before that
//! point the session is anonymous and holds no elevated privileges, so
//! it is reachable by any caller - this preserves the `attach` then
//! `signin` replay sequence emitted by this SDK. Once the session is
//! bound to a principal, every subsequent request that targets that
//! session UUID must present a request-level principal that matches on
//! actor id and [`Level`]; mismatches return `session_not_found`.
//! This ensures an attacker who observes or guesses a victim's session UUID
//! *after* the victim authenticates cannot use it to impersonate them, and
//! an attacker cannot enumerate UUIDs in the first place - see the `sessions`
//! bullet below.
//!
//! Concretely, this has two SDK-visible consequences:
//!
//! - `Command::Attach` on a freshly created session is always accepted: it creates a fresh,
//!   anonymous session keyed by a new 128-bit UUID. The caller is bound to it later by the first
//!   `signin` / `signup` / `authenticate` call, so the standard SDK replay ordering (`Attach`, then
//!   `Use`/`Signin`/etc.) continues to work as before.
//! - If credentials on a `Surreal::<Http>` handle change (for example via a new `signin` with
//!   different credentials) the next HTTP call continues to use the same session UUID. The server
//!   accepts it because the `signin` command itself mutates the session's stored principal to match
//!   the new credentials before any subsequent request is gated against it.
//!
//! Enumeration of attached HTTP sessions via the `sessions` RPC method
//! is not permitted and always returns `method_not_allowed`. Session
//! UUIDs are 128-bit random and cannot be guessed, so a session that
//! has not yet been authenticated is effectively unreachable to anyone
//! but its creator even though the server does not enforce ownership
//! on it.
//!
//! [`Level`]: surrealdb_iam::Level
//!
//! # Multi-Node Deployments
//!
//! The server stores each attached HTTP session — its authentication state, selected namespace
//! and database, and session variables — in its datastore, and every request loads it from
//! there. Any node sharing that datastore therefore serves any session, so a load balancer in
//! front of several nodes may route each request to any of them, and a session survives a
//! server restart.
//!
//! Older servers keep an HTTP session in the memory of the node that attached it. Behind a load
//! balancer they need sticky sessions (session affinity), or a [WebSocket
//! connection](`super::ws`), which stays on one node; otherwise a request routed to another node
//! fails with "session not found".
//!
//! The SDK resolves the server's address once, at connection time, and sends every request to
//! that address.
//!
//! ## Example: Connecting via HTTP
//!
//! ```no_run
//! use surrealdb::Surreal;
//! use surrealdb::engine::remote::http::Http;
//!
//! # #[tokio::main]
//! # async fn main() -> surrealdb::Result<()> {
//! let db = Surreal::new::<Http>("localhost:8000").await?;
//! # Ok(())
//! # }
//! ```

#[cfg(not(target_family = "wasm"))]
pub(crate) mod native;
#[cfg(target_family = "wasm")]
pub(crate) mod wasm;

use std::marker::PhantomData;
#[cfg(not(target_family = "wasm"))]
use std::path::PathBuf;
use std::sync::Arc;

use futures::TryStreamExt;
use reqwest::RequestBuilder;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use surrealdb_rpc::{DbResponse, DbResult, QueryResult, QueryResultBuilder, Token as CoreToken};
use surrealdb_types::{AuthError, NotAllowedError};
#[cfg(not(target_family = "wasm"))]
use surrealdb_types::{ConnectionError, SerializationError};
#[cfg(not(target_family = "wasm"))]
use tokio::fs::OpenOptions;
#[cfg(not(target_family = "wasm"))]
use tokio::io;
use tokio::sync::RwLock;
#[cfg(not(target_family = "wasm"))]
use tokio_util::compat::FuturesAsyncReadCompatExt;
use url::Url;
use uuid::Uuid;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local;

use crate::conn::{Command, RequestData};
use crate::engine::SessionError;
use crate::engine::remote::{RemoteCommand, RouterRequest};
use crate::headers::{AUTH_DB, AUTH_NS, DB, NS};
use crate::opt::IntoEndpoint;
use crate::opt::auth::{AccessToken, Token};
use crate::types::{HashMap, SurrealValue, Value};
use crate::{Connect, Error, Result, Surreal};

const RPC_PATH: &str = "rpc";

/// Per-session state for HTTP connections.
/// Uses RwLock for headers and auth to allow concurrent request handling
/// without cloning the entire state for each request.
#[derive(Debug)]
struct SessionState {
	/// HTTP headers for this session (e.g., namespace, database)
	headers: RwLock<HeaderMap>,
	/// Authentication state for REST endpoints (export/import).
	/// RPC calls don't need this as the server session is already authenticated.
	auth: RwLock<Option<Auth>>,
	/// Commands to replay when cloning a session (e.g., Attach, Use, Signin, etc.)
	/// Uses boxcar::Vec for lock-free concurrent appends.
	replay: boxcar::Vec<Command>,
}

impl Default for SessionState {
	fn default() -> Self {
		Self {
			headers: RwLock::new(HeaderMap::new()),
			auth: RwLock::new(None),
			replay: boxcar::Vec::new(),
		}
	}
}

impl SessionState {
	/// Clone the session state by reading the current values.
	/// This is used when cloning a session to create a new independent copy.
	async fn clone_state(&self) -> Self {
		Self {
			headers: RwLock::new(self.headers.read().await.clone()),
			auth: RwLock::new(self.auth.read().await.clone()),
			replay: self.replay.clone(),
		}
	}
}

type SessionResult = std::result::Result<Arc<SessionState>, SessionError>;

/// Router state for HTTP connections
struct RouterState {
	/// Per-session state (headers, auth for REST endpoints, replay commands)
	sessions: HashMap<Uuid, SessionResult>,
	/// Sessions whose setup failed and whose cleanup detach failed too, with
	/// the headers and authentication to detach them with when they are
	/// dropped.
	undetached: HashMap<Uuid, (HeaderMap, Option<Auth>)>,
	/// The shared HTTP client used to send requests.
	/// On native platforms, this client is configured with a resolved address
	/// via `reqwest::ClientBuilder::resolve()` to ensure all requests go to
	/// the same server node, avoiding issues with DNS round-robin.
	client: reqwest::Client,
	/// The base URL for the SurrealDB server
	base_url: Url,
}

impl RouterState {
	/// Creates a new RouterState with the given client and base URL
	fn new(client: reqwest::Client, base_url: Url) -> Self {
		Self {
			sessions: HashMap::new(),
			undetached: HashMap::new(),
			client,
			base_url,
		}
	}

	/// Replay all stored commands for a session (attach + any Use/Signin/etc.)
	///
	/// Each command is sent with the authentication the server session holds
	/// at that point in the replay, the way the original session sent it: none
	/// before the first sign-in, then whatever each successful sign-in, sign-up,
	/// authenticate or invalidate left. Returns that authentication as it stands
	/// after the commands that went through, alongside the first failure.
	async fn replay_session(
		&self,
		session_id: Uuid,
		session_state: &SessionState,
	) -> (Option<Auth>, Result<()>) {
		// Clone headers upfront to avoid holding the lock across network I/O
		let headers = session_state.headers.read().await.clone();
		let mut auth = None;

		for (_, command) in &session_state.replay {
			let request = command
				.clone()
				.into_router_request(None, Some(session_id))
				.expect("replay command should convert to router request");

			match send_request(request, &self.base_url, &self.client, &headers, &auth).await {
				Ok(results) => auth = auth_after(command, &results, auth),
				Err(error) => return (auth, Err(error)),
			}
		}
		(auth, Ok(()))
	}

	/// Handle a new session being created.
	async fn handle_session_initial(&self, session_id: Uuid) {
		let session_state = SessionState::default();
		session_state.replay.push(Command::Attach {
			session_id,
		});
		let session_state = Arc::new(session_state);
		self.sessions.insert(session_id, Ok(Arc::clone(&session_state)));

		if let (auth, Err(error)) = self.replay_session(session_id, &session_state).await {
			// A failed response does not say whether the attach landed.
			self.release_failed_setup(session_id, &session_state.headers, auth).await;
			self.sessions.insert(session_id, Err(SessionError::Remote(error.to_string())));
		}
	}

	/// Handle a session being cloned.
	async fn handle_session_clone(&self, old: Uuid, new: Uuid) {
		match self.sessions.get(&old) {
			Some(Ok(session_state)) => {
				let mut new_state = session_state.clone_state().await;
				// Replace the attach command with the new session id
				if let Some(cmd) = new_state.replay.get_mut(0) {
					*cmd = Command::Attach {
						session_id: new,
					};
				}
				let new_state = Arc::new(new_state);
				self.sessions.insert(new, Ok(Arc::clone(&new_state)));

				match self.replay_session(new, &new_state).await {
					// The copied authentication belongs to the old session. A
					// replayed sign-in may have issued the new one a different
					// credential, which is the one its requests must carry.
					(auth, Ok(())) => *new_state.auth.write().await = auth,
					// The server may have accepted the attach before a later
					// command failed. Nothing can use the session now, so it is
					// detached here, as the principal the replay left it with,
					// rather than when it is dropped.
					(auth, Err(error)) => {
						self.release_failed_setup(new, &new_state.headers, auth).await;
						self.sessions.insert(new, Err(SessionError::Remote(error.to_string())));
					}
				}
			}
			Some(Err(error)) => {
				self.sessions.insert(new, Err(error));
			}
			None => {
				self.sessions.insert(new, Err(SessionError::NotFound(old)));
			}
		}
	}

	/// Detach a session on the server, reporting whether the server answered.
	///
	/// The server only lets a session's owner detach it, so `auth` must be the
	/// authentication the server session holds. The request is awaited so that
	/// a detach reaches the server before any lifecycle event queued after it,
	/// such as an attach that needs the slot it frees.
	async fn detach(&self, session_id: Uuid, headers: &HeaderMap, auth: &Option<Auth>) -> bool {
		let request = Command::Detach {
			session_id,
		}
		.into_router_request(None, Some(session_id))
		.expect("detach command should convert to router request");
		send_request(request, &self.base_url, &self.client, headers, auth).await.is_ok()
	}

	/// Detach a session whose setup failed.
	///
	/// A failed response does not say whether the server accepted the attach,
	/// so the session is detached now. If that fails as well, the detach is
	/// kept and sent again when the session is dropped.
	async fn release_failed_setup(
		&self,
		session_id: Uuid,
		headers: &RwLock<HeaderMap>,
		auth: Option<Auth>,
	) {
		let headers = headers.read().await.clone();
		if !self.detach(session_id, &headers, &auth).await {
			self.undetached.insert(session_id, (headers, auth));
		}
	}

	/// Handle a session being dropped.
	///
	/// The detach carries the session's own credentials: the server binds an
	/// attached session to the principal that signed in on it and refuses any
	/// other caller, a detach included.
	async fn handle_session_drop(&self, session_id: Uuid) {
		let found = self.sessions.get(&session_id);
		self.sessions.remove(&session_id);
		let undetached = self.undetached.get(&session_id);
		self.undetached.remove(&session_id);
		match found {
			Some(Ok(state)) => {
				let headers = state.headers.read().await.clone();
				let auth = state.auth.read().await.clone();
				self.detach(session_id, &headers, &auth).await;
			}
			_ => {
				if let Some((headers, auth)) = undetached {
					self.detach(session_id, &headers, &auth).await;
				}
			}
		}
	}

	/// Detaches every session still attached, as the router exits.
	async fn detach_all(&self) {
		for (session_id, _) in self.sessions.to_vec() {
			self.handle_session_drop(session_id).await;
		}
	}

	/// Dispatch a session-lifecycle event to the appropriate handler.
	async fn handle_session(&self, session_id: crate::SessionId) {
		match session_id {
			crate::SessionId::Initial(id) => self.handle_session_initial(id).await,
			crate::SessionId::Clone {
				old,
				new,
			} => self.handle_session_clone(old, new).await,
			crate::SessionId::Drop(id) => self.handle_session_drop(id).await,
		}
	}
}

/// The HTTP scheme used to connect to `http://` endpoints.
///
/// Use this for unencrypted connections to SurrealDB servers. For production
/// deployments, consider using [`Https`] instead for encrypted connections.
///
/// # Multi-Node Deployments
///
/// Any node behind a load balancer can serve this client's session; older
/// servers need sticky sessions. See the [module documentation](self) for
/// details.
///
/// # Example
///
/// ```no_run
/// use surrealdb::Surreal;
/// use surrealdb::engine::remote::http::Http;
///
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let db = Surreal::new::<Http>("localhost:8000").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Http;

/// The HTTPS scheme used to connect to `https://` endpoints.
///
/// Use this for TLS-encrypted connections to SurrealDB servers. This is the
/// recommended protocol for production deployments.
///
/// # Multi-Node Deployments
///
/// Any node behind a load balancer can serve this client's session; older
/// servers need sticky sessions. See the [module documentation](self) for
/// details.
///
/// # Example
///
/// ```no_run
/// use surrealdb::Surreal;
/// use surrealdb::engine::remote::http::Https;
///
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let db = Surreal::new::<Https>("localhost:8000").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Https;

/// An HTTP client for communicating with the server via HTTP.
///
/// This client maintains session state and supports all SurrealDB operations
/// over HTTP. The client sends every request to the server address resolved at
/// connection time.
///
/// # Multi-Node Deployments
///
/// The server stores the client's session in its datastore, so any node behind
/// a load balancer can serve each request; older servers need sticky sessions.
/// See the [module documentation](self) for details.
///
/// # Comparison with WebSocket
///
/// | Feature | HTTP | WebSocket |
/// |---------|------|-----------|
/// | Live queries | ❌ Not supported | ✅ Supported |
/// | Connection persistence | Per-request | Persistent |
/// | Proxy compatibility | Excellent | May require configuration |
/// | Multi-node clusters | Any node serves the session | Natural session affinity |
///
/// When live queries are needed, use [WebSocket
/// connections](`super::ws::Client`) instead.
#[derive(Debug, Clone)]
pub struct Client(());

impl Surreal<Client> {
	/// Connects to a specific database endpoint, saving the connection on the
	/// static client
	///
	/// # Examples
	///
	/// ```no_run
	/// use std::sync::LazyLock;
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::remote::http::Client;
	/// use surrealdb::engine::remote::http::Http;
	///
	/// static DB: LazyLock<Surreal<Client>> = LazyLock::new(Surreal::init);
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// DB.connect::<Http>("localhost:8000").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn connect<P>(
		&self,
		address: impl IntoEndpoint<P, Client = Client>,
	) -> Connect<Client, ()> {
		Connect {
			surreal: Arc::clone(&self.inner).into(),
			address: address.into_endpoint(),
			capacity: 0,
			response_type: PhantomData,
		}
	}
}

/// Serialize a `Value` as a JSON string for a REST body.
///
/// Cannot fail: the conversion goes through `serde_json::Value`, whose string
/// serialization is infallible.
fn json_encode_str(value: Value) -> String {
	serde_json::to_string(&value.into_json_value())
		.expect("serialization to json string should not fail")
}

pub(crate) fn default_headers() -> HeaderMap {
	let mut headers = HeaderMap::new();
	headers.insert(ACCEPT, HeaderValue::from_static(surrealdb_rpc::wire::ACCEPT));
	headers.insert(CONTENT_TYPE, HeaderValue::from_static(surrealdb_rpc::wire::CONTENT_TYPE));
	headers
}

#[derive(Debug, Clone)]
enum Auth {
	Basic {
		user: String,
		pass: String,
		ns: Option<String>,
		db: Option<String>,
	},
	Bearer {
		token: AccessToken,
	},
}

trait Authenticate {
	fn auth(self, auth: &Option<Auth>) -> Self;
}

impl Authenticate for RequestBuilder {
	fn auth(self, auth: &Option<Auth>) -> Self {
		match auth {
			Some(Auth::Basic {
				user,
				pass,
				ns,
				db,
			}) => {
				let mut req = self.basic_auth(user, Some(pass));
				if let Some(ns) = ns {
					req = req.header(&AUTH_NS, ns);
				}
				if let Some(db) = db {
					req = req.header(&AUTH_DB, db);
				}
				req
			}
			Some(Auth::Bearer {
				token,
			}) => self.bearer_auth(token.as_insecure_token()),
			None => self,
		}
	}
}

#[derive(Debug, Serialize, Deserialize, SurrealValue)]
#[surreal(crate = "crate::types")]
struct Credentials {
	user: String,
	pass: String,
	ac: Option<String>,
	ns: Option<String>,
	db: Option<String>,
}

#[derive(Debug, Deserialize)]
#[expect(dead_code)]
struct AuthResponse {
	code: u16,
	details: String,
	token: Option<Token>,
}

type BackupSender = async_channel::Sender<Result<Vec<u8>>>;

#[cfg(not(target_family = "wasm"))]
async fn export_file(request: RequestBuilder, path: PathBuf) -> Result<()> {
	let mut response = request
		.send()
		.await
		.map_err(crate::std_error_to_types_error)?
		.error_for_status()
		.map_err(crate::std_error_to_types_error)?
		.bytes_stream()
		.map_err(futures::io::Error::other)
		.into_async_read()
		.compat();
	let mut file =
		match OpenOptions::new().write(true).create(true).truncate(true).open(&path).await {
			Ok(path) => path,
			Err(error) => {
				return Err(Error::internal(format!(
					"Failed to open `{}`: {error}",
					path.display()
				)));
			}
		};
	if let Err(error) = io::copy(&mut response, &mut file).await {
		return Err(Error::internal(format!("Failed to read `{}`: {error}", path.display())));
	}

	Ok(())
}

async fn export_bytes(request: RequestBuilder, bytes: BackupSender) -> Result<()> {
	let response = request
		.send()
		.await
		.map_err(crate::std_error_to_types_error)?
		.error_for_status()
		.map_err(crate::std_error_to_types_error)?;

	let future = async move {
		let mut response = response.bytes_stream();
		loop {
			let item = match response.try_next().await {
				Ok(Some(b)) => Ok(b.to_vec()),
				Ok(None) => break,
				// A stream that ends in an error is a truncated export, which
				// the receiver must not mistake for a complete one.
				Err(error) => Err(crate::std_error_to_types_error(error)),
			};
			let failed = item.is_err();
			if bytes.send(item).await.is_err() || failed {
				break;
			}
		}
	};

	#[cfg(not(target_family = "wasm"))]
	tokio::spawn(future);

	#[cfg(target_family = "wasm")]
	spawn_local(future);

	Ok(())
}

#[cfg(not(target_family = "wasm"))]
async fn import(request: RequestBuilder, path: PathBuf) -> Result<()> {
	let file = match OpenOptions::new().read(true).open(&path).await {
		Ok(path) => path,
		Err(error) => {
			return Err(Error::internal(format!("Failed to open `{}`: {}", path.display(), error)));
		}
	};

	let res = request
		// `/import` answers in `cbor`, not `cbor.2`; the wire decoder reads both.
		.header(ACCEPT, surrealdb_rpc::format::CBOR)
		.body(file)
		.send()
		.await
		.map_err(crate::std_error_to_types_error)?;

	let status = res.status();
	let bytes = res.bytes().await.map_err(crate::std_error_to_types_error)?;

	// A dump that reached execution is answered with the array of statements
	// that failed - empty when the whole dump applied - and the status says
	// whether it applied in full. Read that array first, so that a caller whose
	// import failed is told which statement failed rather than which status the
	// transport carried. A request rejected before execution is answered with
	// an error document instead, and is reported from that document.
	match surrealdb_rpc::wire::decode(&bytes) {
		Ok(Value::Array(arr)) => {
			for val in arr.into_vec() {
				let result = QueryResult::from_value(val)
					.map_err(|e| Error::internal(format!("Failed to parse query result: {e}")))?;
				result.result?;
			}
			// The import applied in full only when the server also said so. A
			// failure it reported without attributing it to a statement is
			// still a failure, and must not be returned as success.
			if status.is_success() {
				Ok(())
			} else {
				Err(Error::internal(format!(
					"The server reported a failed import with HTTP status {status}, and the import may have been partially applied"
				)))
			}
		}
		decoded => {
			if !status.is_success() {
				let body = String::from_utf8_lossy(&bytes);

				return match body.parse::<serde_json::Value>() {
					Ok(body) => {
						let error_msg = format!(
							"\n{}",
							serde_json::to_string_pretty(&body).unwrap_or_else(|_| "{}".into())
						);
						Err(Error::connection(
							format!("HTTP error: {error_msg}"),
							ConnectionError::ConnectionFailed,
						))
					}
					Err(_) => Err(Error::connection(
						format!("HTTP error: {body}"),
						ConnectionError::ConnectionFailed,
					)),
				};
			}

			match decoded {
				Ok(_) => Err(Error::internal("Expected array response from import".to_string())),
				Err(e) => Err(crate::Error::serialization(
					format!(
						"The server returned an unexpected response: Failed to deserialize CBOR payload: {e:?}"
					),
					SerializationError::Deserialization,
				)),
			}
		}
	}
}

pub(crate) async fn health(request: RequestBuilder) -> Result<()> {
	request
		.send()
		.await
		.map_err(crate::std_error_to_types_error)?
		.error_for_status()
		.map_err(crate::std_error_to_types_error)?;
	Ok(())
}

/// Record a command that succeeded on `session_state` in its replay log.
///
/// A refresh, a signup, and an authenticate the router refreshed on the way,
/// leave the session holding an access token that no logged command produces
/// without side effects. They are logged as an authenticate with the token the
/// session holds afterwards, so a replay ends with the credential the session
/// has now without re-executing `SIGNUP`.
async fn record_replay(session_state: &SessionState, command: Command) {
	if matches!(
		command,
		Command::Refresh { .. } | Command::Authenticate { .. } | Command::Signup { .. }
	) && let Some(Auth::Bearer {
		token,
	}) = &*session_state.auth.read().await
	{
		session_state.replay.push(Command::Authenticate {
			token: CoreToken::Access(token.as_insecure_token().to_owned()),
		});
	} else if command.replayable() {
		session_state.replay.push(command);
	}
}

/// The authentication a server session holds after `command` succeeded with
/// `results`, given the authentication it held before.
///
/// This mirrors what [`router`] records for the same commands, so a replay
/// reaches each command holding what the original session held there.
fn auth_after(command: &Command, results: &[QueryResult], auth: Option<Auth>) -> Option<Auth> {
	let Some(Ok(value)) = results.first().map(|result| result.clone().result) else {
		return auth;
	};
	match command {
		Command::Signin {
			..
		} => match Credentials::from_value(value.clone()) {
			Ok(credentials) => Some(Auth::Basic {
				user: credentials.user,
				pass: credentials.pass,
				ns: credentials.ns,
				db: credentials.db,
			}),
			Err(_) => match Token::from_value(value) {
				Ok(token) => Some(Auth::Bearer {
					token: token.access,
				}),
				Err(_) => auth,
			},
		},
		Command::Signup {
			..
		} => match Token::from_value(value) {
			Ok(token) => Some(Auth::Bearer {
				token: token.access,
			}),
			Err(_) => auth,
		},
		Command::Authenticate {
			token,
		} => match Token::from_value(token.clone().into_value()) {
			Ok(token) => Some(Auth::Bearer {
				token: token.access,
			}),
			Err(_) => auth,
		},
		Command::Invalidate => None,
		Command::Use {
			..
		}
		| Command::Refresh {
			..
		}
		| Command::Begin {
			..
		}
		| Command::Rollback {
			..
		}
		| Command::Commit {
			..
		}
		| Command::Revoke {
			..
		}
		| Command::Query {
			..
		}
		| Command::ExportFile {
			..
		}
		| Command::ExportBytes {
			..
		}
		| Command::ImportFile {
			..
		}
		| Command::Health
		| Command::Version
		| Command::Set {
			..
		}
		| Command::Unset {
			..
		}
		| Command::SubscribeLive {
			..
		}
		| Command::Kill {
			..
		}
		| Command::Attach {
			..
		}
		| Command::Detach {
			..
		}
		| Command::Run {
			..
		} => auth,
	}
}

/// Sends an RPC request to the SurrealDB server and returns the response.
///
/// # Arguments
/// * `req` - The router request containing the RPC method and parameters
/// * `base_url` - The base URL of the SurrealDB server
/// * `client` - The HTTP client to use for the request
/// * `headers` - HTTP headers including NS/DB for routing context
/// * `auth` - Optional authentication credentials (token). Required for RPC calls where the server
///   needs to identify the authenticated user and extract namespace/database from JWT claims (e.g.,
///   after `authenticate()`)
async fn send_request(
	req: RouterRequest,
	base_url: &Url,
	client: &reqwest::Client,
	headers: &HeaderMap,
	auth: &Option<Auth>,
) -> Result<Vec<QueryResult>> {
	let url = base_url.join(RPC_PATH).expect("valid RPC path");

	let req_value = req.into_value();
	let body = surrealdb_rpc::wire::encode_body(req_value)
		.map_err(|x| format!("Failed to serialize to CBOR: {x}"))
		.map_err(|e| {
			crate::Error::internal(format!(
				"Tried to send a value which could not be serialized: {e}"
			))
		})?;

	// Include auth header so the server can authenticate the request and maintain
	// session state. This is essential for token-based auth flows where the server
	// extracts namespace/database from JWT claims during authenticate().
	let mut headers = headers.clone();
	headers.insert(CONTENT_TYPE, HeaderValue::from_static(body.content_type));
	let http_req = client.post(url).headers(headers).auth(auth).body(body.bytes);
	let response = http_req
		.send()
		.await
		.map_err(crate::std_error_to_types_error)?
		.error_for_status()
		.map_err(crate::std_error_to_types_error)?;
	let bytes = response.bytes().await.map_err(crate::std_error_to_types_error)?;

	let response = surrealdb_rpc::wire::decode(&bytes)
		.and_then(|v| Ok(DbResponse::from_value(v)?))
		.map_err(|x| format!("Failed to deserialize CBOR payload: {x}"))
		.map_err(|e| {
			crate::Error::internal(format!("The server returned an unexpected response: {e}"))
		})?;

	match response.result? {
		DbResult::Query(results) => Ok(results),
		DbResult::Encoded(results) => {
			Ok(results.into_iter().map(|result| result.into_query_result()).collect())
		}
		DbResult::Other(value) => {
			Ok(vec![QueryResultBuilder::started_now().finish_with_result(Ok(value))])
		}
		DbResult::Live(notification) => Ok(vec![
			QueryResultBuilder::started_now().finish_with_result(Ok(notification.into_value())),
		]),
	}
}

async fn refresh_token(
	token: CoreToken,
	base_url: &Url,
	client: &reqwest::Client,
	headers: &HeaderMap,
	auth: &Option<Auth>,
	session_id: Option<uuid::Uuid>,
) -> Result<(Value, Vec<QueryResult>)> {
	let req = Command::Refresh {
		token,
	}
	.into_router_request(None, session_id)
	.expect("refresh should be a valid router request");
	let results = send_request(req, base_url, client, headers, auth).await?;
	let value = match results.first() {
		Some(result) => result.clone().result?,
		None => {
			error!("received invalid result from server");
			return Err(Error::internal("Received invalid result from server".to_string()));
		}
	};
	Ok((value, results))
}

async fn router(
	req: RequestData,
	base_url: &Url,
	client: &reqwest::Client,
	session_state: &SessionState,
) -> Result<Vec<QueryResult>> {
	let session_id = req.session_id;
	match req.command {
		Command::Use {
			namespace,
			database,
		} => {
			let req = Command::Use {
				namespace: namespace.clone(),
				database: database.clone(),
			}
			.into_router_request(None, Some(session_id))
			.expect("USE command should convert to router request");
			// Send the USE request to the server to update the session state
			let out = send_request(
				req,
				base_url,
				client,
				&*session_state.headers.read().await,
				&*session_state.auth.read().await,
			)
			.await?;
			let mut headers = session_state.headers.write().await;

			// Synchronize local HTTP headers with the server's session state.
			//
			// This is critical for token-based authentication flows:
			// 1. Client calls authenticate(token) - server extracts ns/db from JWT claims
			// 2. Client calls use_defaults() (USE with no args) to sync local state
			// 3. Server returns the session's ns/db (from token claims) in the response
			// 4. Client updates local headers to match, ensuring subsequent requests are routed to
			//    the correct namespace/database
			//
			// We use the server response rather than the original request parameters
			// because the server may have resolved defaults or preserved token-derived values.
			if let Some(result) = out.first()
				&& let Ok(Value::Object(ref obj)) = result.clone().result
			{
				match obj.get("namespace") {
					Some(Value::String(ns)) => {
						let header_value = HeaderValue::try_from(ns.as_str()).map_err(|_| {
							Error::internal(format!("Invalid namespace name: {ns:?}"))
						})?;
						headers.insert(&NS, header_value);
					}
					_ => {
						headers.remove(&NS);
					}
				}
				match obj.get("database") {
					Some(Value::String(db)) => {
						let header_value = HeaderValue::try_from(db.as_str()).map_err(|_| {
							Error::internal(format!("Invalid database name: {db:?}"))
						})?;
						headers.insert(&DB, header_value);
					}
					_ => {
						headers.remove(&DB);
					}
				}
			}

			Ok(out)
		}
		Command::Signup {
			credentials,
		} => {
			let req = Command::Signup {
				credentials: credentials.clone(),
			}
			.into_router_request(None, Some(session_id))
			.expect("signup should be a valid router request");

			let results = send_request(
				req,
				base_url,
				client,
				&*session_state.headers.read().await,
				&*session_state.auth.read().await,
			)
			.await?;

			// Stash the signup-issued bearer token so subsequent HTTP
			// requests carry the new credentials and pass the server's
			// per-session ownership gate. Without this the next auth call
			// would arrive anonymous against a session that the server has
			// already bound to the new record principal.
			let value = match results.first() {
				Some(result) => result.clone().result?,
				None => {
					error!("received invalid result from server");
					return Err(Error::internal("Received invalid result from server".to_string()));
				}
			};

			let token = Token::from_value(value).map_err(|e| Error::internal(e.to_string()))?;
			*session_state.auth.write().await = Some(Auth::Bearer {
				token: token.access,
			});

			Ok(results)
		}
		Command::Signin {
			credentials,
		} => {
			let req = Command::Signin {
				credentials: credentials.clone(),
			}
			.into_router_request(None, Some(session_id))
			.expect("signin should be a valid router request");

			let results = send_request(
				req,
				base_url,
				client,
				&*session_state.headers.read().await,
				&*session_state.auth.read().await,
			)
			.await?;

			let value = match results.first() {
				Some(result) => result.clone().result?,
				None => {
					error!("received invalid result from server");
					return Err(Error::internal("Received invalid result from server".to_string()));
				}
			};

			let mut auth = session_state.auth.write().await;
			match Credentials::from_value(value.clone()) {
				Ok(credentials) => {
					*auth = Some(Auth::Basic {
						user: credentials.user,
						pass: credentials.pass,
						ns: credentials.ns,
						db: credentials.db,
					});
				}
				Err(err) => {
					debug!("Error converting Value to Credentials: {err}");
					let token =
						Token::from_value(value).map_err(|e| Error::internal(e.to_string()))?;
					*auth = Some(Auth::Bearer {
						token: token.access,
					});
				}
			}

			Ok(results)
		}
		Command::Authenticate {
			token,
		} => {
			let req = Command::Authenticate {
				token: token.clone(),
			}
			.into_router_request(None, Some(session_id))
			.expect("authenticate should be a valid router request");
			let mut results = send_request(
				req,
				base_url,
				client,
				&*session_state.headers.read().await,
				&*session_state.auth.read().await,
			)
			.await?;
			if let Some(result) = results.first_mut() {
				match &mut result.result {
					Ok(result) => {
						let value = token.into_value();
						*session_state.auth.write().await = Some(Auth::Bearer {
							token: Token::from_value(value.clone())
								.map_err(|e| Error::internal(e.to_string()))?
								.access,
						});
						*result = value;
					}
					Err(error) => {
						// Automatic refresh token handling:
						// If authentication fails with "token has expired" and we have a refresh
						// token, automatically attempt to refresh the authentication and
						// update the stored auth.
						if let CoreToken::WithRefresh {
							..
						} = &token
						{
							// If the error is due to token expiration, attempt automatic refresh
							if error.not_allowed_details().is_some_and(|a| {
								matches!(a, NotAllowedError::Auth(AuthError::TokenExpired))
							}) {
								// Call the refresh_token helper to get new tokens
								let (value, refresh_results) = refresh_token(
									token,
									base_url,
									client,
									&*session_state.headers.read().await,
									&*session_state.auth.read().await,
									Some(session_id),
								)
								.await?;
								// Update the stored authentication with the new access token
								*session_state.auth.write().await = Some(Auth::Bearer {
									token: Token::from_value(value)
										.map_err(|e| Error::internal(e.to_string()))?
										.access,
								});
								// Use the refresh results (which include the new token)
								results = refresh_results;
							}
						}
					}
				}
			}
			Ok(results)
		}
		Command::Refresh {
			token,
		} => {
			let (value, results) = refresh_token(
				token,
				base_url,
				client,
				&*session_state.headers.read().await,
				&*session_state.auth.read().await,
				Some(session_id),
			)
			.await?;
			*session_state.auth.write().await = Some(Auth::Bearer {
				token: Token::from_value(value).map_err(|e| Error::internal(e.to_string()))?.access,
			});
			Ok(results)
		}
		Command::Invalidate => {
			// Send invalidate to server to clear stored session
			let req = Command::Invalidate
				.into_router_request(None, Some(session_id))
				.expect("invalidate should be a valid router request");
			let results = send_request(
				req,
				base_url,
				client,
				&*session_state.headers.read().await,
				&*session_state.auth.read().await,
			)
			.await?;
			// Then clear local auth so future requests don't include auth headers
			*session_state.auth.write().await = None;
			Ok(results)
		}
		Command::Set {
			key,
			value,
		} => {
			surrealdb_rpc::check_protected_param(&key)?;
			let req = Command::Set {
				key,
				value,
			}
			.into_router_request(None, Some(session_id))
			.expect("set should be a valid router request");
			send_request(
				req,
				base_url,
				client,
				&*session_state.headers.read().await,
				&*session_state.auth.read().await,
			)
			.await
		}
		Command::Unset {
			key,
		} => {
			surrealdb_rpc::check_protected_param(&key)?;
			let req = Command::Unset {
				key,
			}
			.into_router_request(None, Some(session_id))
			.expect("unset should be a valid router request");
			send_request(
				req,
				base_url,
				client,
				&*session_state.headers.read().await,
				&*session_state.auth.read().await,
			)
			.await
		}
		#[cfg(target_family = "wasm")]
		Command::ExportFile {
			..
		}
		| Command::ImportFile {
			..
		} => Err(Error::internal(
			"File-based export/import is not supported on this architecture. \
				 Use the byte-stream variants (e.g. `db.export().await?`) instead."
				.to_string(),
		)),

		#[cfg(not(target_family = "wasm"))]
		Command::ExportFile {
			path,
			config,
		} => {
			let req_path = base_url.join("export").map_err(crate::std_error_to_types_error)?;
			let config = config.unwrap_or_default();
			let config_value: Value = config.into_value();
			let headers = session_state.headers.read().await;
			let auth = session_state.auth.read().await;
			let request = client
				.post(req_path)
				.body(json_encode_str(config_value))
				.headers(headers.clone())
				.auth(&auth)
				.header(CONTENT_TYPE, "application/json")
				.header(ACCEPT, "application/octet-stream");
			export_file(request, path).await?;
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		Command::ExportBytes {
			bytes,
			config,
		} => {
			let req_path = base_url.join("export").map_err(crate::std_error_to_types_error)?;
			let config = config.unwrap_or_default();
			let config_value = config.into_value();
			let headers = session_state.headers.read().await;
			let auth = session_state.auth.read().await;
			let request = client
				.post(req_path)
				.body(json_encode_str(config_value))
				.headers(headers.clone())
				.auth(&auth)
				.header(CONTENT_TYPE, "application/json")
				.header(ACCEPT, "application/octet-stream");
			export_bytes(request, bytes).await?;
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		#[cfg(not(target_family = "wasm"))]
		Command::ImportFile {
			path,
		} => {
			let req_path = base_url.join("import").map_err(crate::std_error_to_types_error)?;
			let headers = session_state.headers.read().await;
			let auth = session_state.auth.read().await;
			let request = client
				.post(req_path)
				.headers(headers.clone())
				.auth(&auth)
				.header(CONTENT_TYPE, "application/octet-stream");
			import(request, path).await?;
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		Command::SubscribeLive {
			..
		} => Err(Error::internal(
			"The protocol or storage engine does not support live queries on this architecture"
				.to_string(),
		)),
		Command::Query {
			txn,
			query,
			variables,
		} => {
			let req = Command::Query {
				txn,
				query,
				variables,
			}
			.into_router_request(None, Some(session_id))
			.expect("command should convert to router request");
			send_request(
				req,
				base_url,
				client,
				&*session_state.headers.read().await,
				&*session_state.auth.read().await,
			)
			.await
		}
		cmd => {
			let req = cmd
				.into_router_request(None, Some(session_id))
				.expect("command should convert to router request");
			let res = send_request(
				req,
				base_url,
				client,
				&*session_state.headers.read().await,
				&*session_state.auth.read().await,
			)
			.await?;
			Ok(res)
		}
	}
}

#[cfg(all(test, not(target_family = "wasm")))]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::io::{Read, Write};
	use std::net::TcpListener;
	use std::thread::JoinHandle;

	use super::*;

	/// A request as the stub server received it: the lowercased head, and the
	/// body.
	struct Received {
		head: String,
		body: Vec<u8>,
	}

	/// An answer the stub server gives: a status, and a body.
	type Answer = (u16, Vec<u8>);

	/// An empty answer with `status`, which the client reads as a failure.
	fn empty(status: u16) -> Answer {
		(status, Vec::new())
	}

	/// A successful RPC answer.
	fn ok() -> Answer {
		let response = DbResponse::success(None, None, DbResult::Other(Value::None));
		(200, surrealdb_rpc::wire::encode(response.into_value()).unwrap())
	}

	/// Answers one request per entry in `answers`, each on its own
	/// connection, and returns what each request carried.
	fn stub_server(answers: Vec<Answer>) -> (Url, JoinHandle<Vec<Received>>) {
		let listener = TcpListener::bind("127.0.0.1:0").unwrap();
		let base_url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
		let server = std::thread::spawn(move || {
			let mut received = Vec::new();
			for (status, answer) in answers {
				let (mut stream, _) = listener.accept().unwrap();
				let mut data = Vec::new();
				let mut buf = [0u8; 1024];
				let head_end = loop {
					if let Some(i) = data.windows(4).position(|w| w == b"\r\n\r\n") {
						break i + 4;
					}
					let n = stream.read(&mut buf).unwrap();
					data.extend_from_slice(&buf[..n]);
				};
				let head = String::from_utf8_lossy(&data[..head_end]).to_lowercase();
				let length = head
					.lines()
					.find_map(|line| line.strip_prefix("content-length: "))
					.map_or(0, |n| n.trim().parse::<usize>().unwrap());
				while data.len() < head_end + length {
					let n = stream.read(&mut buf).unwrap();
					data.extend_from_slice(&buf[..n]);
				}
				let body = data[head_end..head_end + length].to_vec();
				let response = format!(
					"HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
					answer.len()
				);
				stream.write_all(response.as_bytes()).unwrap();
				stream.write_all(&answer).unwrap();
				received.push(Received {
					head,
					body,
				});
			}
			received
		});
		(base_url, server)
	}

	fn authenticated_session() -> SessionState {
		let session = SessionState::default();
		*session.auth.try_write().unwrap() = Some(Auth::Basic {
			user: "alice".to_owned(),
			pass: "secret".to_owned(),
			ns: None,
			db: None,
		});
		session
	}

	fn is_detach(request: &Received) -> bool {
		request.body.windows(6).any(|w| w == b"detach")
	}

	/// A request holding a regex, which plain `cbor` has no form for, is sent
	/// as a `cbor.2` body under that content type; any other stays plain `cbor`.
	#[tokio::test]
	async fn a_regex_parameter_is_sent_as_a_cbor_2_body() {
		let regex = Value::Regex("^a/b[0-9]+$".parse().unwrap());
		let (base_url, server) = stub_server(vec![ok(), ok()]);
		let client = reqwest::Client::new();
		for value in [regex.clone(), Value::from_t("plain")] {
			let request = Command::Set {
				key: "r".to_owned(),
				value,
			}
			.into_router_request(Some(1), None)
			.unwrap();
			send_request(request, &base_url, &client, &default_headers(), &None).await.unwrap();
		}
		let received = server.join().unwrap();
		fn content_types(r: &Received) -> Vec<&str> {
			r.head.lines().filter(|l| l.starts_with("content-type: ")).collect()
		}
		assert_eq!(content_types(&received[0]), ["content-type: application/cbor; v=2"]);
		assert_eq!(content_types(&received[1]), ["content-type: application/cbor"]);
		let sent = surrealdb_types::cbor::decode_v2(&received[0].body, 64, None).unwrap();
		let Value::Object(sent) = sent else {
			panic!("a request is an object");
		};
		assert_eq!(sent["params"], Value::from_t(vec![Value::from_t("r"), regex]));
	}

	/// The server only lets a session's owner detach it, so the detach sent
	/// when a session is dropped must carry that session's authentication.
	#[tokio::test]
	async fn dropping_a_session_sends_its_authentication_with_the_detach() {
		let (base_url, server) = stub_server(vec![empty(200)]);
		let state = RouterState::new(reqwest::Client::new(), base_url);
		let session_id = Uuid::new_v4();
		state.sessions.insert(session_id, Ok(Arc::new(authenticated_session())));

		state.handle_session_drop(session_id).await;

		// The detach is awaited, so it has reached the server by now.
		let received = server.join().unwrap();
		assert!(is_detach(&received[0]), "the drop sent no detach");
		assert!(received[0].head.contains("authorization: basic "), "the detach was anonymous");
		assert!(state.sessions.get(&session_id).is_none(), "the session was not released");
	}

	/// A clone whose replay fails may still hold a server session it attached
	/// before the failure, so the failure detaches it. A failed response tells
	/// the client nothing about whether the attach landed, so the detach is
	/// sent whichever request failed.
	#[tokio::test]
	async fn a_clone_whose_replay_fails_is_detached() {
		// The replay fails on its first request, then the detach.
		let (base_url, server) = stub_server(vec![empty(500), empty(200)]);
		let state = RouterState::new(reqwest::Client::new(), base_url);
		let old = Uuid::new_v4();
		let new = Uuid::new_v4();
		let session = SessionState::default();
		session.replay.push(Command::Attach {
			session_id: old,
		});
		state.sessions.insert(old, Ok(Arc::new(session)));

		state.handle_session_clone(old, new).await;

		let received = server.join().unwrap();
		assert!(matches!(state.sessions.get(&new), Some(Err(_))), "the clone should have failed");
		assert!(is_detach(&received[1]), "the failed clone was not detached");
		// No sign-in was replayed, so a session the attach created is anonymous.
		assert!(!received[1].head.contains("authorization:"), "the detach claimed a principal");
	}

	/// A replay sends each command as the principal the original session held
	/// at that point, and a failed replay detaches as the principal it reached,
	/// not as the one the copied state ends with.
	#[tokio::test]
	async fn a_failed_replay_detaches_as_the_principal_it_reached() {
		// The attach and the first authenticate succeed, the second fails.
		let (base_url, server) = stub_server(vec![ok(), ok(), empty(500), empty(200)]);
		let state = RouterState::new(reqwest::Client::new(), base_url);
		let old = Uuid::new_v4();
		let new = Uuid::new_v4();
		let session = SessionState::default();
		session.replay.push(Command::Attach {
			session_id: old,
		});
		session.replay.push(Command::Authenticate {
			token: CoreToken::Access("first".to_owned()),
		});
		session.replay.push(Command::Authenticate {
			token: CoreToken::Access("second".to_owned()),
		});
		*session.auth.try_write().unwrap() = Some(Auth::Bearer {
			token: Token::from_value(CoreToken::Access("second".to_owned()).into_value())
				.unwrap()
				.access,
		});
		state.sessions.insert(old, Ok(Arc::new(session)));

		state.handle_session_clone(old, new).await;

		let received = server.join().unwrap();
		let authorization = |i: usize| {
			received[i]
				.head
				.lines()
				.find(|line| line.starts_with("authorization:"))
				.map(str::to_owned)
		};
		assert_eq!(authorization(0), None, "the attach is sent before any sign-in");
		assert_eq!(authorization(1), None, "the first authenticate is sent before any sign-in");
		assert_eq!(authorization(2).as_deref(), Some("authorization: bearer first"));
		assert!(is_detach(&received[3]), "the failed clone was not detached");
		assert_eq!(authorization(3).as_deref(), Some("authorization: bearer first"));
	}

	/// A clone carries the credential its own replay produced, not the one
	/// copied from the session it was cloned from.
	#[tokio::test]
	async fn a_replayed_clone_keeps_the_credential_its_replay_produced() {
		let (base_url, server) = stub_server(vec![ok(), ok()]);
		let state = RouterState::new(reqwest::Client::new(), base_url);
		let old = Uuid::new_v4();
		let new = Uuid::new_v4();
		let session = SessionState::default();
		session.replay.push(Command::Attach {
			session_id: old,
		});
		session.replay.push(Command::Authenticate {
			token: CoreToken::Access("replayed".to_owned()),
		});
		*session.auth.try_write().unwrap() = Some(Auth::Bearer {
			token: Token::from_value(CoreToken::Access("copied".to_owned()).into_value())
				.unwrap()
				.access,
		});
		state.sessions.insert(old, Ok(Arc::new(session)));

		state.handle_session_clone(old, new).await;

		server.join().unwrap();
		let Some(Ok(clone)) = state.sessions.get(&new) else {
			panic!("the clone should have succeeded");
		};
		let auth = clone.auth.read().await;
		let Some(Auth::Bearer {
			token,
		}) = &*auth
		else {
			panic!("the clone should carry a bearer token");
		};
		assert_eq!(token.as_insecure_token(), "replayed");
	}

	/// A session whose setup failed and whose cleanup detach failed too is
	/// detached again when it is dropped.
	#[tokio::test]
	async fn a_failed_cleanup_detach_is_sent_again_on_drop() {
		// The attach fails, its cleanup detach fails, the drop's detach.
		let (base_url, server) = stub_server(vec![empty(500), empty(500), ok()]);
		let state = RouterState::new(reqwest::Client::new(), base_url);
		let session_id = Uuid::new_v4();

		state.handle_session_initial(session_id).await;
		assert!(matches!(state.sessions.get(&session_id), Some(Err(_))), "setup should fail");
		state.handle_session_drop(session_id).await;

		let received = server.join().unwrap();
		assert!(is_detach(&received[1]), "the failed setup was not detached");
		assert!(is_detach(&received[2]), "the failed cleanup was not retried on drop");
		assert!(state.undetached.get(&session_id).is_none(), "the retry was not forgotten");
	}

	/// A refresh leaves the session holding a token no logged command
	/// produced, so it is logged as an authenticate with that token, and a
	/// clone's replay ends with the refreshed credential.
	#[tokio::test]
	async fn a_refresh_is_replayed_as_the_token_it_left() {
		let session = SessionState::default();
		*session.auth.write().await = Some(Auth::Bearer {
			token: Token::from_value(CoreToken::Access("refreshed".to_owned()).into_value())
				.unwrap()
				.access,
		});

		record_replay(
			&session,
			Command::Refresh {
				token: CoreToken::Access("original".to_owned()),
			},
		)
		.await;

		let logged: Vec<_> = session.replay.iter().map(|(_, command)| command.clone()).collect();
		assert!(
			matches!(
				logged.as_slice(),
				[Command::Authenticate {
					token: CoreToken::Access(token),
				}] if token == "refreshed"
			),
			"logged {logged:?}"
		);
	}
}
