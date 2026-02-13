//! HTTP engine for connecting to SurrealDB over HTTP/HTTPS.
//!
//! This module provides HTTP-based connectivity to SurrealDB servers. While HTTP is
//! traditionally a stateless protocol, this implementation supports stateful sessions
//! by maintaining server-side session state and using session IDs in requests.
//!
//! # Multi-Node Deployments and Sticky Sessions
//!
//! **Important:** When deploying SurrealDB in a multi-node cluster behind a load balancer,
//! you must configure **sticky sessions** (session affinity) at the load balancer level.
//!
//! The SDK performs DNS resolution at connection time and pins all requests to a single
//! resolved IP address. However, if that IP address belongs to a load balancer, the load
//! balancer may still distribute requests to different backend nodes unless sticky sessions
//! are configured.
//!
//! ## Why Sticky Sessions Are Required
//!
//! SurrealDB maintains session state on the server, including:
//! - Authentication state
//! - Selected namespace and database
//! - Session variables set via `LET`/`SET`
//! - Transaction context
//!
//! If requests for a single SDK session are routed to different server nodes, those nodes
//! will not have the session state, resulting in "session not found" errors.
//!
//! ## Recommendations
//!
//! - **Single-node deployments:** No special configuration needed.
//! - **Multi-node with load balancer:** Configure sticky sessions/session affinity based on client
//!   IP or a session cookie.
//! - **Multi-node clusters:** Consider using [WebSocket connections](`super::ws`) instead, which
//!   maintain a persistent connection to a single node naturally.
//!
//! ## Example: Connecting via HTTP
//!
//! ```no_run
//! use surrealdb::Surreal;
//! use surrealdb::engine::remote::http::Http;
//!
//! # #[tokio::main]
//! # async fn main() -> surrealdb::Result<()> {
//! // Connect to a single server or load balancer with sticky sessions configured
//! let db = Surreal::new::<Http>("localhost:8000").await?;
//! # Ok(())
//! # }
//! ```
//!
//! For multi-node deployments without sticky session support at the infrastructure level,
//! prefer WebSocket connections which maintain session affinity through persistent connections.

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
use surrealdb_core::dbs::{QueryResult, QueryResultBuilder};
use surrealdb_core::iam::Token as CoreToken;
use surrealdb_core::rpc::{self, DbResponse, DbResult};
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
use crate::engine::remote::RouterRequest;
use crate::err::Error;
use crate::headers::{AUTH_DB, AUTH_NS, DB, NS};
use crate::opt::IntoEndpoint;
use crate::opt::auth::{AccessToken, Token};
use crate::types::{HashMap, SurrealValue, Value};
use crate::{Connect, Result, Surreal};

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
			client,
			base_url,
		}
	}

	/// Replay all stored commands for a session (attach + any Use/Signin/etc.)
	async fn replay_session(&self, session_id: Uuid, session_state: &SessionState) -> Result<()> {
		// Clone headers and auth upfront to avoid holding the lock across network I/O
		let headers = session_state.headers.read().await.clone();
		let auth = session_state.auth.read().await.clone();

		for (_, command) in &session_state.replay {
			let request = command
				.clone()
				.into_router_request(None, Some(session_id))
				.expect("replay command should convert to router request");

			send_request(request, &self.base_url, &self.client, &headers, &auth).await?;
		}
		Ok(())
	}

	/// Handle a new session being created.
	async fn handle_session_initial(&self, session_id: Uuid) {
		let session_state = SessionState::default();
		session_state.replay.push(Command::Attach {
			session_id,
		});
		let session_state = Arc::new(session_state);
		self.sessions.insert(session_id, Ok(session_state.clone()));

		if let Err(error) = self.replay_session(session_id, &session_state).await {
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
				self.sessions.insert(new, Ok(new_state.clone()));

				if let Err(error) = self.replay_session(new, &new_state).await {
					self.sessions.insert(new, Err(SessionError::Remote(error.to_string())));
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

	/// Handle a session being dropped.
	async fn handle_session_drop(&self, session_id: Uuid) {
		if self.sessions.get(&session_id).is_some() {
			let session_state = SessionState::default();
			session_state.replay.push(Command::Detach {
				session_id,
			});
			self.replay_session(session_id, &session_state).await.ok();
		}
		self.sessions.remove(&session_id);
	}
}

/// The HTTP scheme used to connect to `http://` endpoints.
///
/// Use this for unencrypted connections to SurrealDB servers. For production
/// deployments, consider using [`Https`] instead for encrypted connections.
///
/// # Multi-Node Deployments
///
/// When connecting to a load balancer in front of multiple SurrealDB nodes,
/// ensure sticky sessions are configured. See the [module documentation](self)
/// for details.
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
/// When connecting to a load balancer in front of multiple SurrealDB nodes,
/// ensure sticky sessions are configured. See the [module documentation](self)
/// for details.
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
/// over HTTP. The client pins connections to a specific server IP address
/// resolved at connection time to ensure session consistency.
///
/// # Session Affinity
///
/// All requests from this client are routed to the same server node (determined
/// by DNS resolution at connection time). However, if connecting through a load
/// balancer, sticky sessions must be configured at the load balancer level.
/// See the [module documentation](self) for details.
///
/// # Comparison with WebSocket
///
/// | Feature | HTTP | WebSocket |
/// |---------|------|-----------|
/// | Live queries | ❌ Not supported | ✅ Supported |
/// | Connection persistence | Per-request | Persistent |
/// | Proxy compatibility | Excellent | May require configuration |
/// | Multi-node clusters | Requires sticky sessions | Natural session affinity |
///
/// For multi-node deployments or when live queries are needed, consider using
/// [WebSocket connections](`super::ws::Client`) instead.
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
			surreal: self.inner.clone().into(),
			address: address.into_endpoint(),
			capacity: 0,
			response_type: PhantomData,
		}
	}
}

pub(crate) fn default_headers() -> HeaderMap {
	let mut headers = HeaderMap::new();
	headers.insert(ACCEPT, HeaderValue::from_static(surrealdb_core::api::format::FLATBUFFERS));
	headers
		.insert(CONTENT_TYPE, HeaderValue::from_static(surrealdb_core::api::format::FLATBUFFERS));
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
		.await?
		.error_for_status()?
		.bytes_stream()
		.map_err(futures::io::Error::other)
		.into_async_read()
		.compat();
	let mut file =
		match OpenOptions::new().write(true).create(true).truncate(true).open(&path).await {
			Ok(path) => path,
			Err(error) => {
				return Err(Error::FileOpen {
					path,
					error,
				});
			}
		};
	if let Err(error) = io::copy(&mut response, &mut file).await {
		return Err(Error::FileRead {
			path,
			error,
		});
	}

	Ok(())
}

async fn export_bytes(request: RequestBuilder, bytes: BackupSender) -> Result<()> {
	let response = request.send().await?.error_for_status()?;

	let future = async move {
		let mut response = response.bytes_stream();
		while let Ok(Some(b)) = response.try_next().await {
			if bytes.send(Ok(b.to_vec())).await.is_err() {
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
			return Err(Error::FileOpen {
				path,
				error,
			});
		}
	};

	let res =
		request.header(ACCEPT, surrealdb_core::api::format::FLATBUFFERS).body(file).send().await?;

	if res.error_for_status_ref().is_err() {
		let res = res.text().await?;

		match res.parse::<serde_json::Value>() {
			Ok(body) => {
				let error_msg = format!(
					"\n{}",
					serde_json::to_string_pretty(&body).unwrap_or_else(|_| "{}".into())
				);
				return Err(Error::Http(error_msg));
			}
			Err(_) => {
				return Err(Error::Http(res));
			}
		}
	}

	let bytes = res.bytes().await?;

	let value: Value = surrealdb_core::rpc::format::flatbuffers::decode(&bytes)
		.map_err(|x| format!("Failed to deserialize flatbuffers payload: {x:?}"))
		.map_err(crate::Error::InvalidResponse)?;

	// Convert Value::Array to Vec<QueryResult>
	let Value::Array(arr) = value else {
		return Err(Error::InvalidResponse("Expected array response from import".to_string()));
	};

	for val in arr.into_vec() {
		let result = QueryResult::from_value(val)
			.map_err(|e| Error::InvalidResponse(format!("Failed to parse query result: {e}")))?;
		if let Err(e) = result.result {
			return Err(Error::Query(e.to_string()));
		}
	}

	Ok(())
}

pub(crate) async fn health(request: RequestBuilder) -> Result<()> {
	request.send().await?.error_for_status()?;
	Ok(())
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
	let body = surrealdb_core::rpc::format::flatbuffers::encode(&req_value)
		.map_err(|x| format!("Failed to serialize to flatbuffers: {x}"))
		.map_err(crate::Error::UnserializableValue)?;

	// Include auth header so the server can authenticate the request and maintain
	// session state. This is essential for token-based auth flows where the server
	// extracts namespace/database from JWT claims during authenticate().
	let http_req = client.post(url).headers(headers.clone()).auth(auth).body(body);
	let response = http_req.send().await?.error_for_status()?;
	let bytes = response.bytes().await?;

	let response: DbResponse = surrealdb_core::rpc::format::flatbuffers::decode(&bytes)
		.map_err(|x| format!("Failed to deserialize flatbuffers payload: {x}"))
		.map_err(crate::Error::InvalidResponse)?;

	match response.result? {
		DbResult::Query(results) => Ok(results),
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
			return Err(Error::InternalError("Received invalid result from server".to_string()));
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
						let header_value = HeaderValue::try_from(ns.as_str())
							.map_err(|_| Error::InvalidNsName(ns.clone()))?;
						headers.insert(&NS, header_value);
					}
					_ => {
						headers.remove(&NS);
					}
				}
				match obj.get("database") {
					Some(Value::String(db)) => {
						let header_value = HeaderValue::try_from(db.as_str())
							.map_err(|_| Error::InvalidDbName(db.clone()))?;
						headers.insert(&DB, header_value);
					}
					_ => {
						headers.remove(&DB);
					}
				}
			}

			Ok(out)
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
					return Err(Error::InternalError(
						"Received invalid result from server".to_string(),
					));
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
					let token = Token::from_value(value)?;
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
							token: Token::from_value(value.clone())?.access,
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
							if error.auth_details().is_some_and(|a| {
								matches!(a, surrealdb_types::AuthError::TokenExpired)
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
									token: Token::from_value(value)?.access,
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
				token: Token::from_value(value)?.access,
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
			surrealdb_core::rpc::check_protected_param(&key)?;
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
		| Command::ExportMl {
			..
		}
		| Command::ImportFile {
			..
		}
		| Command::ImportMl {
			..
		} => {
			// TODO: Better error message here, some backups are supported
			Err(Error::BackupsNotSupported.into())
		}

		#[cfg(not(target_family = "wasm"))]
		Command::ExportFile {
			path,
			config,
		} => {
			let req_path = base_url.join("export")?;
			let config = config.unwrap_or_default();
			let config_value: Value = config.into_value();
			let headers = session_state.headers.read().await;
			let auth = session_state.auth.read().await;
			let request = client
				.post(req_path)
				.body(
					rpc::format::json::encode_str(config_value)
						.map_err(|e| Error::SerializeValue(e.to_string()))?,
				)
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
			let req_path = base_url.join("export")?;
			let config = config.unwrap_or_default();
			let config_value = config.into_value();
			let headers = session_state.headers.read().await;
			let auth = session_state.auth.read().await;
			let request = client
				.post(req_path)
				.body(
					rpc::format::json::encode_str(config_value)
						.map_err(|e| Error::SerializeValue(e.to_string()))?,
				)
				.headers(headers.clone())
				.auth(&auth)
				.header(CONTENT_TYPE, "application/json")
				.header(ACCEPT, "application/octet-stream");
			export_bytes(request, bytes).await?;
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		#[cfg(not(target_family = "wasm"))]
		Command::ExportMl {
			path,
			config,
		} => {
			let req_path =
				base_url.join("ml")?.join("export")?.join(&config.name)?.join(&config.version)?;
			let headers = session_state.headers.read().await;
			let auth = session_state.auth.read().await;
			let request = client
				.get(req_path)
				.headers(headers.clone())
				.auth(&auth)
				.header(ACCEPT, "application/octet-stream");
			export_file(request, path).await?;
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		Command::ExportBytesMl {
			bytes,
			config,
		} => {
			let req_path =
				base_url.join("ml")?.join("export")?.join(&config.name)?.join(&config.version)?;
			let headers = session_state.headers.read().await;
			let auth = session_state.auth.read().await;
			let request = client
				.get(req_path)
				.headers(headers.clone())
				.auth(&auth)
				.header(ACCEPT, "application/octet-stream");
			export_bytes(request, bytes).await?;
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		#[cfg(not(target_family = "wasm"))]
		Command::ImportFile {
			path,
		} => {
			let req_path = base_url.join("import")?;
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
		#[cfg(not(target_family = "wasm"))]
		Command::ImportMl {
			path,
		} => {
			let req_path = base_url.join("ml")?.join("import")?;
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
		} => Err(Error::LiveQueriesNotSupported),
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
