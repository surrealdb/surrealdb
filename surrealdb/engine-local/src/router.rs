//! Per-session state and the command dispatcher that drives the datastore.

#[cfg(not(target_family = "wasm"))]
use std::pin::pin;
use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use std::task::{Poll, ready};
#[cfg(not(target_family = "wasm"))]
use std::{future::Future, path::PathBuf};

use async_channel::{Receiver, Sender};
#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use futures::StreamExt;
#[cfg(not(target_family = "wasm"))]
use futures::stream::poll_fn;
use surrealdb_core::dbs::{QueryResult, QueryResultBuilder, Session};
use surrealdb_core::iam;
#[cfg(not(target_family = "wasm"))]
use surrealdb_core::kvs::export::Config as DbExportConfig;
use surrealdb_core::kvs::{Datastore, Transaction, TransactionType};
#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use surrealdb_core::{
	iam::{Action, ResourceKind, check::check_ns_db},
	ml::storage::surml_file::SurMlFile,
};
#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use surrealdb_engine_api::MlExportConfig;
use surrealdb_engine_api::{Command, SessionError, SessionId};
use surrealdb_types::{Error, HashMap, Notification, SurrealValue, ToSql, Value, Variables};
use tokio::sync::RwLock;
#[cfg(not(target_family = "wasm"))]
use tokio::{
	fs::OpenOptions,
	io::{self, AsyncReadExt, AsyncWriteExt},
};
#[cfg(not(target_family = "wasm"))]
use tokio_util::bytes::BytesMut;
use uuid::Uuid;

use crate::std_error_to_types_error;

pub(crate) struct RouterState {
	pub(crate) kvs: Arc<Datastore>,
	pub(crate) sessions: HashMap<Uuid, SessionResult>,
}

impl RouterState {
	pub(crate) fn new(kvs: Arc<Datastore>) -> Self {
		Self {
			kvs,
			sessions: HashMap::new(),
		}
	}

	/// Handle a new session being created.
	fn handle_session_initial(&self, session_id: Uuid) {
		self.sessions.insert(session_id, Ok(Arc::new(SessionState::new(session_id))));
	}

	/// Handle a session being cloned.
	async fn handle_session_clone(&self, old: Uuid, new: Uuid) {
		let state = match self.sessions.get(&old) {
			Some(Ok(state)) => {
				let mut session = state.session.read().await.clone();
				session.id = Some(new);
				Ok(Arc::new(SessionState {
					session: RwLock::new(session),
					vars: RwLock::new(state.vars.read().await.clone()),
					transactions: HashMap::new(),
					live_queries: HashMap::new(),
				}))
			}
			Some(Err(error)) => Err(error),
			None => Err(SessionError::NotFound(old)),
		};
		self.sessions.insert(new, state);
	}

	/// Handle a session being dropped.
	fn handle_session_drop(&self, session_id: Uuid) {
		self.sessions.remove(&session_id);
	}

	/// Dispatch a session-lifecycle event to the appropriate handler.
	pub(crate) async fn handle_session(&self, session_id: SessionId) {
		match session_id {
			SessionId::Initial(id) => self.handle_session_initial(id),
			SessionId::Clone {
				old,
				new,
			} => self.handle_session_clone(old, new).await,
			SessionId::Drop(id) => self.handle_session_drop(id),
		}
	}

	/// Resolve the session a route targets, after first applying any
	/// session-lifecycle events that were enqueued before it.
	///
	/// Session lifecycle (`Initial`/`Clone`/`Drop`) travels on a channel that is
	/// separate from the route channel, so a freshly registered or cloned session
	/// may not have been applied yet when its first query arrives. Receiving the
	/// route establishes a happens-before with the sender, so a lifecycle event
	/// enqueued before the query is now observable and a single non-blocking drain
	/// pass is sufficient — without it the lookup can spuriously miss and return
	/// [`SessionError::NotFound`] for a session that is in fact registered.
	pub(crate) async fn resolve_route_session(
		&self,
		session_rx: &Receiver<SessionId>,
		session_id: Uuid,
	) -> SessionResult {
		while let Ok(event) = session_rx.try_recv() {
			self.handle_session(event).await;
		}
		match self.sessions.get(&session_id) {
			Some(result) => result,
			None => Err(SessionError::NotFound(session_id)),
		}
	}
}

pub(crate) type SessionResult = Result<Arc<SessionState>, SessionError>;

/// Per-session state for local/embedded connections
pub(crate) struct SessionState {
	pub(crate) session: RwLock<Session>,
	pub(crate) vars: RwLock<Variables>,
	transactions: HashMap<Uuid, Arc<Transaction>>,
	pub(crate) live_queries: HashMap<Uuid, Sender<Result<Notification, Error>>>,
}

impl SessionState {
	fn new(id: Uuid) -> Self {
		let mut session = Session::default().with_rt(true);
		session.id = Some(id);
		Self {
			session: RwLock::new(session),
			vars: RwLock::new(Variables::default()),
			transactions: HashMap::new(),
			live_queries: HashMap::new(),
		}
	}
}

#[cfg(not(target_family = "wasm"))]
async fn export_file(
	kvs: &Datastore,
	sess: &Session,
	chn: async_channel::Sender<Vec<u8>>,
	config: Option<DbExportConfig>,
) -> Result<(), Error> {
	let res = match config {
		Some(config) => {
			kvs.export_with_config(sess, chn, config).await.map_err(std_error_to_types_error)?.await
		}
		None => kvs.export(sess, chn).await.map_err(std_error_to_types_error)?.await,
	};

	if let Err(error) = res {
		// Check if this is a channel error by examining the error message
		let error_str = error.to_string();
		if error_str.contains("channel") || error_str.contains("Channel") {
			// This is not really an error. Just logging it for improved visibility.
			trace!("{error_str}");
			return Ok(());
		}

		return Err(Error::internal(error.to_string()));
	}
	Ok(())
}

#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
async fn export_ml(
	kvs: &Datastore,
	sess: &Session,
	chn: async_channel::Sender<Vec<u8>>,
	MlExportConfig {
		name,
		version,
	}: MlExportConfig,
) -> Result<(), Error> {
	let (nsv, dbv) = check_ns_db(sess).map_err(|e| Error::internal(e.to_string()))?;
	// Check the permissions level
	kvs.check(sess, Action::View, ResourceKind::Model.on_db(&nsv, &dbv))
		.map_err(|e| Error::internal(e.to_string()))?;

	// Attempt to get the model definition
	let Some(model) = kvs
		.get_db_model(&nsv, &dbv, &name, &version)
		.await
		.map_err(|e| Error::internal(e.to_string()))?
	else {
		// Attempt to get the model definition
		return Err(Error::not_found("Model not found".to_string(), None));
	};
	// Export the file data in to the store
	let mut data = surrealdb_core::obs::stream(model.hash.to_string())
		.await
		.map_err(|e| Error::internal(e.to_string()))?;
	// Process all stream values
	while let Some(Ok(bytes)) = data.next().await {
		if chn.send(bytes.to_vec()).await.is_err() {
			break;
		}
	}
	Ok(())
}

#[cfg(not(target_family = "wasm"))]
async fn copy<'a, R, W>(path: PathBuf, reader: &'a mut R, writer: &'a mut W) -> Result<(), Error>
where
	R: tokio::io::AsyncRead + Unpin + ?Sized,
	W: tokio::io::AsyncWrite + Unpin + ?Sized,
{
	io::copy(reader, writer)
		.await
		.map(|_| ())
		.map_err(|error| Error::internal(format!("Failed to read `{}`: {}", path.display(), error)))
}

pub(crate) async fn kill_live_query(
	kvs: &Datastore,
	id: Uuid,
	session: &Session,
	vars: Variables,
) -> Result<Vec<QueryResult>, Error> {
	let sql = format!("KILL {id}");

	let results = kvs.execute(&sql, session, Some(vars)).await?;
	Ok(results)
}

/// Rejects a command that arrives on a session whose authentication has expired.
///
/// `Datastore::execute` refuses an expired session itself, so the query path is
/// covered wherever it is reached from. This is for the commands that change
/// session state without going through it.
async fn ensure_session_active(state: &SessionState) -> Result<(), Error> {
	if state.session.read().await.expired() {
		return Err(surrealdb_core::rpc::session_expired());
	}
	Ok(())
}

pub(crate) async fn router(
	kvs: &Arc<Datastore>,
	state: &SessionState,
	command: Command,
) -> Result<Vec<QueryResult>, Error> {
	match command {
		Command::Use {
			namespace,
			database,
		} => {
			let result = {
				kvs.process_use(None, &mut *state.session.write().await, namespace, database)
					.await?
			};
			Ok(vec![result])
		}
		Command::Signup {
			credentials,
		} => {
			let query_result = QueryResultBuilder::started_now();
			let token = {
				iam::signup::signup(kvs, &mut *state.session.write().await, credentials.into())
					.await
					.map_err(surrealdb_core::err::anyhow_to_types_error)?
			};
			let result = query_result.finish_with_result(Ok(token.into_value()));
			Ok(vec![result])
		}
		Command::Signin {
			credentials,
		} => {
			let query_result = QueryResultBuilder::started_now();
			let token = {
				iam::signin::signin(kvs, &mut *state.session.write().await, credentials.into())
					.await
					.map_err(surrealdb_core::err::anyhow_to_types_error)?
			};
			let result = query_result.finish_with_result(Ok(token.into_value()));
			Ok(vec![result])
		}
		Command::Authenticate {
			token,
		} => {
			let query_result = QueryResultBuilder::started_now();
			// Extract the access token and check if this token supports refresh
			let (access, with_refresh) = match &token {
				iam::Token::Access(access) => (access, false),
				iam::Token::WithRefresh {
					access,
					..
				} => (access, true),
			};
			// Attempt to authenticate with the access token
			let result = {
				match iam::verify::token(kvs, &mut *state.session.write().await, access).await {
					// Authentication successful - return the original token
					Ok(_) => query_result.finish_with_result(Ok(token.into_value())),
					Err(error) => {
						// Automatic refresh token handling:
						// If the access token is expired and we have a refresh token,
						// automatically attempt to refresh and return new tokens.
						if with_refresh && iam::is_expired_token_error(&error) {
							let result = match iam::token::refresh(
								token,
								kvs,
								&mut *state.session.write().await,
							)
							.await
							{
								Ok(token) => {
									query_result.finish_with_result(Ok(token.into_value()))
								}
								Err(error) => query_result
									.finish_with_result(Err(Error::internal(error.to_string()))),
							};
							return Ok(vec![result]);
						}
						// If authentication failed and automatic refresh isn't applicable,
						// return the authentication error
						query_result.finish_with_result(Err(Error::internal(error.to_string())))
					}
				}
			};
			Ok(vec![result])
		}
		Command::Refresh {
			token,
		} => {
			// Refresh command: Exchange a refresh token for new access and refresh tokens
			let query_result = QueryResultBuilder::started_now();
			let result = {
				match iam::token::refresh(token, kvs, &mut *state.session.write().await).await {
					Ok(token) => query_result.finish_with_result(Ok(token.into_value())),
					Err(error) => {
						query_result.finish_with_result(Err(Error::internal(error.to_string())))
					}
				}
			};
			Ok(vec![result])
		}
		Command::Invalidate => {
			let query_result = QueryResultBuilder::started_now();
			let result = {
				match iam::clear::clear(&mut *state.session.write().await) {
					Ok(_) => query_result.finish_with_result(Ok(Value::None)),
					Err(error) => {
						query_result.finish_with_result(Err(Error::internal(error.to_string())))
					}
				}
			};
			Ok(vec![result])
		}
		Command::Begin => {
			let query_result = QueryResultBuilder::started_now();
			let result = match kvs.transaction(TransactionType::Write).await {
				Ok(txn) => {
					let id = Uuid::now_v7();
					state.transactions.insert(id, Arc::new(txn));
					query_result.finish_with_result(Ok(Value::Uuid(id.into())))
				}
				Err(error) => {
					query_result.finish_with_result(Err(Error::internal(error.to_string())))
				}
			};
			Ok(vec![result])
		}
		Command::Revoke {
			token,
		} => {
			// Revoke command: Explicitly invalidate a refresh token to prevent future use
			let query_result = QueryResultBuilder::started_now();
			let result = match iam::token::revoke_refresh_token(token, kvs).await {
				Ok(_) => query_result.finish_with_result(Ok(Value::None)),
				Err(error) => {
					query_result.finish_with_result(Err(Error::internal(error.to_string())))
				}
			};
			Ok(vec![result])
		}
		Command::Rollback {
			txn,
		} => {
			if let Some(tx) = state.transactions.get(&txn) {
				state.transactions.remove(&txn);
				tx.cancel().await.map_err(std_error_to_types_error)?;
			}
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		Command::Commit {
			txn,
		} => {
			if let Some(tx) = state.transactions.get(&txn) {
				state.transactions.remove(&txn);
				tx.commit().await.map_err(std_error_to_types_error)?;
			}
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		Command::Query {
			txn,
			query,
			variables,
		} => {
			// Merge session vars with query vars
			let mut vars = state.vars.read().await.clone();
			vars.extend(variables);

			// If a transaction UUID is provided, we need to retrieve it and use it
			let response = if let Some(txn_id) = txn {
				// Retrieve the transaction from storage
				let tx_option = state.transactions.get(&txn_id);
				if let Some(tx) = tx_option {
					// Execute with the existing transaction
					kvs.execute_with_transaction(
						query.as_ref(),
						&*state.session.read().await,
						Some(vars),
						tx,
					)
					.await?
				} else {
					// Transaction not found - return error
					return Ok(vec![QueryResultBuilder::started_now().finish_with_result(Err(
						Error::not_found(
							"Transaction not found".to_string(),
							Some(surrealdb_types::NotFoundError::Transaction),
						),
					))]);
				}
			} else {
				// No transaction - use normal execution
				kvs.execute(query.as_ref(), &*state.session.read().await, Some(vars)).await?
			};

			Ok(response)
		}
		Command::QueryStream {
			txn,
			query,
			variables,
			items,
		} => {
			// Merge session vars with query vars
			let mut vars = state.vars.read().await.clone();
			vars.extend(variables);

			// Preparing the job is what can fail synchronously -- a parse error,
			// an expired session -- so it happens while the session guard is
			// held. The job itself owns everything it needs from here.
			let job = {
				let session = state.session.read().await;
				if let Some(txn_id) = txn {
					let Some(tx) = state.transactions.get(&txn_id) else {
						return Ok(vec![QueryResultBuilder::started_now().finish_with_result(
							Err(Error::not_found(
								"Transaction not found".to_string(),
								Some(surrealdb_types::NotFoundError::Transaction),
							)),
						)]);
					};
					kvs.execute_stream_with_transaction(
						query.as_ref(),
						&session,
						Some(vars),
						tx,
						None,
						items,
					)?
				} else {
					kvs.execute_stream(query.as_ref(), &session, Some(vars), None, items)?
				}
			};

			// The caller drains the item channel, so driving the execution here
			// is what lets the two make progress against each other. The
			// results come back only so the execution's own failures do; the
			// rows have already gone to the caller.
			job.run.await?;
			Ok(Vec::new())
		}

		#[cfg(target_family = "wasm")]
		Command::ExportFile {
			..
		}
		| Command::ExportBytes {
			..
		}
		| Command::ImportFile {
			..
		} => Err(Error::internal(
			"The protocol or storage engine does not support backups on this architecture"
				.to_string(),
		)),

		#[cfg(any(target_family = "wasm", not(feature = "ml")))]
		Command::ExportMl {
			..
		}
		| Command::ExportBytesMl {
			..
		}
		| Command::ImportMl {
			..
		} => Err(Error::internal(
			"The protocol or storage engine does not support backups on this architecture"
				.to_string(),
		)),

		#[cfg(not(target_family = "wasm"))]
		Command::ExportFile {
			path: file,
			config,
		} => {
			let query_result = QueryResultBuilder::started_now();

			let (tx, rx) = async_channel::bounded(1);
			let (mut writer, mut reader) = io::duplex(10_240);

			// Write to channel.
			let session = state.session.read().await.clone();
			let export = export_file(kvs, &session, tx, config);

			// Read from channel and write to pipe.
			let bridge = async move {
				while let Ok(value) = rx.recv().await {
					if writer.write_all(&value).await.is_err() {
						// Broken pipe. Let either side's error be propagated.
						break;
					}
				}
				Ok(())
			};

			// Output to stdout or file.
			let mut output = match OpenOptions::new()
				.write(true)
				.create(true)
				.truncate(true)
				.open(&file)
				.await
			{
				Ok(path) => path,
				Err(error) => {
					return Err(Error::internal(format!(
						"Failed to open `{}`: {}",
						file.display(),
						error
					)));
				}
			};

			// Copy from pipe to output.
			let copy = copy(file, &mut reader, &mut output);

			tokio::try_join!(export, bridge, copy)?;
			Ok(vec![query_result.finish()])
		}

		#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
		Command::ExportMl {
			path,
			config,
		} => {
			let query_result = QueryResultBuilder::started_now();
			let (tx, rx) = async_channel::bounded(1);
			let (mut writer, mut reader) = io::duplex(10_240);

			// Write to channel.
			let session = state.session.read().await;
			let export = export_ml(kvs, &session, tx, config);

			// Read from channel and write to pipe.
			let bridge = async move {
				while let Ok(value) = rx.recv().await {
					if writer.write_all(&value).await.is_err() {
						// Broken pipe. Let either side's error be propagated.
						break;
					}
				}
				Ok(())
			};

			// Output to stdout or file.
			let mut output = match OpenOptions::new()
				.write(true)
				.create(true)
				.truncate(true)
				.open(&path)
				.await
			{
				Ok(path) => path,
				Err(error) => {
					return Err(Error::internal(format!(
						"Failed to open `{}`: {}",
						path.display(),
						error
					)));
				}
			};

			// Copy from pipe to output.
			let copy = copy(path, &mut reader, &mut output);

			tokio::try_join!(export, bridge, copy)?;
			Ok(vec![query_result.finish()])
		}

		#[cfg(not(target_family = "wasm"))]
		Command::ExportBytes {
			bytes,
			config,
		} => {
			let query_result = QueryResultBuilder::started_now();
			let (tx, rx) = async_channel::bounded(1);

			let kvs = Arc::clone(kvs);
			let session = state.session.read().await.clone();
			tokio::spawn(async move {
				let export = async {
					if let Err(error) = export_file(&kvs, &session, tx, config).await {
						bytes.send(Err(error)).await.ok();
					}
				};

				let bridge = async {
					while let Ok(b) = rx.recv().await {
						if bytes.send(Ok(b)).await.is_err() {
							break;
						}
					}
				};

				tokio::join!(export, bridge);
			});
			Ok(vec![query_result.finish()])
		}
		#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
		Command::ExportBytesMl {
			bytes,
			config,
		} => {
			let query_result = QueryResultBuilder::started_now();
			let (tx, rx) = async_channel::bounded(1);

			let kvs = Arc::clone(kvs);
			let session = state.session.read().await.clone();
			tokio::spawn(async move {
				let export = async {
					if let Err(error) = export_ml(&kvs, &session, tx, config).await {
						bytes.send(Err(error)).await.ok();
					}
				};

				let bridge = async {
					while let Ok(b) = rx.recv().await {
						if bytes.send(Ok(b)).await.is_err() {
							break;
						}
					}
				};

				tokio::join!(export, bridge);
			});

			Ok(vec![query_result.finish()])
		}
		#[cfg(not(target_family = "wasm"))]
		Command::ImportFile {
			path,
		} => {
			let query_result = QueryResultBuilder::started_now();
			let file = match OpenOptions::new().read(true).open(&path).await {
				Ok(path) => path,
				Err(error) => {
					return Err(Error::internal(format!(
						"Failed to open `{}`: {}",
						path.display(),
						error
					)));
				}
			};

			let mut file = pin!(file);
			let mut buffer = BytesMut::with_capacity(4096);

			let stream = poll_fn(|ctx| {
				// Doing it this way optimizes allocation.
				// It is highly likely that the buffer we return from this stream will be
				// dropped between calls to this function.
				// If this is the case than instead of allocating new memory the call to reserve
				// will instead reclaim the existing used memory.
				if buffer.capacity() == 0 {
					buffer.reserve(4096);
				}

				let future = pin!(file.read_buf(&mut buffer));
				match ready!(future.poll(ctx)) {
					Ok(0) => Poll::Ready(None),
					Ok(_) => Poll::Ready(Some(Ok(buffer.split().freeze()))),
					Err(e) => Poll::Ready(Some(Err(anyhow::anyhow!("{}", e)))),
				}
			});

			let responses = kvs
				.execute_import(
					&*state.session.read().await,
					Some(state.vars.read().await.clone()),
					stream,
				)
				.await
				.map_err(std_error_to_types_error)?;

			for response in responses {
				response.result?;
			}

			Ok(vec![query_result.finish()])
		}
		#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
		Command::ImportMl {
			path,
		} => {
			let query_result = QueryResultBuilder::started_now();
			let mut file = match OpenOptions::new().read(true).open(&path).await {
				Ok(path) => path,
				Err(error) => {
					return Err(Error::internal(format!(
						"Failed to open `{}`: {}",
						path.display(),
						error
					)));
				}
			};

			// Ensure a NS and DB are set
			let (nsv, dbv) =
				check_ns_db(&*state.session.read().await).map_err(std_error_to_types_error)?;
			// Check the permissions level
			kvs.check(
				&*state.session.read().await,
				Action::Edit,
				ResourceKind::Model.on_db(&nsv, &dbv),
			)
			.map_err(std_error_to_types_error)?;
			// Create a new buffer
			let mut buffer = Vec::new();
			// Load all the uploaded file chunks
			if let Err(error) = file.read_to_end(&mut buffer).await {
				return Err(Error::internal(format!(
					"Failed to read `{}`: {}",
					path.display(),
					error
				)));
			}
			// Check that the SurrealML file is valid
			let file = match SurMlFile::from_bytes(buffer) {
				Ok(file) => file,
				Err(error) => {
					return Err(Error::internal(format!(
						"Invalid SurrealML file: {}",
						error.message
					)));
				}
			};
			// Convert the file back in to raw bytes
			let data = file.to_bytes();

			kvs.put_ml_model(
				&*state.session.read().await,
				&file.header.name.to_string(),
				&file.header.version.to_string(),
				&file.header.description.to_string(),
				data,
			)
			.await
			.map_err(std_error_to_types_error)?;

			Ok(vec![query_result.finish()])
		}
		Command::Health => Ok(vec![QueryResultBuilder::instant_none()]),
		Command::Version => {
			let query_result = QueryResultBuilder::started_now();
			Ok(vec![
				query_result.finish_with_result(Ok(Value::from_t(
					surrealdb_core::env::VERSION.to_string(),
				))),
			])
		}
		Command::Set {
			key,
			value,
		} => {
			let query_result = QueryResultBuilder::started_now();
			// SECURITY: an expired session must not be able to change session
			// state, the same as over the RPC transports. `SECURITY_GUIDE.md`
			// section 3 requires expiry to be checked before any statement or
			// method is processed; `Datastore::execute` covers the query path,
			// so without this the session methods are the way past it.
			ensure_session_active(state).await?;
			surrealdb_core::rpc::check_protected_param(&key)
				.map_err(|e| Error::internal(e.to_string()))?;
			// Need to compute because certain keys might not be allowed to be set and those
			// should be rejected by an error.
			match value {
				Value::None => state.vars.write().await.remove(&key),
				v => state.vars.write().await.insert(key, v),
			};

			Ok(vec![query_result.finish()])
		}
		Command::Unset {
			key,
		} => {
			let query_result = QueryResultBuilder::started_now();
			ensure_session_active(state).await?;
			state.vars.write().await.remove(&key);
			Ok(vec![query_result.finish()])
		}
		Command::SubscribeLive {
			uuid,
			notification_sender,
		} => {
			let query_result = QueryResultBuilder::started_now();
			state.live_queries.insert(uuid, notification_sender);
			Ok(vec![query_result.finish()])
		}
		Command::Kill {
			uuid,
		} => {
			state.live_queries.remove(&uuid);
			let results = kill_live_query(
				kvs,
				uuid,
				&*state.session.read().await,
				state.vars.read().await.clone(),
			)
			.await?;
			Ok(results)
		}

		Command::Run {
			name,
			version,
			args,
		} => {
			// Format arguments as comma-separated SQL values
			let formatted_args = args.iter().map(|v| v.to_sql()).collect::<Vec<_>>().join(", ");

			// Build SQL query: name<version>(args) or name(args)
			let sql = match version {
				Some(v) => format!("{name}<{v}>({formatted_args})"),
				None => format!("{name}({formatted_args})"),
			};

			// Execute the query
			let results = kvs
				.execute(&sql, &*state.session.read().await, Some(state.vars.read().await.clone()))
				.await?;
			Ok(results)
		}
		Command::Attach {
			..
		} => {
			// Local engines don't use remote sessions, so attach is a no-op
			let query_result = QueryResultBuilder::started_now();
			Ok(vec![query_result.finish()])
		}
		Command::Detach {
			..
		} => {
			// Local engines don't use remote sessions, so detach is a no-op
			let query_result = QueryResultBuilder::started_now();
			Ok(vec![query_result.finish()])
		}
	}
}

#[cfg(test)]
mod tests {
	#[cfg(feature = "kv-mem")]
	#[cfg(feature = "kv-mem")]
	use surrealdb_core::kvs::Datastore;
	#[cfg(feature = "kv-mem")]
	use surrealdb_engine_api::SessionId;
	#[cfg(feature = "kv-mem")]
	use uuid::Uuid;

	#[cfg(feature = "kv-mem")]
	use super::RouterState;

	#[cfg(feature = "kv-mem")]
	async fn new_state() -> RouterState {
		let kvs = Datastore::new("memory").await.unwrap();
		RouterState::new(kvs)
	}

	/// Focused check that the `handle_session` dispatcher maps a `Clone` event to
	/// a registered session. Does not exercise the router's route arm — see
	/// `resolve_route_session_drains_pending_events` for that.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	async fn handle_session_clone_registers_new_session() {
		let state = new_state().await;
		let old = Uuid::new_v4();
		let new = Uuid::new_v4();

		state.handle_session(SessionId::Initial(old)).await;
		state
			.handle_session(SessionId::Clone {
				old,
				new,
			})
			.await;

		assert!(
			matches!(state.sessions.get(&new), Some(Ok(_))),
			"cloned session should be registered after dispatch"
		);
	}

	/// Regression for the cold-start race, driving the *actual* production path:
	/// `resolve_route_session` is exactly what the router's `route` arm calls.
	/// Session-lifecycle events sit unprocessed in the session channel while a
	/// route for the freshly cloned session arrives — the cross-channel ordering
	/// the real engine hits. This fails (resolves to `NotFound`) if the internal
	/// drain is removed, i.e. if the fix is reverted in `native.rs`/`wasm.rs`.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	async fn resolve_route_session_drains_pending_events() {
		let state = new_state().await;
		let old = Uuid::new_v4();
		let new = Uuid::new_v4();

		// Queue the lifecycle events without applying them, exactly as when a
		// clone's registration races its first query across the two channels.
		let (tx, rx) = async_channel::unbounded::<SessionId>();
		tx.try_send(SessionId::Initial(old)).unwrap();
		tx.try_send(SessionId::Clone {
			old,
			new,
		})
		.unwrap();

		// Sanity: the cloned session is not registered until the events drain.
		assert!(state.sessions.get(&new).is_none());

		// Drive the real route-resolution path the router uses.
		let resolved = state.resolve_route_session(&rx, new).await;

		assert!(
			resolved.is_ok(),
			"route resolution must drain pending session events and register the \
			 cloned session before the lookup"
		);
	}
}
