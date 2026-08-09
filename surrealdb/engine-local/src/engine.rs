//! The engine the SDK drives when it embeds the database.

use std::borrow::Cow;
#[cfg(not(target_family = "wasm"))]
use std::pin::pin;
use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use std::task::{Poll, ready};
#[cfg(not(target_family = "wasm"))]
use std::{future::Future, path::PathBuf};

use async_channel::Sender;
#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use futures::StreamExt;
#[cfg(not(target_family = "wasm"))]
use futures::stream::poll_fn;
use surrealdb_core::dbs::{AuthPrincipalSnapshot, Session};
use surrealdb_core::iam;
#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use surrealdb_core::iam::check::check_ns_db;
use surrealdb_core::kvs::Datastore;
#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use surrealdb_engine_api::MlExportConfig;
use surrealdb_engine_api::{EngineContext, EngineFuture, SurrealEngine};
#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use surrealdb_iam::{Action, ResourceKind};
use surrealdb_kvs::TransactionType;
#[cfg(not(target_family = "wasm"))]
use surrealdb_rpc::export::Config as DbExportConfig;
use surrealdb_rpc::{QueryResult, QueryResultBuilder, QueryStreamItem, Token};
use surrealdb_types::{Array, Error, Notification, Object, ToSql, Value, Variables};
#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use surrealml_core::storage::surml_file::SurMlFile;
#[cfg(not(target_family = "wasm"))]
use tokio::{
	fs::OpenOptions,
	io::{self, AsyncReadExt, AsyncWriteExt},
};
#[cfg(not(target_family = "wasm"))]
use tokio_util::bytes::BytesMut;
use uuid::Uuid;

use crate::session::{SessionRegistry, SessionState};
use crate::std_error_to_types_error;

/// A datastore this process owns, served through the SDK's engine interface.
///
/// Values cross this boundary as themselves: the SDK asked for a query result,
/// and the datastore produced one, so there is no wire format in between and
/// nothing to encode a result into purely for the SDK to take it apart again.
pub struct LocalEngine {
	pub(crate) kvs: Arc<Datastore>,
	pub(crate) sessions: Arc<SessionRegistry>,
}

impl std::fmt::Debug for LocalEngine {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("LocalEngine").finish_non_exhaustive()
	}
}

impl LocalEngine {
	/// The state the request's session owns, once that session is registered.
	async fn state(&self, ctx: EngineContext) -> Result<Arc<SessionState>, Error> {
		self.sessions.resolve(ctx.session).await
	}
}

/// Flattens the results of a statement run for its single value.
///
/// An empty reply reads as [`Value::None`]: an operation with no result may
/// answer with either, and both mean the same thing.
fn single_value(mut results: Vec<QueryResult>) -> Result<Value, Error> {
	match results.len() {
		0 => Ok(Value::None),
		1 => results.remove(0).result,
		_ => Err(Error::internal("expected the database to return one or no results".to_string())),
	}
}

/// The failure of naming a transaction the session does not hold.
fn transaction_not_found() -> Error {
	Error::not_found(
		"Transaction not found".to_string(),
		Some(surrealdb_types::NotFoundError::Transaction),
	)
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

/// Ends this session's live queries when an auth operation changed the
/// principal.
///
/// SECURITY: a subscription captures the principal that registered it, so one
/// left running across a principal change keeps dispatching under the previous
/// access controls (GHSA-2xrp-m9c6-75rj). See [`AuthPrincipalSnapshot`].
///
/// Deletion goes straight to the datastore rather than through a `KILL`
/// statement, as the WebSocket transport does it: the session now belongs to
/// the *new* principal, which may have no permission to kill what the previous
/// one registered -- and a teardown that can be refused is not a teardown.
async fn cleanup_lqs_on_principal_change(
	kvs: &Datastore,
	state: &SessionState,
	before: &AuthPrincipalSnapshot,
) {
	if !before.differs_from(&*state.session.read().await) {
		return;
	}
	let mut gc = Vec::new();
	state.live_queries.retain(|id, _| {
		gc.push(*id);
		false
	});
	if gc.is_empty() {
		return;
	}
	if let Err(error) = kvs.delete_queries(gc).await {
		warn!("Failed to end live queries after the session's principal changed; {error}");
	}
}

/// Rejects an operation that arrives on a session whose authentication has
/// expired.
///
/// `Datastore::execute` refuses an expired session itself, so the query path is
/// covered wherever it is reached from. This is for the operations that change
/// session state without going through it.
async fn ensure_session_active(state: &SessionState) -> Result<(), Error> {
	if state.session.read().await.expired() {
		return Err(surrealdb_rpc::error::session_expired());
	}
	Ok(())
}

#[cfg(not(target_family = "wasm"))]
async fn export_file(
	kvs: &Datastore,
	sess: &Session,
	chn: Sender<Vec<u8>>,
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
	chn: Sender<Vec<u8>>,
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

/// Forwards to `bytes` what `export` sends on the channel it is given.
///
/// Started on a task of its own and returns immediately: the caller reads
/// `bytes`, and the export fills a bounded channel, so the two have to make
/// progress against each other.
#[cfg(not(target_family = "wasm"))]
fn export_to_channel<F, Fut>(bytes: Sender<Result<Vec<u8>, Error>>, export: F)
where
	F: FnOnce(Sender<Vec<u8>>) -> Fut + Send + 'static,
	Fut: Future<Output = Result<(), Error>> + Send,
{
	crate::spawn(async move {
		let (tx, rx) = async_channel::bounded(1);
		let produce = async {
			if let Err(error) = export(tx).await {
				bytes.send(Err(error)).await.ok();
			}
		};

		let bridge = async {
			while let Ok(chunk) = rx.recv().await {
				if bytes.send(Ok(chunk)).await.is_err() {
					// The caller stopped reading. Closing the export's channel
					// is what ends it: the channel is bounded and this loop was
					// the only thing draining it, so the next send would never
					// complete and this task would never finish.
					rx.close();
					return;
				}
			}
		};

		tokio::join!(produce, bridge);
	});
}

/// Writes to `path` what `export` sends on the channel it is given.
///
/// The export owns the sender, so the bridge ends when the export does.
#[cfg(not(target_family = "wasm"))]
async fn export_to_file<F, Fut>(path: PathBuf, export: F) -> Result<(), Error>
where
	F: FnOnce(Sender<Vec<u8>>) -> Fut,
	Fut: Future<Output = Result<(), Error>>,
{
	let (tx, rx) = async_channel::bounded(1);
	let (mut writer, mut reader) = io::duplex(10_240);

	// Write to channel.
	let export = export(tx);

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
	let mut output =
		match OpenOptions::new().write(true).create(true).truncate(true).open(&path).await {
			Ok(file) => file,
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
	Ok(())
}

impl SurrealEngine for LocalEngine {
	fn query(
		&self,
		ctx: EngineContext,
		query: Cow<'static, str>,
		variables: Variables,
	) -> EngineFuture<'_, Vec<QueryResult>> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			// Merge session vars with query vars
			let mut vars = state.vars.read().await.clone();
			vars.extend(variables);

			let session = state.session.read().await;
			match ctx.transaction {
				Some(txn) => match state.transactions.get(&txn) {
					Some(tx) => {
						self.kvs
							.execute_with_transaction(query.as_ref(), &session, Some(vars), tx)
							.await
					}
					// Reported as the statement's own failure rather than the
					// call's, so a caller reading a multi-statement response
					// sees it where the statement was.
					None => Ok(vec![
						QueryResultBuilder::started_now()
							.finish_with_result(Err(transaction_not_found())),
					]),
				},
				None => self.kvs.execute(query.as_ref(), &session, Some(vars)).await,
			}
		})
	}

	fn query_stream(
		&self,
		ctx: EngineContext,
		query: Cow<'static, str>,
		variables: Variables,
		items: Sender<QueryStreamItem>,
	) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			// Merge session vars with query vars
			let mut vars = state.vars.read().await.clone();
			vars.extend(variables);

			// Preparing the job is what can fail synchronously -- a parse error,
			// an expired session -- so it happens while the session guard is
			// held. The job itself owns everything it needs from here.
			let job = {
				let session = state.session.read().await;
				match ctx.transaction {
					Some(txn) => {
						// The statement never runs, so there is no statement
						// result to report this on; it belongs to the call.
						let Some(tx) = state.transactions.get(&txn) else {
							return Err(transaction_not_found());
						};
						self.kvs.execute_stream_with_transaction(
							query.as_ref(),
							&session,
							Some(vars),
							tx,
							None,
							items,
						)?
					}
					None => self.kvs.execute_stream(
						query.as_ref(),
						&session,
						Some(vars),
						None,
						items,
					)?,
				}
			};

			// The caller drains the item channel, so driving the execution here
			// is what lets the two make progress against each other. The results
			// come back only so the execution's own failures do; the rows have
			// already gone to the caller.
			job.run.await?;
			Ok(())
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
			let state = self.state(ctx).await?;
			// Format arguments as comma-separated SQL values
			let formatted_args = args.iter().map(|v| v.to_sql()).collect::<Vec<_>>().join(", ");

			// Build SQL query: name<version>(args) or name(args)
			let sql = match version {
				Some(v) => format!("{name}<{v}>({formatted_args})"),
				None => format!("{name}({formatted_args})"),
			};

			let results = self
				.kvs
				.execute(&sql, &*state.session.read().await, Some(state.vars.read().await.clone()))
				.await?;
			single_value(results)
		})
	}

	fn use_ns_db(
		&self,
		ctx: EngineContext,
		namespace: Option<String>,
		database: Option<String>,
	) -> EngineFuture<'_, (Option<String>, Option<String>)> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			let mut session = state.session.write().await;
			self.kvs.process_use(None, &mut session, namespace, database).await?;
			Ok((session.ns.clone(), session.db.clone()))
		})
	}

	fn set(&self, ctx: EngineContext, key: String, value: Value) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			// SECURITY: an expired session must not be able to change session
			// state, the same as over the RPC transports. `SECURITY_GUIDE.md`
			// section 3 requires expiry to be checked before any statement or
			// method is processed; `Datastore::execute` covers the query path,
			// so without this the session methods are the way past it.
			ensure_session_active(&state).await?;
			surrealdb_rpc::check_protected_param(&key)
				.map_err(|e| Error::internal(e.to_string()))?;
			// Need to compute because certain keys might not be allowed to be set and those
			// should be rejected by an error.
			match value {
				Value::None => state.vars.write().await.remove(&key),
				v => state.vars.write().await.insert(key, v),
			};
			Ok(())
		})
	}

	fn unset(&self, ctx: EngineContext, key: String) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			ensure_session_active(&state).await?;
			state.vars.write().await.remove(&key);
			Ok(())
		})
	}

	fn signup(&self, ctx: EngineContext, credentials: Object) -> EngineFuture<'_, Token> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			let before = AuthPrincipalSnapshot::capture(&*state.session.read().await);
			let token = iam::signup::signup(
				&self.kvs,
				&mut *state.session.write().await,
				credentials.into(),
			)
			.await
			.map_err(surrealdb_core::err::anyhow_to_types_error);
			cleanup_lqs_on_principal_change(&self.kvs, &state, &before).await;
			token
		})
	}

	fn signin(&self, ctx: EngineContext, credentials: Object) -> EngineFuture<'_, Token> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			let before = AuthPrincipalSnapshot::capture(&*state.session.read().await);
			let token = iam::signin::signin(
				&self.kvs,
				&mut *state.session.write().await,
				credentials.into(),
			)
			.await
			.map_err(surrealdb_core::err::anyhow_to_types_error);
			cleanup_lqs_on_principal_change(&self.kvs, &state, &before).await;
			token
		})
	}

	fn authenticate(&self, ctx: EngineContext, token: Token) -> EngineFuture<'_, Token> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			let before = AuthPrincipalSnapshot::capture(&*state.session.read().await);
			// Extract the access token and check if this token supports refresh
			let (access, with_refresh) = match &token {
				Token::Access(access) => (access, false),
				Token::WithRefresh {
					access,
					..
				} => (access, true),
			};
			// Bound the guard to this block rather than letting it live as a
			// temporary in the `match` scrutinee below. A scrutinee's temporaries
			// are dropped only at the end of the whole `match`, and the refresh
			// arm takes the same lock -- which `RwLock` does not grant twice to
			// one task.
			let verified = {
				let mut session = state.session.write().await;
				iam::verify::token(&self.kvs, &mut session, access).await
			};
			let result = match verified {
				// Authentication successful - return the original token
				Ok(_) => Ok(token),
				Err(error) => {
					// Automatic refresh token handling:
					// If the access token is expired and we have a refresh token,
					// automatically attempt to refresh and return new tokens.
					if with_refresh && iam::is_expired_token_error(&error) {
						iam::token::refresh(token, &self.kvs, &mut *state.session.write().await)
							.await
							.map_err(|error| Error::internal(error.to_string()))
					} else {
						// If authentication failed and automatic refresh isn't applicable,
						// return the authentication error
						Err(Error::internal(error.to_string()))
					}
				}
			};
			cleanup_lqs_on_principal_change(&self.kvs, &state, &before).await;
			result
		})
	}

	fn refresh(&self, ctx: EngineContext, token: Token) -> EngineFuture<'_, Token> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			let before = AuthPrincipalSnapshot::capture(&*state.session.read().await);
			let result = iam::token::refresh(token, &self.kvs, &mut *state.session.write().await)
				.await
				.map_err(|error| Error::internal(error.to_string()));
			cleanup_lqs_on_principal_change(&self.kvs, &state, &before).await;
			result
		})
	}

	fn revoke(&self, ctx: EngineContext, token: Token) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			// Revoking is about the token, not the session, but a request still
			// belongs to one -- and resolving it is what reports a session the
			// engine does not know.
			self.state(ctx).await?;
			iam::token::revoke_refresh_token(token, &self.kvs)
				.await
				.map_err(|error| Error::internal(error.to_string()))
		})
	}

	fn invalidate(&self, ctx: EngineContext) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			let before = AuthPrincipalSnapshot::capture(&*state.session.read().await);
			let result = iam::clear::clear(&mut *state.session.write().await)
				.map_err(|error| Error::internal(error.to_string()));
			cleanup_lqs_on_principal_change(&self.kvs, &state, &before).await;
			result
		})
	}

	fn begin(&self, ctx: EngineContext) -> EngineFuture<'_, Uuid> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			let txn = self
				.kvs
				.transaction(TransactionType::Write)
				.await
				.map_err(|error| Error::internal(error.to_string()))?;
			let id = Uuid::now_v7();
			state.transactions.insert(id, Arc::new(txn));
			Ok(id)
		})
	}

	fn commit(&self, ctx: EngineContext, txn: Uuid) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			// Removing is what claims the transaction, so two concurrent calls
			// naming the same one cannot both go on to finalise it.
			if let Some(tx) = state.transactions.take(&txn) {
				tx.commit().await.map_err(std_error_to_types_error)?;
			}
			Ok(())
		})
	}

	fn rollback(&self, ctx: EngineContext, txn: Uuid) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			if let Some(tx) = state.transactions.take(&txn) {
				tx.cancel().await.map_err(std_error_to_types_error)?;
			}
			Ok(())
		})
	}

	fn health(&self, ctx: EngineContext) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			// The datastore is in this process, so there is no round trip to
			// report on -- but a session the engine cannot resolve is exactly
			// what a caller checking the connection needs to hear about.
			self.state(ctx).await?;
			Ok(())
		})
	}

	fn version(&self, ctx: EngineContext) -> EngineFuture<'_, String> {
		Box::pin(async move {
			self.state(ctx).await?;
			Ok(surrealdb_core::env::VERSION.to_string())
		})
	}

	fn subscribe_live(
		&self,
		ctx: EngineContext,
		uuid: Uuid,
		notifications: Sender<Result<Notification, Error>>,
	) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			state.live_queries.insert(uuid, notifications);
			Ok(())
		})
	}

	fn kill(&self, ctx: EngineContext, uuid: Uuid) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			state.live_queries.remove(&uuid);
			let results = kill_live_query(
				&self.kvs,
				uuid,
				&*state.session.read().await,
				state.vars.read().await.clone(),
			)
			.await?;
			single_value(results)?;
			Ok(())
		})
	}

	#[cfg(not(target_family = "wasm"))]
	fn export_file(
		&self,
		ctx: EngineContext,
		path: PathBuf,
		config: Option<DbExportConfig>,
	) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			let session = state.session.read().await.clone();
			export_to_file(path, |chn| export_file(&self.kvs, &session, chn, config)).await
		})
	}

	#[cfg(not(target_family = "wasm"))]
	fn export_bytes(
		&self,
		ctx: EngineContext,
		bytes: Sender<Result<Vec<u8>, Error>>,
		config: Option<DbExportConfig>,
	) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			let kvs = Arc::clone(&self.kvs);
			let session = state.session.read().await.clone();
			export_to_channel(bytes, |chn| async move {
				export_file(&kvs, &session, chn, config).await
			});
			Ok(())
		})
	}

	#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
	fn export_ml_file(
		&self,
		ctx: EngineContext,
		path: PathBuf,
		config: MlExportConfig,
	) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			let session = state.session.read().await.clone();
			export_to_file(path, |chn| export_ml(&self.kvs, &session, chn, config)).await
		})
	}

	#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
	fn export_ml_bytes(
		&self,
		ctx: EngineContext,
		bytes: Sender<Result<Vec<u8>, Error>>,
		config: MlExportConfig,
	) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			let kvs = Arc::clone(&self.kvs);
			let session = state.session.read().await.clone();
			export_to_channel(
				bytes,
				|chn| async move { export_ml(&kvs, &session, chn, config).await },
			);
			Ok(())
		})
	}

	#[cfg(not(target_family = "wasm"))]
	fn import_file(&self, ctx: EngineContext, path: PathBuf) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			let file = match OpenOptions::new().read(true).open(&path).await {
				Ok(file) => file,
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

			let responses = self
				.kvs
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

			Ok(())
		})
	}

	#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
	fn import_ml_file(&self, ctx: EngineContext, path: PathBuf) -> EngineFuture<'_, ()> {
		Box::pin(async move {
			let state = self.state(ctx).await?;
			let mut file = match OpenOptions::new().read(true).open(&path).await {
				Ok(file) => file,
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
			self.kvs
				.check(
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

			self.kvs
				.put_ml_model(
					&*state.session.read().await,
					&file.header.name.to_string(),
					&file.header.version.to_string(),
					&file.header.description.to_string(),
					data,
				)
				.await
				.map_err(std_error_to_types_error)?;

			Ok(())
		})
	}
}
