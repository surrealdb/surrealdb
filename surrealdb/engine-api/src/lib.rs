//! The service-provider interface between the SurrealDB Rust SDK and the
//! engines it drives.
//!
//! An engine is a task that owns a datastore or a connection to one. The SDK
//! hands it a [`Route`] per request and the engine answers on the route's
//! response channel; session lifetime travels alongside on a separate
//! [`SessionId`] channel. This crate holds exactly the types that cross that
//! boundary, so an engine can live in its own crate without the SDK depending
//! on it, or on anything it in turn depends on.
//!
//! # Stability
//!
//! This is an internal interface between crates released together. It carries
//! no stability guarantee and may change in any release, including a patch
//! release. Depend on it only if you implement an engine; application code
//! should use the [`surrealdb`](https://docs.rs/surrealdb) crate.

use std::borrow::Cow;
use std::path::PathBuf;

use async_channel::Sender;
use surrealdb_rpc::export::Config as DbExportConfig;
use surrealdb_rpc::{QueryResult, Token};
use surrealdb_types::{Array, Error, NotFoundError, Notification, Object, Value, Variables};
use uuid::Uuid;

/// A request travelling from the SDK to an engine, tagged with the session it
/// belongs to.
#[derive(Debug)]
pub struct RequestData {
	/// The operation the engine should perform.
	pub command: Command,
	/// The session the command runs under.
	pub session_id: Uuid,
}

/// A request paired with the channel its response must be sent on.
#[derive(Debug)]
pub struct Route {
	/// The request to execute.
	pub request: RequestData,
	/// Where the engine sends the outcome. Exactly one message is expected.
	pub response: Sender<Result<Vec<QueryResult>, Error>>,
}

/// A session-lifetime event. Engines keep per-session state (the authenticated
/// session, its variables, its live queries), and these events tell them when
/// to create, copy and discard it.
#[derive(Debug, Clone, Copy)]
pub enum SessionId {
	/// A new session was opened.
	Initial(Uuid),
	/// A session was cloned; `new` starts as a copy of `old`.
	Clone {
		/// The session being copied.
		old: Uuid,
		/// The session receiving the copy.
		new: Uuid,
	},
	/// A session was dropped and its state can be released.
	Drop(Uuid),
}

/// Why a route could not be matched to a session.
#[derive(Debug, Clone)]
pub enum SessionError {
	/// No state is registered for the session the route targets.
	NotFound(Uuid),
	/// The remote end reported a session failure.
	Remote(String),
}

/// Convert a session error into the error type the SDK surfaces to callers.
pub fn session_error_to_error(e: SessionError) -> Error {
	match e {
		SessionError::NotFound(id) => Error::not_found(
			format!("Session not found: {id}"),
			NotFoundError::Session {
				id: Some(id.to_string()),
			},
		),
		SessionError::Remote(msg) => Error::internal(msg),
	}
}

/// Which machine learning model to export, by name and version.
#[derive(Debug, Clone)]
pub struct MlExportConfig {
	/// The model name.
	pub name: String,
	/// The model version.
	pub version: String,
}

/// The operations an engine can be asked to perform.
#[derive(Debug, Clone)]
pub enum Command {
	/// Select the namespace and/or database for the session.
	Use {
		/// The namespace to use, or `None` to leave it unchanged.
		namespace: Option<String>,
		/// The database to use, or `None` to leave it unchanged.
		database: Option<String>,
	},
	/// Sign up as a record user and authenticate the session.
	Signup {
		/// The signup credentials.
		credentials: Object,
	},
	/// Sign in and authenticate the session.
	Signin {
		/// The signin credentials.
		credentials: Object,
	},
	/// Authenticate the session with an existing token.
	Authenticate {
		/// The token to authenticate with.
		token: Token,
	},
	/// Exchange a refresh token for a fresh token pair.
	Refresh {
		/// The token carrying the refresh token.
		token: Token,
	},
	/// Drop the session's authentication.
	Invalidate,
	/// Begin a manual transaction.
	Begin,
	/// Cancel a manual transaction.
	Rollback {
		/// The transaction to cancel.
		txn: Uuid,
	},
	/// Commit a manual transaction.
	Commit {
		/// The transaction to commit.
		txn: Uuid,
	},
	/// Invalidate a refresh token so it can no longer be redeemed.
	Revoke {
		/// The token carrying the refresh token.
		token: Token,
	},
	/// Execute a query, optionally inside a manual transaction.
	Query {
		/// The transaction to run in, or `None` for an implicit one.
		txn: Option<Uuid>,
		/// The query text.
		query: Cow<'static, str>,
		/// The variables bound for this query only.
		variables: Variables,
	},
	/// Export the database to a file.
	ExportFile {
		/// The file to write.
		path: PathBuf,
		/// What to include in the export.
		config: Option<DbExportConfig>,
	},
	/// Export a machine learning model to a file.
	ExportMl {
		/// The file to write.
		path: PathBuf,
		/// The model to export.
		config: MlExportConfig,
	},
	/// Export the database to a channel.
	ExportBytes {
		/// Where the export is streamed.
		bytes: Sender<Result<Vec<u8>, Error>>,
		/// What to include in the export.
		config: Option<DbExportConfig>,
	},
	/// Export a machine learning model to a channel.
	ExportBytesMl {
		/// Where the export is streamed.
		bytes: Sender<Result<Vec<u8>, Error>>,
		/// The model to export.
		config: MlExportConfig,
	},
	/// Import a database export from a file.
	ImportFile {
		/// The file to read.
		path: PathBuf,
	},
	/// Import a machine learning model from a file.
	ImportMl {
		/// The file to read.
		path: PathBuf,
	},
	/// Check that the engine is reachable.
	Health,
	/// Report the database version.
	Version,
	/// Set a session variable.
	Set {
		/// The variable name.
		key: String,
		/// The value to bind, or `Value::None` to remove it.
		value: Value,
	},
	/// Remove a session variable.
	Unset {
		/// The variable name.
		key: String,
	},
	/// Register the channel a live query's notifications are delivered on.
	SubscribeLive {
		/// The live query.
		uuid: Uuid,
		/// Where notifications are delivered.
		notification_sender: Sender<Result<Notification, Error>>,
	},
	/// Kill a live query.
	Kill {
		/// The live query.
		uuid: Uuid,
	},
	/// Adopt an existing remote session.
	Attach {
		/// The session to adopt.
		session_id: Uuid,
	},
	/// Release a remote session without ending it.
	Detach {
		/// The session to release.
		session_id: Uuid,
	},
	/// Call a function or a machine learning model.
	Run {
		/// The function or model name.
		name: String,
		/// The model version, if any.
		version: Option<String>,
		/// The arguments to pass.
		args: Array,
	},
}
