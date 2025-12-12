//! Bridge trait and types for interacting with SurrealDB.
//!
//! This module defines the core [`SurrealBridge`] trait, which provides a unified interface
//! for interacting with a SurrealDB instance. It abstracts over different connection types
//! and provides methods for managing sessions, transactions, authentication, queries, and more.
//!
//! # Architecture
//!
//! The bridge interface uses session-based operations, where each client connection maintains
//! a session identified by a [`Uuid`]. Sessions can have associated namespace/database contexts,
//! variables, and transactions.
//!
//! # Example Usage
//!
//! ```ignore
//! // Create a new session
//! let session_id = bridge.new_session().await?;
//!
//! // Set the namespace and database
//! bridge.use(session_id, Some("my_ns".into()), Some("my_db".into())).await?;
//!
//! // Execute a query
//! let results = bridge.query(session_id, None, "SELECT * FROM users".into(), Variables::default()).await?;
//! ```

use anyhow::Result;
use bytes::Bytes;
use futures::Stream;
use uuid::Uuid;

// Needed because we use the SurrealValue derive macro inside the crate which exports it :)
use crate as surrealdb_types;
use crate::{Duration, Notification, SurrealValue, Value, Variables};

pub mod requirements {
	//! Trait requirements for the bridge trait.
	//!
	//! This module exists to conditionally require `Send` for the bridge trait based on
	//! the target platform. WASM targets don't support `Send` (single-threaded), while
	//! non-WASM targets require it for safe concurrent access.

	/// WASM bridge requirements (no Send/Sync needed in single-threaded environment).
	#[cfg(target_family = "wasm")]
	pub trait BridgeRequirements {}

	/// Implements bridge requirements for all types on WASM.
	#[cfg(target_family = "wasm")]
	impl<T> BridgeRequirements for T {}

	/// Non-WASM bridge requirements (requires Send for multi-threaded safety).
	#[cfg(not(target_family = "wasm"))]
	pub trait BridgeRequirements: Send + Sync {}

	/// Implements bridge requirements for Send + Sync types on non-WASM platforms.
	#[cfg(not(target_family = "wasm"))]
	impl<T: Send + Sync> BridgeRequirements for T {}
}

/// Core trait for bridging to a SurrealDB instance.
///
/// This trait defines the complete interface for interacting with SurrealDB, including
/// connection management, session handling, authentication, queries, and live notifications.
/// Implementors of this trait can provide different backend strategies (e.g., in-process,
/// networked, embedded) while maintaining a consistent API.
///
/// # Session Management
///
/// Most operations require a session ID, which represents a client connection with its own
/// namespace/database context, variables, and authentication state. Sessions persist across
/// multiple operations and must be explicitly created and dropped.
///
/// # Error Handling
///
/// All async methods return `anyhow::Result`, allowing implementors to provide detailed
/// error context while maintaining flexibility in error propagation.
///
/// # Thread Safety
///
/// On non-WASM platforms, implementors must be `Send + Sync` to allow safe concurrent access.
/// On WASM platforms (single-threaded), these requirements are relaxed.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub trait SurrealBridge: requirements::BridgeRequirements {
    // Connection
    
    /// Checks the health of the SurrealDB connection.
    ///
    /// Returns `Ok(())` if the connection is healthy and ready to accept requests.
    async fn health(&self) -> Result<()>;
    
    /// Returns the version string of the SurrealDB instance.
    ///
    /// This typically includes the SurrealDB version number and build information.
    async fn version(&self) -> Result<String>;

    // Sessions
    
    /// Drops a session and releases all associated resources.
    ///
    /// This will cancel any active transactions and clear session state.
    async fn drop_session(&self, session_id: Uuid) -> Result<()>;
    
    /// Resets a session to its initial state.
    ///
    /// This clears namespace/database context, variables, and authentication,
    /// but keeps the session ID active.
    async fn reset_session(&self, session_id: Option<Uuid>) -> Result<()>;
    
    /// Lists all active session IDs.
    async fn list_sessions(&self) -> Result<Vec<Uuid>>;

    // Session modifiers
    
    /// Sets the namespace and/or database context for a session.
    ///
    /// Subsequent queries in this session will execute within the specified context.
    /// Pass `None` to clear the namespace or database.
    async fn r#use(&self, session_id: Option<Uuid>, ns: Nullable<String>, db: Nullable<String>) -> Result<(Option<String>, Option<String>)>;
    
    /// Sets a variable in the session scope.
    ///
    /// Variables can be referenced in queries using `$variable_name` syntax.
    async fn set_variable(&self, session_id: Option<Uuid>, name: String, value: Value) -> Result<()>;
    
    /// Removes a variable from the session scope.
    async fn drop_variable(&self, session_id: Option<Uuid>, name: String) -> Result<()>;

    // Transactions
    
    /// Begins a new transaction within a session.
    ///
    /// Returns a transaction ID that can be used to execute queries within the
    /// transaction context. Transactions must be explicitly committed or cancelled.
    async fn begin_transaction(&self, session_id: Option<Uuid>) -> Result<Uuid>;
    
    /// Commits a transaction, persisting all changes made within it.
    async fn commit_transaction(&self, session_id: Option<Uuid>, transaction_id: Uuid) -> Result<()>;
    
    /// Cancels a transaction, discarding all changes made within it.
    async fn cancel_transaction(&self, session_id: Option<Uuid>, transaction_id: Uuid) -> Result<()>;
    
    /// Lists all active transaction IDs for a session.
    async fn list_transactions(&self, session_id: Option<Uuid>) -> Result<Vec<Uuid>>;

    // Authentication
    
    /// Signs up a new user with the provided parameters.
    ///
    /// Returns access and refresh tokens upon successful registration.
    /// The `params` should include credentials and any required user information.
    async fn signup(&self, session_id: Option<Uuid>, params: Variables) -> Result<Tokens>;
    
    /// Signs in a user with the provided credentials.
    ///
    /// Returns access and refresh tokens upon successful authentication.
    /// The `params` typically include username/email and password.
    async fn signin(&self, session_id: Option<Uuid>, params: Variables) -> Result<Tokens>;
    
    /// Authenticates a session using a JWT token.
    ///
    /// This sets the authentication context for subsequent operations in the session.
    async fn authenticate(&self, session_id: Option<Uuid>, token: String) -> Result<()>;
    
    /// Refreshes authentication tokens using a refresh token.
    ///
    /// Returns a new set of access and refresh tokens.
    async fn refresh(&self, session_id: Option<Uuid>, tokens: Tokens) -> Result<Tokens>;
    
    /// Revokes the provided tokens, invalidating them for future use.
    async fn revoke(&self, tokens: Tokens) -> Result<()>;
    
    /// Clears authentication from a session.
    ///
    /// This removes any authenticated user context but keeps the session active.
    async fn invalidate(&self, session_id: Option<Uuid>) -> Result<()>;

    // Export & Import
    
    /// Exports database contents as a stream of SQL bytes.
    ///
    /// The `config` parameter controls what database elements are included in the export.
    /// The returned stream produces chunks of SQL statements that can reconstruct the
    /// database state.
    async fn export(&self, session_id: Option<Uuid>, config: ExportConfig) -> Result<std::pin::Pin<Box<dyn Stream<Item = Bytes> + Send>>>;
    
    /// Imports SQL statements from a stream of bytes.
    ///
    /// The SQL stream should contain valid SurrealQL statements. This operation
    /// executes the statements within the session's current namespace/database context.
    async fn import(&self, session_id: Option<Uuid>, sql: std::pin::Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>) -> Result<()>;

    // Query
    
    /// Executes a SurrealQL query and returns results as a stream of chunks.
    ///
    /// # Parameters
    /// - `session_id`: The session context for the query
    /// - `txn`: Optional transaction ID to execute the query within
    /// - `query`: The SurrealQL query string
    /// - `vars`: Variables to bind into the query
    ///
    /// The query result is streamed in chunks, allowing for efficient handling of
    /// large result sets and batched queries.
    async fn query(&self, session_id: Option<Uuid>, txn: Option<Uuid>, query: String, vars: Variables) -> Result<std::pin::Pin<Box<dyn Stream<Item = QueryChunk> + Send>>>;

    // Live notifications
    
    /// Returns a stream of live query notifications.
    ///
    /// This stream is unfiltered and includes notifications for all active live queries
    /// across all sessions. Implementors can filter and route these notifications to
    /// the appropriate recipients based on the notification metadata.
    async fn notifications(&self) -> Result<std::pin::Pin<Box<dyn Stream<Item = Notification> + Send>>>;
}

/// Authentication tokens returned from signup, signin, and refresh operations.
///
/// These tokens are used to authenticate sessions and maintain user identity.
/// The access token is used for standard operations, while the refresh token
/// is used to obtain new access tokens when they expire.
#[derive(Clone, Debug, SurrealValue)]
pub struct Tokens {
    /// JWT access token for authenticating requests
    pub access: Option<String>,
    /// Refresh token for obtaining new access tokens
    pub refresh: Option<String>,
}

/// Represents the state of a SurrealDB session.
///
/// A session maintains context for a client connection, including the active
/// namespace and database, along with any session-scoped variables.
#[derive(Clone, Debug, SurrealValue)]
pub struct Session {
    /// Unique identifier for this session
    pub id: Uuid,
    /// Active namespace context, if set
    pub ns: Option<String>,
    /// Active database context, if set
    pub db: Option<String>,
    /// Session-scoped variables that can be referenced in queries
    pub variables: Variables,
}

/// Statistics about query execution.
///
/// Provides metrics about data volume and scanning for performance analysis.
#[derive(Clone, Debug, SurrealValue)]
pub struct QueryStats {
    /// Number of records returned in the result
    pub records_received: u64,
    /// Total bytes of data in the result
    pub bytes_received: u64,
    /// Number of records scanned during query execution
    pub records_scanned: u64,
    /// Total bytes scanned during query execution
    pub bytes_scanned: u64,
    /// Time taken to execute the query
    pub duration: Duration,
}

/// Indicates the type of query response chunk.
///
/// Query results can be delivered in multiple chunks, especially for large result sets
/// or when executing multiple statements in a batch.
#[derive(Clone, Debug, SurrealValue)]
#[surreal(untagged)]
pub enum QueryResponseKind {
    /// A single standalone query result
    Single,
    /// Part of a batched result set (more chunks will follow)
    Batched,
    /// The final chunk in a batched result set
    BatchedFinal,
}

/// Classifies the type of query being executed.
///
/// This allows special handling for queries that have side effects or require
/// ongoing resource management.
#[derive(Clone, Debug, SurrealValue)]
#[surreal(untagged, lowercase)]
pub enum QueryType {
    /// Standard query (SELECT, INSERT, UPDATE, etc.)
    Other,
    /// Live query that will send ongoing notifications
    Live,
    /// Kill query that terminates a live query
    Kill,
}

/// A chunk of query results streamed from the bridge.
///
/// Query results are delivered as a stream of chunks to support large result sets
/// and batched queries efficiently. Each chunk contains metadata about its position
/// in the overall result stream and may contain data, statistics, or errors.
#[derive(Clone, Debug, SurrealValue)]
pub struct QueryChunk {
    /// Index of the query within a batch (0-based)
    pub query: u64,
    /// Index of this chunk within the query results (0-based)
    pub batch: u64,
    /// Type of chunk (single, batched, or final)
    pub kind: QueryResponseKind,
    /// Execution statistics, if available
    pub stats: Option<QueryStats>,
    /// Result data, if any
    pub result: Option<Vec<Value>>,
    /// Type of query that produced this chunk
    pub r#type: Option<QueryType>,
    /// Error message if the query failed
    pub error: Option<String>,
}

/// Configuration for database export operations.
///
/// Controls which database elements are included in the export. By default,
/// most elements are exported except for versions.
///
/// # Example
///
/// ```ignore
/// let config = ExportConfig {
///     users: true,
///     tables: ExportTableConfig::Some(vec!["users".into(), "posts".into()]),
///     records: true,
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Debug, SurrealValue)]
#[surreal(default)]
pub struct ExportConfig {
	/// Include user definitions
	pub users: bool,
	/// Include access (permissions) definitions
	pub accesses: bool,
	/// Include parameter definitions
	pub params: bool,
	/// Include function definitions
	pub functions: bool,
	/// Include analyzer definitions
	pub analyzers: bool,
	/// Which tables to include (all, none, or specific tables)
	pub tables: ExportTableConfig,
	/// Include version history (disabled by default)
	pub versions: bool,
	/// Include record data
	pub records: bool,
	/// Include sequence definitions
	pub sequences: bool,
}

impl Default for ExportConfig {
	fn default() -> ExportConfig {
		ExportConfig {
			users: true,
			accesses: true,
			params: true,
			functions: true,
			analyzers: true,
			tables: ExportTableConfig::default(),
			versions: false,
			records: true,
			sequences: true,
		}
	}
}

/// Configuration for which tables to include in an export.
///
/// Provides fine-grained control over table inclusion: export all tables,
/// no tables, or a specific list of tables.
///
/// # Examples
///
/// ```ignore
/// // Export all tables
/// let config = ExportTableConfig::All;
///
/// // Export no tables
/// let config = ExportTableConfig::None;
///
/// // Export specific tables
/// let config = ExportTableConfig::Some(vec!["users".into(), "posts".into()]);
///
/// // Convert from bool
/// let config: ExportTableConfig = true.into(); // All tables
/// ```
#[derive(Clone, Debug, Default, SurrealValue)]
#[surreal(untagged)]
pub enum ExportTableConfig {
	/// Export all tables (default)
	#[default]
	#[surreal(value = true)]
	All,
	/// Export no tables
	#[surreal(value = false)]
	None,
	/// Export only the specified tables
	Some(Vec<String>),
}

impl From<bool> for ExportTableConfig {
	fn from(value: bool) -> Self {
		match value {
			true => ExportTableConfig::All,
			false => ExportTableConfig::None,
		}
	}
}

impl From<Vec<String>> for ExportTableConfig {
	fn from(value: Vec<String>) -> Self {
		ExportTableConfig::Some(value)
	}
}

impl From<Vec<&str>> for ExportTableConfig {
	fn from(value: Vec<&str>) -> Self {
		ExportTableConfig::Some(value.into_iter().map(ToOwned::to_owned).collect())
	}
}

impl ExportTableConfig {
	/// Checks if any tables should be exported.
	///
	/// Returns `true` for `All` and `Some(_)`, `false` for `None`.
	pub fn is_any(&self) -> bool {
		matches!(self, Self::All | Self::Some(_))
	}
	
	/// Checks if a specific table should be included in the export.
	///
	/// # Parameters
	/// - `table`: The name of the table to check
	///
	/// # Returns
	/// - `true` if the table should be exported
	/// - `false` if the table should be excluded
	pub fn includes(&self, table: &str) -> bool {
		match self {
			Self::All => true,
			Self::None => false,
			Self::Some(v) => v.iter().any(|v| v.eq(table)),
		}
	}
}

/// A three-state value that distinguishes between absent, explicitly null, and present values.
///
/// This is useful for update operations where:
/// - `None` means "don't change this field" (absent from the request)
/// - `Null` means "set this field to null" (explicitly nullify)
/// - `Some(T)` means "set this field to this value"
#[derive(Clone, Copy, SurrealValue)]
#[surreal(untagged)]
pub enum Nullable<T: Clone + SurrealValue> {
    /// The value is absent (not provided). Typically means "leave unchanged".
    #[surreal(value = none)]
    None,
    /// The value is explicitly null. Typically means "clear/unset this field".
    #[surreal(value = null)]
    Null,
    /// The value is present.
    Some(T),
}

impl<T: Clone + SurrealValue> Nullable<T> {
    /// Maps a `Nullable<T>` to `Nullable<U>` by applying a function to the contained value.
    ///
    /// `None` and `Null` variants are preserved unchanged.
    pub fn map<U, F>(self, f: F) -> Nullable<U>
    where
        F: FnOnce(T) -> U,
        U: Clone + SurrealValue,
    {
        match self {
            Nullable::None => Nullable::None,
            Nullable::Null => Nullable::Null,
            Nullable::Some(x) => Nullable::Some(f(x)),
        }
    }
}

impl<T: Clone + SurrealValue> From<Option<T>> for Nullable<T> {
    fn from(value: Option<T>) -> Self {
        if let Some(x) = value {
            Nullable::Some(x)
        } else {
            Nullable::None
        }
    }
}