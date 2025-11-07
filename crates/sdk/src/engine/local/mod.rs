//! Embedded database instance
//!
//! `SurrealDB` itself can be embedded in this library, allowing you to query it
//! using the same crate and API that you would use when connecting to it
//! remotely via WebSockets or HTTP. All storage engines are supported but you
//! have to activate their feature flags first.
//!
//! **NB**: Some storage engines like `TiKV` and `RocksDB` depend on non-Rust
//! libraries so you need to install those libraries before you can build this
//! crate when you activate their feature flags. Please refer to [these instructions](https://github.com/surrealdb/surrealdb/blob/main/doc/BUILDING.md)
//! for more details on how to install them. If you are on Linux and you use
//! [the Nix package manager](https://github.com/surrealdb/surrealdb/tree/main/pkg/nix#installing-nix)
//! you can just run
//!
//! ```bash
//! nix develop github:surrealdb/surrealdb
//! ```
//!
//! which will drop you into a shell with all the dependencies available. One
//! tip you may find useful is to only enable the in-memory engine (`kv-mem`)
//! during development. Besides letting you not worry about those dependencies
//! on your dev machine, it allows you to keep compile times low
//! during development while allowing you to test your code fully.
//!
//! When running SurrealDB as an embedded database within Rust, using the
//! correct release profile and memory allocator can greatly improve the
//! performance of the database core engine. In addition using an optimised
//! asynchronous runtime configuration can help speed up concurrent queries and
//! increase database throughput.
//!
//! In your project’s Cargo.toml file, ensure that the release profile uses the
//! following configuration:
//!
//! ```toml
//! [profile.release]
//! lto = true
//! strip = true
//! opt-level = 3
//! panic = 'abort'
//! codegen-units = 1
//! ```
//!
//! In your project’s Cargo.toml file, ensure that the allocator feature is
//! among those enabled on the surrealdb dependency:
//!
//! ```toml
//! [dependencies]
//! surrealdb = { version = "2", features = ["allocator", "storage-rocksdb"] }
//! ```
//!
//! When running SurrealDB within your Rust code, ensure that the asynchronous
//! runtime is configured correctly, making use of multiple threads, an
//! increased stack size, and an optimised number of threads:
//!
//! ```toml
//! [dependencies]
//! tokio = { version = "1", features = ["sync", "rt-multi-thread"] }
//! ```
//!
//! ```no_run
//! tokio::runtime::Builder::new_multi_thread()
//!     .enable_all()
//!     .thread_stack_size(10 * 1024 * 1024) // 10MiB
//!     .build()
//!     .unwrap()
//!     .block_on(async {
//!         // Your application code
//!     })
//! ```
//!
//! # Example
//!
//! ```no_compile
//! use std::borrow::Cow;
//! use serde::{Serialize, Deserialize};
//! use serde_json::json;
//! use surrealdb::{Error, Surreal};
//! use surrealdb::opt::auth::Root;
//! use surrealdb::engine::local::RocksDb;
//!
//! #[derive(Serialize, Deserialize)]
//! struct Person {
//!     title: String,
//!     name: Name,
//!     marketing: bool,
//! }
//!
//! // Pro tip: Replace String with Cow<'static, str> to
//! // avoid unnecessary heap allocations when inserting
//!
//! #[derive(Serialize, Deserialize)]
//! struct Name {
//!     first: Cow<'static, str>,
//!     last: Cow<'static, str>,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let db = Surreal::new::<RocksDb>("path/to/database/folder").await?;
//!
//!     // Select a specific namespace / database
//!     db.use_ns("namespace").use_db("database").await?;
//!
//!     // Create a new person with a random ID
//!     let created: Option<Person> = db.create("person")
//!         .content(Person {
//!             title: "Founder & CEO".into(),
//!             name: Name {
//!                 first: "Tobie".into(),
//!                 last: "Morgan Hitchcock".into(),
//!             },
//!             marketing: true,
//!         })
//!         .await?;
//!
//!     // Create a new person with a specific ID
//!     let created: Option<Person> = db.create(("person", "jaime"))
//!         .content(Person {
//!             title: "Founder & COO".into(),
//!             name: Name {
//!                 first: "Jaime".into(),
//!                 last: "Morgan Hitchcock".into(),
//!             },
//!             marketing: false,
//!         })
//!         .await?;
//!
//!     // Update a person record with a specific ID
//!     let updated: Option<Person> = db.update(("person", "jaime"))
//!         .merge(json!({"marketing": true}))
//!         .await?;
//!
//!     // Select all people records
//!     let people: Vec<Person> = db.select("person").await?;
//!
//!     // Perform a custom advanced query
//!     let query = r#"
//!         SELECT marketing, count()
//!         FROM type::table($table)
//!         GROUP BY marketing
//!     "#;
//!
//!     let groups = db.query(query)
//!         .bind(("table", "person"))
//!         .await?;
//!
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;
use std::marker::PhantomData;
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
use surrealdb_core::dbs::{QueryResult, QueryResultBuilder, Session};
use surrealdb_core::iam;
#[cfg(not(target_family = "wasm"))]
use surrealdb_core::kvs::export::Config as DbExportConfig;
use surrealdb_core::kvs::{Datastore, LockType, Transaction, TransactionType};
use surrealdb_core::rpc::DbResultError;
#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use surrealdb_core::{
	iam::{Action, ResourceKind, check::check_ns_db},
	ml::storage::surml_file::SurMlFile,
};
use surrealdb_types::{Notification, SurrealValue, ToSql, Value, Variables};
use tokio::sync::RwLock;
#[cfg(not(target_family = "wasm"))]
use tokio::{
	fs::OpenOptions,
	io::{self, AsyncReadExt, AsyncWriteExt},
};
#[cfg(not(target_family = "wasm"))]
use tokio_util::bytes::BytesMut;
use uuid::Uuid;

#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use crate::conn::MlExportConfig;
use crate::conn::{Command, RequestData};
use crate::opt::IntoEndpoint;
use crate::opt::auth::{AccessToken, RefreshToken, SecureToken, Token};
use crate::{Connect, Result, Surreal};

type LiveQueryMap = HashMap<Uuid, LiveQueryState>;

struct LiveQueryState {
	session_id: Uuid,
	sender: Sender<Result<Notification>>,
}

/// Per-session state for local/embedded connections
#[derive(Clone)]
pub(crate) struct LocalSessionState {
	session: Session,
	vars: Variables,
	transactions: HashMap<Uuid, Arc<Transaction>>,
}

impl Default for LocalSessionState {
	fn default() -> Self {
		Self {
			session: Session::default().with_rt(true),
			vars: Variables::default(),
			transactions: HashMap::new(),
		}
	}
}

#[derive(Clone)]
struct RouterState {
	kvs: Arc<Datastore>,
	live_queries: Arc<RwLock<LiveQueryMap>>,
	sessions: Arc<RwLock<HashMap<Uuid, LocalSessionState>>>,
}

#[cfg(not(target_family = "wasm"))]
pub(crate) mod native;
#[cfg(target_family = "wasm")]
pub(crate) mod wasm;

/// In-memory database
///
/// # Examples
///
/// Instantiating a global instance
///
/// ```
/// use std::sync::LazyLock;
/// use surrealdb::{Result, Surreal};
/// use surrealdb::engine::local::Db;
/// use surrealdb::engine::local::Mem;
///
/// static DB: LazyLock<Surreal<Db>> = LazyLock::new(Surreal::init);
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     DB.connect::<Mem>(()).await?;
///
///     Ok(())
/// }
/// ```
///
/// Instantiating an in-memory instance
///
/// ```
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::Mem;
///
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let db = Surreal::new::<Mem>(()).await?;
/// # Ok(())
/// # }
/// ```
///
/// Instantiating an in-memory strict instance
///
/// ```
/// use surrealdb::opt::Config;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::Mem;
///
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let config = Config::default().strict();
/// let db = Surreal::new::<Mem>(config).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-mem")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-mem")))]
#[derive(Debug)]
pub struct Mem;

/// RocksDB database
///
/// # Examples
///
/// Instantiating a RocksDB-backed instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::RocksDb;
///
/// let db = Surreal::new::<RocksDb>("path/to/database-folder").await?;
/// # Ok(())
/// # }
/// ```
///
/// Instantiating a RocksDB-backed strict instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::opt::Config;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::RocksDb;
///
/// let config = Config::default().strict();
/// let db = Surreal::new::<RocksDb>(("path/to/database-folder", config)).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-rocksdb")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-rocksdb")))]
#[derive(Debug)]
pub struct RocksDb;

/// IndxDB database
///
/// # Examples
///
/// Instantiating a IndxDB-backed instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::IndxDb;
///
/// let db = Surreal::new::<IndxDb>("DatabaseName").await?;
/// # Ok(())
/// # }
/// ```
///
/// Instantiating an IndxDB-backed strict instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::opt::Config;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::IndxDb;
///
/// let config = Config::default().strict();
/// let db = Surreal::new::<IndxDb>(("DatabaseName", config)).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-indxdb")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-indxdb")))]
#[derive(Debug)]
pub struct IndxDb;

/// TiKV database
///
/// # Examples
///
/// Instantiating a TiKV instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::TiKv;
///
/// let db = Surreal::new::<TiKv>("localhost:2379").await?;
/// # Ok(())
/// # }
/// ```
///
/// Instantiating a TiKV strict instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::opt::Config;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::TiKv;
///
/// let config = Config::default().strict();
/// let db = Surreal::new::<TiKv>(("localhost:2379", config)).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-tikv")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-tikv")))]
#[derive(Debug)]
pub struct TiKv;

/// SurrealKV database
///
/// # Examples
///
/// Instantiating a SurrealKV-backed instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::SurrealKv;
///
/// let db = Surreal::new::<SurrealKv>("path/to/database-folder").await?;
/// # Ok(())
/// # }
/// ```
///
/// Instantiating a SurrealKV-backed strict instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::opt::Config;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::SurrealKv;
///
/// let config = Config::default().strict();
/// let db = Surreal::new::<SurrealKv>(("path/to/database-folder", config)).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-surrealkv")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-surrealkv")))]
#[derive(Debug)]
pub struct SurrealKv;

/// An embedded database
#[derive(Debug, Clone)]
pub struct Db(());

impl Surreal<Db> {
	/// Connects to a specific database endpoint, saving the connection on the
	/// static client
	pub fn connect<P>(&self, address: impl IntoEndpoint<P, Client = Db>) -> Connect<Db, ()> {
		Connect {
			surreal: self.inner.clone().into(),
			address: address.into_endpoint(),
			capacity: 0,
			response_type: PhantomData,
		}
	}
}

#[cfg(not(target_family = "wasm"))]
async fn export_file(
	kvs: &Datastore,
	sess: &Session,
	chn: async_channel::Sender<Vec<u8>>,
	config: Option<DbExportConfig>,
) -> std::result::Result<(), crate::Error> {
	let res = match config {
		Some(config) => kvs.export_with_config(sess, chn, config).await?.await,
		None => kvs.export(sess, chn).await?.await,
	};

	if let Err(error) = res {
		// Check if this is a channel error by examining the error message
		let error_str = error.to_string();
		if error_str.contains("channel") || error_str.contains("Channel") {
			// This is not really an error. Just logging it for improved visibility.
			trace!("{error_str}");
			return Ok(());
		}

		return Err(crate::Error::InternalError(error.to_string()));
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
) -> std::result::Result<(), crate::Error> {
	let (nsv, dbv) = check_ns_db(sess).map_err(|e| crate::Error::InternalError(e.to_string()))?;
	// Check the permissions level
	kvs.check(sess, Action::View, ResourceKind::Model.on_db(&nsv, &dbv))
		.map_err(|e| crate::Error::InternalError(e.to_string()))?;

	// Attempt to get the model definition
	let Some(model) = kvs
		.get_db_model(&nsv, &dbv, &name, &version)
		.await
		.map_err(|e| crate::Error::InternalError(e.to_string()))?
	else {
		// Attempt to get the model definition
		return Err(crate::Error::InternalError("Model not found".to_string()));
	};
	// Export the file data in to the store
	let mut data = surrealdb_core::obs::stream(model.hash.clone())
		.await
		.map_err(|e| crate::Error::InternalError(e.to_string()))?;
	// Process all stream values
	while let Some(Ok(bytes)) = data.next().await {
		if chn.send(bytes.to_vec()).await.is_err() {
			break;
		}
	}
	Ok(())
}

#[cfg(not(target_family = "wasm"))]
async fn copy<'a, R, W>(
	path: PathBuf,
	reader: &'a mut R,
	writer: &'a mut W,
) -> std::result::Result<(), crate::Error>
where
	R: tokio::io::AsyncRead + Unpin + ?Sized,
	W: tokio::io::AsyncWrite + Unpin + ?Sized,
{
	io::copy(reader, writer).await.map(|_| ()).map_err(|error| crate::Error::FileRead {
		path,
		error,
	})
}

async fn kill_live_query(
	kvs: &Datastore,
	id: Uuid,
	session: &Session,
	vars: Variables,
) -> std::result::Result<Vec<QueryResult>, DbResultError> {
	let sql = format!("KILL {id}");

	let results = kvs.execute(&sql, session, Some(vars)).await?;
	Ok(results)
}

async fn router(
	RequestData {
		command,
		session_id,
		..
	}: RequestData,
	RouterState {
		kvs,
		sessions,
		live_queries,
	}: RouterState,
) -> std::result::Result<Vec<QueryResult>, crate::Error> {
	match command {
		Command::Use {
			namespace,
			database,
		} => {
			let session_id = session_id.expect("use command must have a session ID");
			let mut sessions_lock = sessions.write().await;
			let state = sessions_lock.get_mut(&session_id).expect("session should exist");
			let result = kvs.process_use(None, &mut state.session, namespace, database).await?;
			Ok(vec![result])
		}
		Command::Signup {
			credentials,
		} => {
			let session_id = session_id.expect("signup command must have a session ID");
			let query_result = QueryResultBuilder::started_now();
			let mut sessions_lock = sessions.write().await;
			let state = sessions_lock.get_mut(&session_id).expect("session should exist");
			let signup_data = iam::signup::signup(&kvs, &mut state.session, credentials.into())
				.await
				.map_err(|e| DbResultError::InvalidAuth(e.to_string()))?;
			let token = match signup_data {
				iam::Token::Access(token) => Token {
					access: AccessToken(SecureToken(token)),
					refresh: None,
				},
				iam::Token::WithRefresh {
					access: token,
					refresh,
				} => Token {
					access: AccessToken(SecureToken(token)),
					refresh: Some(RefreshToken(SecureToken(refresh))),
				},
			};
			let result = query_result.finish_with_result(Ok(token.into_value()));

			Ok(vec![result])
		}
		Command::Signin {
			credentials,
		} => {
			let session_id = session_id.expect("signin command must have a session ID");
			let query_result = QueryResultBuilder::started_now();
			let mut sessions_lock = sessions.write().await;
			let state = sessions_lock.get_mut(&session_id).expect("session should exist");
			let signin_data = iam::signin::signin(&kvs, &mut state.session, credentials.into())
				.await
				.map_err(|e| DbResultError::InvalidAuth(e.to_string()))?;
			let token = match signin_data {
				iam::Token::Access(token) => Token {
					access: AccessToken(SecureToken(token)),
					refresh: None,
				},
				iam::Token::WithRefresh {
					access,
					refresh,
				} => Token {
					access: AccessToken(SecureToken(access)),
					refresh: Some(RefreshToken(SecureToken(refresh))),
				},
			};
			let result = query_result.finish_with_result(Ok(token.into_value()));
			Ok(vec![result])
		}
		Command::Authenticate {
			token,
		} => {
			let session_id = session_id.expect("authenticate command must have a session ID");
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
			let mut sessions_lock = sessions.write().await;
			let state = sessions_lock.get_mut(&session_id).expect("session should exist");
			let result = match iam::verify::token(&kvs, &mut state.session, access).await {
				// Authentication successful - return the original token
				Ok(_) => query_result.finish_with_result(Ok(token.into_value())),
				Err(error) => {
					// Automatic refresh token handling:
					// If the access token is expired and we have a refresh token,
					// automatically attempt to refresh and return new tokens.
					if with_refresh && error.to_string().contains("token has expired") {
						let result = match token.refresh(&kvs, &mut state.session).await {
							Ok(token) => query_result.finish_with_result(Ok(token.into_value())),
							Err(error) => query_result.finish_with_result(Err(
								DbResultError::InternalError(error.to_string()),
							)),
						};
						return Ok(vec![result]);
					}
					// If authentication failed and automatic refresh isn't applicable,
					// return the authentication error
					query_result
						.finish_with_result(Err(DbResultError::InternalError(error.to_string())))
				}
			};
			Ok(vec![result])
		}
		Command::Refresh {
			token,
		} => {
			let session_id = session_id.expect("refresh command must have a session ID");
			// Refresh command: Exchange a refresh token for new access and refresh tokens
			let query_result = QueryResultBuilder::started_now();
			let mut sessions_lock = sessions.write().await;
			let state = sessions_lock.get_mut(&session_id).expect("session should exist");
			let result = match token.refresh(&kvs, &mut state.session).await {
				Ok(token) => query_result.finish_with_result(Ok(token.into_value())),
				Err(error) => query_result
					.finish_with_result(Err(DbResultError::InternalError(error.to_string()))),
			};
			Ok(vec![result])
		}
		Command::Invalidate => {
			let session_id = session_id.expect("invalidate command must have a session ID");
			let query_result = QueryResultBuilder::started_now();
			let mut sessions_lock = sessions.write().await;
			let state = sessions_lock.get_mut(&session_id).expect("session should exist");
			let result = match iam::clear::clear(&mut state.session) {
				Ok(_) => query_result.finish_with_result(Ok(Value::None)),
				Err(error) => query_result
					.finish_with_result(Err(DbResultError::InternalError(error.to_string()))),
			};
			Ok(vec![result])
		}
		Command::Begin => {
			let session_id = session_id.expect("begin command must have a session ID");
			let query_result = QueryResultBuilder::started_now();
			let result = match kvs.transaction(TransactionType::Write, LockType::Optimistic).await {
				Ok(txn) => {
					let id = Uuid::now_v7();
					let mut sessions_lock = sessions.write().await;
					let state = sessions_lock.get_mut(&session_id).expect("session should exist");
					state.transactions.insert(id, std::sync::Arc::new(txn));
					query_result.finish_with_result(Ok(Value::Uuid(id.into())))
				}
				Err(error) => query_result
					.finish_with_result(Err(DbResultError::InternalError(error.to_string()))),
			};
			Ok(vec![result])
		}
		Command::Revoke {
			token,
		} => {
			// Revoke command: Explicitly invalidate a refresh token to prevent future use
			let query_result = QueryResultBuilder::started_now();
			let result = match token.revoke_refresh_token(&kvs).await {
				Ok(_) => query_result.finish_with_result(Ok(Value::None)),
				Err(error) => query_result
					.finish_with_result(Err(DbResultError::InternalError(error.to_string()))),
			};
			Ok(vec![result])
		}
		Command::Rollback {
			txn,
		} => {
			let session_id = session_id.expect("rollback command must have a session ID");
			let mut sessions_lock = sessions.write().await;
			let state = sessions_lock.get_mut(&session_id).expect("session should exist");
			let tx = state.transactions.remove(&txn);
			if let Some(tx) = tx {
				tx.cancel().await?;
			}
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		Command::Commit {
			txn,
		} => {
			let session_id = session_id.expect("commit command must have a session ID");
			let mut sessions_lock = sessions.write().await;
			let state = sessions_lock.get_mut(&session_id).expect("session should exist");
			let tx = state.transactions.remove(&txn);
			if let Some(tx) = tx {
				tx.commit().await?;
			}
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		Command::Query {
			txn,
			query,
			variables,
		} => {
			let session_id = session_id.expect("query command must have a session ID");
			// Get the session state
			let sessions_lock = sessions.read().await;
			let state = sessions_lock.get(&session_id).expect("session should exist");

			// Merge session vars with query vars
			let mut vars = state.vars.clone();
			vars.extend(variables);

			// If a transaction UUID is provided, we need to retrieve it and use it
			let response = if let Some(txn_id) = txn {
				// Retrieve the transaction from storage
				let tx_option = state.transactions.get(&txn_id).cloned();
				if let Some(tx) = tx_option {
					// Execute with the existing transaction
					kvs.execute_with_transaction(query.as_ref(), &state.session, Some(vars), tx)
						.await?
				} else {
					// Transaction not found - return error
					return Ok(vec![QueryResultBuilder::started_now().finish_with_result(Err(
						DbResultError::InternalError("Transaction not found".to_string()),
					))]);
				}
			} else {
				// No transaction - use normal execution
				kvs.execute(query.as_ref(), &state.session, Some(vars)).await?
			};

			Ok(response)
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
		} => Err(crate::Error::BackupsNotSupported.into()),

		#[cfg(any(target_family = "wasm", not(feature = "ml")))]
		Command::ExportMl {
			..
		}
		| Command::ExportBytesMl {
			..
		}
		| Command::ImportMl {
			..
		} => Err(crate::Error::BackupsNotSupported),

		#[cfg(not(target_family = "wasm"))]
		Command::ExportFile {
			path: file,
			config,
		} => {
			let session_id = session_id.expect("export file command must have a session ID");
			let query_result = QueryResultBuilder::started_now();

			let (tx, rx) = crate::channel::bounded(1);
			let (mut writer, mut reader) = io::duplex(10_240);

			// Write to channel.
			let state_clone =
				sessions.read().await.get(&session_id).expect("session should exist").clone();
			let export = export_file(&kvs, &state_clone.session, tx, config);

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
					return Err(crate::Error::FileOpen {
						path: file,
						error,
					});
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
			let session_id = session_id.expect("export ML command must have a session ID");
			let query_result = QueryResultBuilder::started_now();
			let (tx, rx) = crate::channel::bounded(1);
			let (mut writer, mut reader) = io::duplex(10_240);

			// Write to channel.
			let sessions_lock = sessions.read().await;
			let state = sessions_lock.get(&session_id).expect("session should exist");
			let export = export_ml(&kvs, &state.session, tx, config);

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
					return Err(crate::Error::FileOpen {
						path,
						error,
					});
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
			let session_id = session_id.expect("export bytes command must have a session ID");
			let query_result = QueryResultBuilder::started_now();
			let (tx, rx) = crate::channel::bounded(1);

			let kvs = kvs.clone();
			let state_clone =
				sessions.read().await.get(&session_id).expect("session should exist").clone();
			tokio::spawn(async move {
				let export = async {
					if let Err(error) = export_file(&kvs, &state_clone.session, tx, config).await {
						let _ = bytes.send(Err(error)).await;
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
			let session_id = session_id.expect("export bytes ML command must have a session ID");
			let query_result = QueryResultBuilder::started_now();
			let (tx, rx) = crate::channel::bounded(1);

			let kvs = kvs.clone();
			let state_clone =
				sessions.read().await.get(&session_id).expect("session should exist").clone();
			tokio::spawn(async move {
				let export = async {
					if let Err(error) = export_ml(&kvs, &state_clone.session, tx, config).await {
						let _ = bytes.send(Err(error)).await;
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
			let session_id = session_id.expect("import file command must have a session ID");
			let query_result = QueryResultBuilder::started_now();
			let file = match OpenOptions::new().read(true).open(&path).await {
				Ok(path) => path,
				Err(error) => {
					return Err(crate::Error::FileOpen {
						path,
						error,
					});
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
					Err(e) => {
						let error = surrealdb_types::anyhow::Error::new(e);
						Poll::Ready(Some(Err(error)))
					}
				}
			});

			let sessions_lock = sessions.read().await;
			let state = sessions_lock.get(&session_id).expect("session should exist");
			let responses = kvs
				.execute_import(&state.session, Some(state.vars.clone()), stream)
				.await
				.map_err(|e| crate::Error::InternalError(e.to_string()))?;

			for response in responses {
				response.result?;
			}

			Ok(vec![query_result.finish()])
		}
		#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
		Command::ImportMl {
			path,
		} => {
			let session_id = session_id.expect("import ML command must have a session ID");
			let query_result = QueryResultBuilder::started_now();
			let mut file = match OpenOptions::new().read(true).open(&path).await {
				Ok(path) => path,
				Err(error) => {
					return Err(crate::Error::FileOpen {
						path,
						error,
					});
				}
			};

			// Get the session for this session_id
			let sessions_lock = sessions.read().await;
			let state = sessions_lock.get(&session_id).expect("session should exist");

			// Ensure a NS and DB are set
			let (nsv, dbv) = check_ns_db(&state.session)?;
			// Check the permissions level
			kvs.check(&state.session, Action::Edit, ResourceKind::Model.on_db(&nsv, &dbv))?;
			// Create a new buffer
			let mut buffer = Vec::new();
			// Load all the uploaded file chunks
			if let Err(error) = file.read_to_end(&mut buffer).await {
				return Err(crate::Error::FileRead {
					path,
					error,
				});
			}
			// Check that the SurrealML file is valid
			let file = match SurMlFile::from_bytes(buffer) {
				Ok(file) => file,
				Err(error) => {
					return Err(crate::Error::FileRead {
						path,
						error: io::Error::new(io::ErrorKind::InvalidData, error.message.clone()),
					});
				}
			};
			// Convert the file back in to raw bytes
			let data = file.to_bytes();

			kvs.put_ml_model(
				&state.session,
				&file.header.name.to_string(),
				&file.header.version.to_string(),
				&file.header.description.to_string(),
				data,
			)
			.await?;

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
			let session_id = session_id.expect("set command must have a session ID");
			let query_result = QueryResultBuilder::started_now();
			surrealdb_core::rpc::check_protected_param(&key)
				.map_err(|e| crate::Error::InternalError(e.to_string()))?;
			// Need to compute because certain keys might not be allowed to be set and those
			// should be rejected by an error.
			let mut sessions_lock = sessions.write().await;
			let state = sessions_lock.get_mut(&session_id).expect("session should exist");
			match value {
				Value::None => state.vars.remove(&key),
				v => state.vars.insert(key, v),
			};

			Ok(vec![query_result.finish()])
		}
		Command::Unset {
			key,
		} => {
			let session_id = session_id.expect("unset command must have a session ID");
			let query_result = QueryResultBuilder::started_now();
			let mut sessions_lock = sessions.write().await;
			let state = sessions_lock.get_mut(&session_id).expect("session should exist");
			state.vars.remove(&key);
			Ok(vec![query_result.finish()])
		}
		Command::SubscribeLive {
			uuid,
			notification_sender,
		} => {
			let session_id = session_id.expect("subscribe live command must have a session ID");
			let query_result = QueryResultBuilder::started_now();
			live_queries.write().await.insert(
				uuid,
				LiveQueryState {
					session_id,
					sender: notification_sender,
				},
			);
			Ok(vec![query_result.finish()])
		}
		Command::Kill {
			uuid,
		} => {
			let session_id = session_id.expect("kill live query command must have a session ID");
			live_queries.write().await.remove(&uuid);
			let sessions_lock = sessions.read().await;
			let state = sessions_lock.get(&session_id).expect("session should exist");
			let results = kill_live_query(&kvs, uuid, &state.session, state.vars.clone()).await?;
			Ok(results)
		}

		Command::Run {
			name,
			version,
			args,
		} => {
			let session_id = session_id.expect("run command must have a session ID");
			// Format arguments as comma-separated SQL values
			let formatted_args = args.iter().map(|v| v.to_sql()).collect::<Vec<_>>().join(", ");

			// Build SQL query: name<version>(args) or name(args)
			let sql = match version {
				Some(v) => format!("{name}<{v}>({formatted_args})"),
				None => format!("{name}({formatted_args})"),
			};

			// Execute the query
			let sessions_lock = sessions.read().await;
			let state = sessions_lock.get(&session_id).expect("session should exist");
			let results = kvs.execute(&sql, &state.session, Some(state.vars.clone())).await?;
			Ok(results)
		}
	}
}
