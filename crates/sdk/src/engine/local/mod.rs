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
use surrealdb_core::kvs::Datastore;
#[cfg(not(target_family = "wasm"))]
use surrealdb_core::kvs::export::Config as DbExportConfig;
use surrealdb_core::rpc::DbResultError;
#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use surrealdb_core::{
	iam::{Action, ResourceKind, check::check_ns_db},
	ml::storage::surml_file::SurMlFile,
};
use surrealdb_types::{Notification, ToSql, Value, Variables};
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
use crate::{Connect, Result, Surreal};

#[cfg(not(target_family = "wasm"))]
pub(crate) mod native;
#[cfg(target_family = "wasm")]
pub(crate) mod wasm;

type LiveQueryMap = HashMap<Uuid, Sender<Result<Notification>>>;

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

/// FoundationDB database
///
/// # Examples
///
/// Instantiating a FoundationDB-backed instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::FDb;
///
/// let db = Surreal::new::<FDb>("path/to/fdb.cluster").await?;
/// # Ok(())
/// # }
/// ```
///
/// Instantiating a FoundationDB-backed strict instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::opt::Config;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::FDb;
///
/// let config = Config::default().strict();
/// let db = Surreal::new::<FDb>(("path/to/fdb.cluster", config)).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-fdb")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-fdb")))]
#[derive(Debug)]
pub struct FDb;

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
		..
	}: RequestData,
	kvs: &Arc<Datastore>,
	session: &Arc<RwLock<Session>>,
	vars: &Arc<RwLock<Variables>>,
	live_queries: &Arc<RwLock<LiveQueryMap>>,
) -> std::result::Result<Vec<QueryResult>, crate::Error> {
	match command {
		Command::Use {
			namespace,
			database,
		} => {
			let result =
				kvs.process_use(None, &mut *session.write().await, namespace, database).await?;
			Ok(vec![result])
		}
		Command::Signup {
			credentials,
		} => {
			let query_result = QueryResultBuilder::started_now();
			let signup_data =
				iam::signup::signup(kvs, &mut *session.write().await, credentials.into())
					.await
					.map_err(|e| DbResultError::InvalidAuth(e.to_string()))?;
			let token = signup_data.token.map(Value::String).unwrap_or(Value::None);
			let result = query_result.finish_with_result(Ok(token));

			Ok(vec![result])
		}
		Command::Signin {
			credentials,
		} => {
			let query_result = QueryResultBuilder::started_now();
			let signin_data =
				iam::signin::signin(kvs, &mut *session.write().await, credentials.into())
					.await
					.map_err(|e| DbResultError::InvalidAuth(e.to_string()))?;

			let result = query_result.finish_with_result(Ok(Value::String(signin_data.token)));

			Ok(vec![result])
		}
		Command::Authenticate {
			token,
		} => {
			let query_result = QueryResultBuilder::started_now();
			let result = match iam::verify::token(kvs, &mut *session.write().await, &token).await {
				Ok(_) => query_result.finish_with_result(Ok(Value::None)),
				Err(error) => query_result
					.finish_with_result(Err(DbResultError::InternalError(error.to_string()))),
			};
			Ok(vec![result])
		}
		Command::Invalidate => {
			let query_result = QueryResultBuilder::started_now();
			let result = match iam::clear::clear(&mut *session.write().await) {
				Ok(_) => query_result.finish_with_result(Ok(Value::None)),
				Err(error) => query_result
					.finish_with_result(Err(DbResultError::InternalError(error.to_string()))),
			};
			Ok(vec![result])
		}
		Command::RawQuery {
			txn: _,
			query,
			variables,
		} => {
			let mut vars = vars.read().await.clone();
			vars.extend(variables);
			let response = kvs.execute(query.as_ref(), &*session.read().await, Some(vars)).await?;
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
			let query_result = QueryResultBuilder::started_now();

			let (tx, rx) = crate::channel::bounded(1);
			let (mut writer, mut reader) = io::duplex(10_240);

			// Write to channel.
			let session = session.read().await.clone();
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
			let query_result = QueryResultBuilder::started_now();
			let (tx, rx) = crate::channel::bounded(1);
			let (mut writer, mut reader) = io::duplex(10_240);

			// Write to channel.
			let session = session.read().await;
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
			let query_result = QueryResultBuilder::started_now();
			let (tx, rx) = crate::channel::bounded(1);

			let kvs = kvs.clone();
			let session = session.read().await.clone();
			tokio::spawn(async move {
				let export = async {
					if let Err(error) = export_file(&kvs, &session, tx, config).await {
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
			let query_result = QueryResultBuilder::started_now();
			let (tx, rx) = crate::channel::bounded(1);

			let kvs = kvs.clone();
			let session = session.clone();
			tokio::spawn(async move {
				let export = async {
					if let Err(error) = export_ml(&kvs, &*session.read().await, tx, config).await {
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

			let responses = kvs
				.execute_import(&*session.read().await, Some(vars.read().await.clone()), stream)
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

			// Ensure a NS and DB are set
			let (nsv, dbv) = check_ns_db(&*session.read().await)?;
			// Check the permissions level
			kvs.check(&*session.read().await, Action::Edit, ResourceKind::Model.on_db(&nsv, &dbv))?;
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
						error: io::Error::new(
							io::ErrorKind::InvalidData,
							error.message.to_string(),
						),
					});
				}
			};
			// Convert the file back in to raw bytes
			let data = file.to_bytes();

			kvs.put_ml_model(
				&*session.read().await,
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
			let query_result = QueryResultBuilder::started_now();
			surrealdb_core::rpc::check_protected_param(&key)
				.map_err(|e| crate::Error::InternalError(e.to_string()))?;
			// Need to compute because certain keys might not be allowed to be set and those
			// should be rejected by an error.
			match value {
				Value::None => vars.write().await.remove(&key),
				v => vars.write().await.insert(key, v),
			};

			Ok(vec![query_result.finish()])
		}
		Command::Unset {
			key,
		} => {
			let query_result = QueryResultBuilder::started_now();
			vars.write().await.remove(&key);
			Ok(vec![query_result.finish()])
		}
		Command::SubscribeLive {
			uuid,
			notification_sender,
		} => {
			let query_result = QueryResultBuilder::started_now();
			live_queries.write().await.insert(uuid, notification_sender);
			Ok(vec![query_result.finish()])
		}
		Command::Kill {
			uuid,
		} => {
			live_queries.write().await.remove(&uuid);
			let results =
				kill_live_query(kvs, uuid, &*session.read().await, vars.read().await.clone())
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
			let results =
				kvs.execute(&sql, &*session.read().await, Some(vars.read().await.clone())).await?;
			Ok(results)
		}
	}
}
