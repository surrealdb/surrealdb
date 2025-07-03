//! Embedded database instance
//!
//! `SurrealDB` itself can be embedded in this library, allowing you to query it using the same
//! crate and API that you would use when connecting to it remotely via WebSockets or HTTP.
//! All storage engines are supported but you have to activate their feature
//! flags first.
//!
//! **NB**: Some storage engines like `TiKV` and `RocksDB` depend on non-Rust libraries so you need
//! to install those libraries before you can build this crate when you activate their feature
//! flags. Please refer to [these instructions](https://github.com/surrealdb/surrealdb/blob/main/doc/BUILDING.md)
//! for more details on how to install them. If you are on Linux and you use
//! [the Nix package manager](https://github.com/surrealdb/surrealdb/tree/main/pkg/nix#installing-nix)
//! you can just run
//!
//! ```bash
//! nix develop github:surrealdb/surrealdb
//! ```
//!
//! which will drop you into a shell with all the dependencies available. One tip you may find
//! useful is to only enable the in-memory engine (`kv-mem`) during development. Besides letting you not
//! worry about those dependencies on your dev machine, it allows you to keep compile times low
//! during development while allowing you to test your code fully.
//!
//! When running SurrealDB as an embedded database within Rust, using the correct release profile and
//! memory allocator can greatly improve the performance of the database core engine. In addition using
//! an optimised asynchronous runtime configuration can help speed up concurrent queries and increase
//! database throughput.
//!
//! In your project’s Cargo.toml file, ensure that the release profile uses the following configuration:
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
//! In your project’s Cargo.toml file, ensure that the allocator feature is among those enabled on the
//! surrealdb dependency:
//!
//! ```toml
//! [dependencies]
//! surrealdb = { version = "2", features = ["allocator", "storage-rocksdb"] }
//! ```
//!
//! When running SurrealDB within your Rust code, ensure that the asynchronous runtime is configured
//! correctly, making use of multiple threads, an increased stack size, and an optimised number of threads:
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
//! ```no_run
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

use crate::api::conn::LiveQueryParams;
use crate::api::err::Error;
use crate::{
	Result,
	api::{
		QueryResults as QueryResponse, Surreal,
		conn::{Command, Request},
	},
	method::Stats,
	opt::{IntoEndpoint, Table},
};
// use surrealdb_core::proto::surrealdb::rpc::{LiveParams, RawQueryParams, Request, UnsetParams};
// use surrealdb_core::proto::surrealdb::rpc::request::Command;

#[cfg(not(target_family = "wasm"))]
use anyhow::bail;
use async_channel::Sender;
#[cfg(not(target_family = "wasm"))]
use futures::stream::poll_fn;
use indexmap::IndexMap;
#[cfg(not(target_family = "wasm"))]
use std::pin::pin;
#[cfg(not(target_family = "wasm"))]
use std::task::{Poll, ready};
use std::{
	collections::{BTreeMap, HashMap},
	marker::PhantomData,
	mem,
	sync::Arc,
};
use surrealdb_core::dbs::{QueryResult, ResponseData, Variables};
use surrealdb_core::expr::LogicalPlan;
use surrealdb_core::expr::statements::{
	CreateStatement, DeleteStatement, InsertStatement, KillStatement, LiveStatement,
	SelectStatement, UpdateStatement, UpsertStatement,
};
use surrealdb_core::expr::{Cond, Function};
#[cfg(not(target_family = "wasm"))]
use surrealdb_core::kvs::export::Config as DbExportConfig;
use surrealdb_core::{
	dbs::{Notification, Session},
	expr::{Data, Field, Output, Value},
	iam,
	kvs::Datastore,
};
use tokio::sync::RwLock;
#[cfg(not(target_family = "wasm"))]
use tokio_util::bytes::BytesMut;
use uuid::Uuid;

#[cfg(not(target_family = "wasm"))]
use std::{future::Future, path::PathBuf};
#[cfg(not(target_family = "wasm"))]
use tokio::{
	fs::OpenOptions,
	io::{self, AsyncReadExt, AsyncWriteExt},
};

#[cfg(feature = "ml")]
use surrealdb_core::expr::Model;

#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use crate::api::conn::MlExportConfig;
#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use futures::StreamExt;
#[cfg(all(not(target_family = "wasm"), feature = "ml"))]
use surrealdb_core::{
	iam::{Action, ResourceKind, check::check_ns_db},
	kvs::{LockType, TransactionType},
	ml::storage::surml_file::SurMlFile,
	sql::statements::{DefineModelStatement, DefineStatement},
};

#[cfg(not(target_family = "wasm"))]
pub(crate) mod native;
#[cfg(target_family = "wasm")]
pub(crate) mod wasm;

pub(crate) mod grpc;
pub(crate) mod middleware;

type LiveQueryMap = HashMap<Uuid, Sender<Notification>>;

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
#[cfg(kv_fdb)]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-fdb-7_3")))]
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

fn process(responses: Vec<QueryResult>) -> QueryResponse {
	let mut map = IndexMap::with_capacity(responses.len());
	for (index, query_result) in responses.into_iter().enumerate() {
		map.insert(index, query_result);
	}
	QueryResponse {
		results: map.into(),
		..QueryResponse::new()
	}
}

async fn take(one: bool, responses: Vec<QueryResult>) -> Result<Value> {
	todo!("STU: take");
	// if let Some(result) = process(responses).results.swap_remove(&0) {
	// 	let value = result.result?;

	// 	match one {
	// 		true => match value {
	// 			Value::Array(mut array) => {
	// 				if let [ref mut value] = array[..] {
	// 					return Ok(mem::replace(value, Value::None));
	// 				}
	// 			}
	// 			Value::None | Value::Null => {}
	// 			value => return Ok(value),
	// 		},
	// 		false => return Ok(value),
	// 	}
	// }
	// match one {
	// 	true => Ok(Value::None),
	// 	false => Ok(Value::Array(Default::default())),
	// }
}

#[cfg(not(target_family = "wasm"))]
async fn export_file(
	kvs: &Datastore,
	sess: &Session,
	chn: async_channel::Sender<Vec<u8>>,
	config: Option<DbExportConfig>,
) -> Result<()> {
	let res = match config {
		Some(config) => kvs.export_with_config(sess, chn, config).await?.await,
		None => kvs.export(sess, chn).await?.await,
	};

	if let Err(error) = res {
		if let Some(surrealdb_core::err::Error::Channel(message)) = error.downcast_ref() {
			// This is not really an error. Just logging it for improved visibility.
			trace!("{message}");
			return Ok(());
		}

		return Err(error);
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
) -> Result<()> {
	// Ensure a NS and DB are set
	let (nsv, dbv) = check_ns_db(sess)?;
	// Check the permissions level
	kvs.check(sess, Action::View, ResourceKind::Model.on_db(&nsv, &dbv))?;
	// Start a new readonly transaction
	let tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
	// Attempt to get the model definition
	let info = tx.get_db_model(&nsv, &dbv, &name, &version).await?;
	// Export the file data in to the store
	let mut data = crate::obs::stream(info.hash.clone()).await?;
	// Process all stream values
	while let Some(Ok(bytes)) = data.next().await {
		if chn.send(bytes.to_vec()).await.is_err() {
			break;
		}
	}
	Ok(())
}
