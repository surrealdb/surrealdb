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

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
#[cfg(target_arch = "wasm32")]
pub(crate) mod wasm;

use crate::{
	api::{
		conn::{Command, DbResponse, RequestData},
		Connect, Response as QueryResponse, Result, Surreal,
	},
	method::Stats,
	opt::IntoEndpoint,
};
use channel::Sender;
use indexmap::IndexMap;
use std::{
	collections::{BTreeMap, HashMap},
	marker::PhantomData,
	mem,
	sync::Arc,
	time::Duration,
};
use surrealdb_core::{
	dbs::{Notification, Response, Session},
	kvs::Datastore,
	sql::{
		statements::{
			CreateStatement, DeleteStatement, InsertStatement, KillStatement, SelectStatement,
			UpdateStatement, UpsertStatement,
		},
		Data, Field, Output, Query, Statement, Uuid, Value,
	},
};

#[cfg(not(target_arch = "wasm32"))]
use crate::api::err::Error;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
#[cfg(not(target_arch = "wasm32"))]
use tokio::{
	fs::OpenOptions,
	io::{self, AsyncReadExt, AsyncWriteExt},
};

#[cfg(all(not(target_arch = "wasm32"), feature = "ml"))]
use crate::api::conn::MlExportConfig;
#[cfg(all(not(target_arch = "wasm32"), feature = "ml"))]
use futures::StreamExt;
#[cfg(all(not(target_arch = "wasm32"), feature = "ml"))]
use surrealdb_core::{
	iam::{check::check_ns_db, Action, ResourceKind},
	kvs::{LockType, TransactionType},
	ml::storage::surml_file::SurMlFile,
	sql::statements::{DefineModelStatement, DefineStatement},
};

use super::value_to_values;

const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(10);

/// In-memory database
///
/// # Examples
///
/// Instantiating a global instance
///
/// ```
/// use once_cell::sync::Lazy;
/// use surrealdb::{Result, Surreal};
/// use surrealdb::engine::local::Db;
/// use surrealdb::engine::local::Mem;
///
/// static DB: Lazy<Surreal<Db>> = Lazy::new(Surreal::init);
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

/// File database
///
/// # Examples
///
/// Instantiating a file-backed instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::File;
///
/// let db = Surreal::new::<File>("path/to/database-folder").await?;
/// # Ok(())
/// # }
/// ```
///
/// Instantiating a file-backed strict instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::opt::Config;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::File;
///
/// let config = Config::default().strict();
/// let db = Surreal::new::<File>(("path/to/database-folder", config)).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-rocksdb")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-rocksdb")))]
#[derive(Debug)]
pub struct File;

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
/// use surrealdb::engine::local::SurrealKV;
///
/// let db = Surreal::new::<SurrealKV>("path/to/database-folder").await?;
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
/// use surrealdb::engine::local::SurrealKV;
///
/// let config = Config::default().strict();
/// let db = Surreal::new::<SurrealKV>(("path/to/database-folder", config)).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-surrealkv")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-surrealkv")))]
#[derive(Debug)]
pub struct SurrealKV;

/// SurrealCS database
///
/// # Examples
///
/// Instantiating a SurrealCS-backed instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::SurrealCS;
///
/// let db = Surreal::new::<SurrealCS>("path/to/database-folder").await?;
/// # Ok(())
/// # }
/// ```
///
/// Instantiating a SurrealCS-backed strict instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::opt::Config;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::SurrealCS;
///
/// let config = Config::default().strict();
/// let db = Surreal::new::<SurrealCS>(("path/to/database-folder", config)).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-surrealcs")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-surrealcs")))]
#[derive(Debug)]
pub struct SurrealCS;

/// An embedded database
#[derive(Debug, Clone)]
pub struct Db(());

impl Surreal<Db> {
	/// Connects to a specific database endpoint, saving the connection on the static client
	pub fn connect<P>(&self, address: impl IntoEndpoint<P, Client = Db>) -> Connect<Db, ()> {
		Connect {
			router: self.router.clone(),
			engine: PhantomData,
			address: address.into_endpoint(),
			capacity: 0,
			waiter: self.waiter.clone(),
			response_type: PhantomData,
		}
	}
}

fn process(responses: Vec<Response>) -> QueryResponse {
	let mut map = IndexMap::with_capacity(responses.len());
	for (index, response) in responses.into_iter().enumerate() {
		let stats = Stats {
			execution_time: Some(response.time),
		};
		match response.result {
			Ok(value) => map.insert(index, (stats, Ok(value))),
			Err(error) => map.insert(index, (stats, Err(error.into()))),
		};
	}
	QueryResponse {
		results: map,
		..QueryResponse::new()
	}
}

async fn take(one: bool, responses: Vec<Response>) -> Result<Value> {
	if let Some((_stats, result)) = process(responses).results.swap_remove(&0) {
		let value = result?;
		match one {
			true => match value {
				Value::Array(mut array) => {
					if let [value] = &mut array.0[..] {
						return Ok(mem::take(value));
					}
				}
				Value::None | Value::Null => {}
				value => return Ok(value),
			},
			false => return Ok(value),
		}
	}
	match one {
		true => Ok(Value::None),
		false => Ok(Value::Array(Default::default())),
	}
}

#[cfg(not(target_arch = "wasm32"))]
async fn export_file(kvs: &Datastore, sess: &Session, chn: channel::Sender<Vec<u8>>) -> Result<()> {
	if let Err(error) = kvs.export(sess, chn).await?.await {
		if let crate::error::Db::Channel(message) = error {
			// This is not really an error. Just logging it for improved visibility.
			trace!("{message}");
			return Ok(());
		}
		return Err(error.into());
	}
	Ok(())
}

#[cfg(all(not(target_arch = "wasm32"), feature = "ml"))]
async fn export_ml(
	kvs: &Datastore,
	sess: &Session,
	chn: channel::Sender<Vec<u8>>,
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
	let mut data = crate::obs::stream(info.hash.to_owned()).await?;
	// Process all stream values
	while let Some(Ok(bytes)) = data.next().await {
		if chn.send(bytes.to_vec()).await.is_err() {
			break;
		}
	}
	Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn copy<'a, R, W>(
	path: PathBuf,
	reader: &'a mut R,
	writer: &'a mut W,
) -> std::result::Result<(), crate::Error>
where
	R: tokio::io::AsyncRead + Unpin + ?Sized,
	W: tokio::io::AsyncWrite + Unpin + ?Sized,
{
	io::copy(reader, writer).await.map(|_| ()).map_err(|error| {
		crate::Error::Api(crate::error::Api::FileRead {
			path,
			error,
		})
	})
}

async fn kill_live_query(
	kvs: &Datastore,
	id: Uuid,
	session: &Session,
	vars: BTreeMap<String, Value>,
) -> Result<Value> {
	let mut query = Query::default();
	let mut kill = KillStatement::default();
	kill.id = id.into();
	query.0 .0 = vec![Statement::Kill(kill)];
	let response = kvs.process(query, session, Some(vars)).await?;
	take(true, response).await
}

async fn router(
	RequestData {
		command,
		..
	}: RequestData,
	kvs: &Arc<Datastore>,
	session: &mut Session,
	vars: &mut BTreeMap<String, Value>,
	live_queries: &mut HashMap<Uuid, Sender<Notification>>,
) -> Result<DbResponse> {
	match command {
		Command::Use {
			namespace,
			database,
		} => {
			if let Some(ns) = namespace {
				session.ns = Some(ns);
			}
			if let Some(db) = database {
				session.db = Some(db);
			}
			Ok(DbResponse::Other(Value::None))
		}
		Command::Signup {
			credentials,
		} => {
			let response = crate::iam::signup::signup(kvs, session, credentials).await?;
			Ok(DbResponse::Other(response.into()))
		}
		Command::Signin {
			credentials,
		} => {
			let response = crate::iam::signin::signin(kvs, session, credentials).await?;
			Ok(DbResponse::Other(response.into()))
		}
		Command::Authenticate {
			token,
		} => {
			crate::iam::verify::token(kvs, session, &token).await?;
			Ok(DbResponse::Other(Value::None))
		}
		Command::Invalidate => {
			crate::iam::clear::clear(session)?;
			Ok(DbResponse::Other(Value::None))
		}
		Command::Create {
			what,
			data,
		} => {
			let mut query = Query::default();
			let statement = {
				let mut stmt = CreateStatement::default();
				stmt.what = value_to_values(what);
				stmt.data = data.map(Data::ContentExpression);
				stmt.output = Some(Output::After);
				stmt
			};
			query.0 .0 = vec![Statement::Create(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(true, response).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Upsert {
			what,
			data,
		} => {
			let mut query = Query::default();
			let one = what.is_thing();
			let statement = {
				let mut stmt = UpsertStatement::default();
				stmt.what = value_to_values(what);
				stmt.data = data.map(Data::ContentExpression);
				stmt.output = Some(Output::After);
				stmt
			};
			query.0 .0 = vec![Statement::Upsert(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Update {
			what,
			data,
		} => {
			let mut query = Query::default();
			let one = what.is_thing();
			let statement = {
				let mut stmt = UpdateStatement::default();
				stmt.what = value_to_values(what);
				stmt.data = data.map(Data::ContentExpression);
				stmt.output = Some(Output::After);
				stmt
			};
			query.0 .0 = vec![Statement::Update(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Insert {
			what,
			data,
		} => {
			let mut query = Query::default();
			let one = !data.is_array();
			let statement = {
				let mut stmt = InsertStatement::default();
				stmt.into = what;
				stmt.data = Data::SingleExpression(data);
				stmt.output = Some(Output::After);
				stmt
			};
			query.0 .0 = vec![Statement::Insert(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Patch {
			what,
			data,
		} => {
			let mut query = Query::default();
			let one = what.is_thing();
			let statement = {
				let mut stmt = UpdateStatement::default();
				stmt.what = value_to_values(what);
				stmt.data = data.map(Data::PatchExpression);
				stmt.output = Some(Output::After);
				stmt
			};
			query.0 .0 = vec![Statement::Update(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Merge {
			what,
			data,
		} => {
			let mut query = Query::default();
			let one = what.is_thing();
			let statement = {
				let mut stmt = UpdateStatement::default();
				stmt.what = value_to_values(what);
				stmt.data = data.map(Data::MergeExpression);
				stmt.output = Some(Output::After);
				stmt
			};
			query.0 .0 = vec![Statement::Update(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Select {
			what,
		} => {
			let mut query = Query::default();
			let one = what.is_thing();
			let statement = {
				let mut stmt = SelectStatement::default();
				stmt.what = value_to_values(what);
				stmt.expr.0 = vec![Field::All];
				stmt
			};
			query.0 .0 = vec![Statement::Select(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Delete {
			what,
		} => {
			let mut query = Query::default();
			let one = what.is_thing();
			let statement = {
				let mut stmt = DeleteStatement::default();
				stmt.what = value_to_values(what);
				stmt.output = Some(Output::Before);
				stmt
			};
			query.0 .0 = vec![Statement::Delete(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Query {
			query,
			mut variables,
		} => {
			let mut vars = vars.clone();
			vars.append(&mut variables);
			let response = kvs.process(query, &*session, Some(vars)).await?;
			let response = process(response);
			Ok(DbResponse::Query(response))
		}

		#[cfg(target_arch = "wasm32")]
		Command::ExportFile {
			..
		}
		| Command::ExportBytes {
			..
		}
		| Command::ImportFile {
			..
		} => Err(crate::api::Error::BackupsNotSupported.into()),

		#[cfg(any(target_arch = "wasm32", not(feature = "ml")))]
		Command::ExportMl {
			..
		}
		| Command::ExportBytesMl {
			..
		}
		| Command::ImportMl {
			..
		} => Err(crate::api::Error::BackupsNotSupported.into()),

		#[cfg(not(target_arch = "wasm32"))]
		Command::ExportFile {
			path: file,
		} => {
			let (tx, rx) = crate::channel::bounded(1);
			let (mut writer, mut reader) = io::duplex(10_240);

			// Write to channel.
			let export = export_file(kvs, session, tx);

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
					return Err(Error::FileOpen {
						path: file,
						error,
					}
					.into());
				}
			};

			// Copy from pipe to output.
			let copy = copy(file, &mut reader, &mut output);

			tokio::try_join!(export, bridge, copy)?;
			Ok(DbResponse::Other(Value::None))
		}

		#[cfg(all(not(target_arch = "wasm32"), feature = "ml"))]
		Command::ExportMl {
			path,
			config,
		} => {
			let (tx, rx) = crate::channel::bounded(1);
			let (mut writer, mut reader) = io::duplex(10_240);

			// Write to channel.
			let export = export_ml(kvs, session, tx, config);

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
					return Err(Error::FileOpen {
						path,
						error,
					}
					.into());
				}
			};

			// Copy from pipe to output.
			let copy = copy(path, &mut reader, &mut output);

			tokio::try_join!(export, bridge, copy)?;
			Ok(DbResponse::Other(Value::None))
		}

		#[cfg(not(target_arch = "wasm32"))]
		Command::ExportBytes {
			bytes,
		} => {
			let (tx, rx) = crate::channel::bounded(1);

			let kvs = kvs.clone();
			let session = session.clone();
			tokio::spawn(async move {
				let export = async {
					if let Err(error) = export_file(&kvs, &session, tx).await {
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

			Ok(DbResponse::Other(Value::None))
		}
		#[cfg(all(not(target_arch = "wasm32"), feature = "ml"))]
		Command::ExportBytesMl {
			bytes,
			config,
		} => {
			let (tx, rx) = crate::channel::bounded(1);

			let kvs = kvs.clone();
			let session = session.clone();
			tokio::spawn(async move {
				let export = async {
					if let Err(error) = export_ml(&kvs, &session, tx, config).await {
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

			Ok(DbResponse::Other(Value::None))
		}
		#[cfg(not(target_arch = "wasm32"))]
		Command::ImportFile {
			path,
		} => {
			let mut file = match OpenOptions::new().read(true).open(&path).await {
				Ok(path) => path,
				Err(error) => {
					return Err(Error::FileOpen {
						path,
						error,
					}
					.into());
				}
			};
			let mut statements = String::new();
			if let Err(error) = file.read_to_string(&mut statements).await {
				return Err(Error::FileRead {
					path,
					error,
				}
				.into());
			}

			let responses = kvs.execute(&statements, &*session, Some(vars.clone())).await?;

			for response in responses {
				response.result?;
			}

			Ok(DbResponse::Other(Value::None))
		}
		#[cfg(all(not(target_arch = "wasm32"), feature = "ml"))]
		Command::ImportMl {
			path,
		} => {
			let mut file = match OpenOptions::new().read(true).open(&path).await {
				Ok(path) => path,
				Err(error) => {
					return Err(Error::FileOpen {
						path,
						error,
					}
					.into());
				}
			};

			// Ensure a NS and DB are set
			let (nsv, dbv) = check_ns_db(session)?;
			// Check the permissions level
			kvs.check(session, Action::Edit, ResourceKind::Model.on_db(&nsv, &dbv))?;
			// Create a new buffer
			let mut buffer = Vec::new();
			// Load all the uploaded file chunks
			if let Err(error) = file.read_to_end(&mut buffer).await {
				return Err(Error::FileRead {
					path,
					error,
				}
				.into());
			}
			// Check that the SurrealML file is valid
			let file = match SurMlFile::from_bytes(buffer) {
				Ok(file) => file,
				Err(error) => {
					return Err(Error::FileRead {
						path,
						error: io::Error::new(
							io::ErrorKind::InvalidData,
							error.message.to_string(),
						),
					}
					.into());
				}
			};
			// Convert the file back in to raw bytes
			let data = file.to_bytes();
			// Calculate the hash of the model file
			let hash = crate::obs::hash(&data);
			// Insert the file data in to the store
			crate::obs::put(&hash, data).await?;
			// Insert the model in to the database
			let mut model = DefineModelStatement::default();
			model.name = file.header.name.to_string().into();
			model.version = file.header.version.to_string();
			model.comment = Some(file.header.description.to_string().into());
			model.hash = hash;
			let query = DefineStatement::Model(model).into();
			let responses = kvs.process(query, session, Some(vars.clone())).await?;

			for response in responses {
				response.result?;
			}

			Ok(DbResponse::Other(Value::None))
		}
		Command::Health => Ok(DbResponse::Other(Value::None)),
		Command::Version => Ok(DbResponse::Other(crate::env::VERSION.into())),
		Command::Set {
			key,
			value,
		} => {
			let var = Some(map! {
				key.clone() => Value::None,
				=> vars
			});
			match kvs.compute(value, &*session, var).await? {
				Value::None => vars.remove(&key),
				v => vars.insert(key, v),
			};
			Ok(DbResponse::Other(Value::None))
		}
		Command::Unset {
			key,
		} => {
			vars.remove(&key);
			Ok(DbResponse::Other(Value::None))
		}
		Command::SubscribeLive {
			uuid,
			notification_sender,
		} => {
			live_queries.insert(uuid.into(), notification_sender);
			Ok(DbResponse::Other(Value::None))
		}
		Command::Kill {
			uuid,
		} => {
			live_queries.remove(&uuid.into());
			let value = kill_live_query(kvs, uuid.into(), session, vars.clone()).await?;
			Ok(DbResponse::Other(value))
		}
	}
}
