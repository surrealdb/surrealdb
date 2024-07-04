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
use crate::{
	api::{
		conn::{DbResponse, Method, Param},
		engine::{
			create_statement, delete_statement, insert_statement, merge_statement, patch_statement,
			select_statement, update_statement, upsert_statement,
		},
		Connect, Response as QueryResponse, Result, Surreal,
	},
	method::Stats,
	opt::IntoEndpoint,
	value::ToCore,
	Value,
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
	sql::{statements::KillStatement, Query, Statement, Value as CoreValue},
};
use uuid::Uuid;

#[cfg(not(target_arch = "wasm32"))]
use crate::api::{conn::MlConfig, err::Error};
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

#[cfg(feature = "ml")]
#[cfg(not(target_arch = "wasm32"))]
use futures::StreamExt;
#[cfg(feature = "ml")]
#[cfg(not(target_arch = "wasm32"))]
use surrealdb_core::{
	iam::{check::check_ns_db, Action, ResourceKind},
	kvs::{LockType, TransactionType},
	ml::storage::surml_file::SurMlFile,
	sql::statements::{DefineModelStatement, DefineStatement},
};
#[cfg(not(target_arch = "wasm32"))]
use tokio::{
	fs::OpenOptions,
	io::{self, AsyncReadExt, AsyncWriteExt},
};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
#[cfg(target_arch = "wasm32")]
pub(crate) mod wasm;

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

/// An embedded database
#[derive(Debug, Clone)]
pub struct Db {
	pub(crate) method: crate::api::conn::Method,
}

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
	let mut map = IndexMap::<usize, (Stats, Result<Value>)>::with_capacity(responses.len());
	for (index, response) in responses.into_iter().enumerate() {
		let stats = Stats {
			execution_time: Some(response.time),
		};
		match response.result {
			Ok(value) => {
				let v: CoreValue = value;
				// Deserializing from a core value should always work.
				let v = Value::from_core(v)
					.ok_or(crate::Error::Api(crate::api::Error::RecievedInvalidValue));
				map.insert(index, (stats, v));
			}
			Err(error) => {
				map.insert(index, (stats, Err(error.into())));
			}
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
					if let [ref mut value] = array[..] {
						return Ok(mem::replace(value, Value::None));
					}
				}
				Value::None => {}
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
async fn export(
	kvs: &Datastore,
	sess: &Session,
	chn: channel::Sender<Vec<u8>>,
	ml_config: Option<MlConfig>,
) -> Result<()> {
	match ml_config {
		#[cfg(feature = "ml")]
		Some(MlConfig::Export {
			name,
			version,
		}) => {
			// Ensure a NS and DB are set
			let (nsv, dbv) = check_ns_db(sess)?;
			// Check the permissions level
			kvs.check(sess, Action::View, ResourceKind::Model.on_db(&nsv, &dbv))?;
			// Start a new readonly transaction
			let mut tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
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
		}
		_ => {
			if let Err(error) = kvs.export(sess, chn).await?.await {
				if let crate::error::Db::Channel(message) = error {
					// This is not really an error. Just logging it for improved visibility.
					trace!("{message}");
					return Ok(());
				}
				return Err(error.into());
			}
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
	let vars = vars.into_iter().map(|(k, v)| (k, v.to_core())).collect();
	let response = kvs.process(query, session, Some(vars)).await?;
	take(true, response).await
}

async fn router(
	(_, method, param): (i64, Method, Param),
	kvs: &Arc<Datastore>,
	session: &mut Session,
	vars: &mut BTreeMap<String, Value>,
	live_queries: &mut HashMap<Uuid, Sender<Notification>>,
) -> Result<DbResponse> {
	let mut params = param.other;

	match method {
		Method::Use => {
			match &mut params[..] {
				[Value::String(ref mut ns), Value::String(ref mut db)] => {
					session.ns = Some(mem::take(ns));
					session.db = Some(mem::take(db));
				}
				[Value::String(ref mut ns), Value::None] => {
					session.ns = Some(mem::take(ns));
				}
				[Value::None, Value::String(ref mut db)] => {
					session.db = Some(mem::take(db));
				}
				_ => unreachable!(),
			}
			Ok(DbResponse::Other(Value::None))
		}
		Method::Signup => {
			let credentials = match &mut params[..] {
				[Value::Object(credentials)] => mem::take(credentials),
				_ => unreachable!(),
			};
			let response = crate::iam::signup::signup(kvs, session, credentials.to_core()).await?;
			Ok(DbResponse::Other(response.into()))
		}
		Method::Signin => {
			let credentials = match &mut params[..] {
				[Value::Object(credentials)] => mem::take(credentials),
				_ => unreachable!(),
			};
			let response = crate::iam::signin::signin(kvs, session, credentials.to_core()).await?;
			Ok(DbResponse::Other(response.into()))
		}
		Method::Authenticate => {
			let token = match &mut params[..] {
				[Value::String(token)] => mem::take(token),
				_ => unreachable!(),
			};
			crate::iam::verify::token(kvs, session, &token).await?;
			Ok(DbResponse::Other(Value::None))
		}
		Method::Invalidate => {
			crate::iam::clear::clear(session)?;
			Ok(DbResponse::Other(Value::None))
		}
		Method::Create => {
			let mut query = Query::default();
			let statement = create_statement(&mut params);
			query.0 .0 = vec![Statement::Create(statement)];
			let vars = vars.iter().map(|(k, v)| (k.clone(), v.clone().to_core())).collect();
			let response = kvs.process(query, &*session, Some(vars)).await?;
			let value = take(true, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Upsert => {
			let mut query = Query::default();
			let (one, statement) = upsert_statement(&mut params);
			query.0 .0 = vec![Statement::Upsert(statement)];
			let vars = vars.iter().map(|(k, v)| (k.clone(), v.clone().to_core())).collect();
			let response = kvs.process(query, &*session, Some(vars)).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Update => {
			let mut query = Query::default();
			let (one, statement) = update_statement(&mut params);
			query.0 .0 = vec![Statement::Update(statement)];
			let vars = vars.iter().map(|(k, v)| (k.clone(), v.clone().to_core())).collect();
			let response = kvs.process(query, &*session, Some(vars)).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Insert => {
			let mut query = Query::default();
			let (one, statement) = insert_statement(&mut params);
			query.0 .0 = vec![Statement::Insert(statement)];
			let vars = vars.iter().map(|(k, v)| (k.clone(), v.clone().to_core())).collect();
			let response = kvs.process(query, &*session, Some(vars)).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Patch => {
			let mut query = Query::default();
			let (one, statement) = patch_statement(&mut params);
			query.0 .0 = vec![Statement::Update(statement)];
			let vars = vars.iter().map(|(k, v)| (k.clone(), v.clone().to_core())).collect();
			let response = kvs.process(query, &*session, Some(vars)).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Merge => {
			let mut query = Query::default();
			let (one, statement) = merge_statement(&mut params);
			query.0 .0 = vec![Statement::Update(statement)];
			let vars = vars.iter().map(|(k, v)| (k.clone(), v.clone().to_core())).collect();
			let response = kvs.process(query, &*session, Some(vars)).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Select => {
			let mut query = Query::default();
			let (one, statement) = select_statement(&mut params);
			query.0 .0 = vec![Statement::Select(statement)];
			let vars = vars.iter().map(|(k, v)| (k.clone(), v.clone().to_core())).collect();
			let response = kvs.process(query, &*session, Some(vars)).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Delete => {
			let mut query = Query::default();
			let (one, statement) = delete_statement(&mut params);
			query.0 .0 = vec![Statement::Delete(statement)];
			let vars = vars.iter().map(|(k, v)| (k.clone(), v.clone().to_core())).collect();
			let response = kvs.process(query, &*session, Some(vars)).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Query => {
			let response = match param.query {
				Some((query, bindings)) => {
					let mut vars = vars
						.iter()
						.map(|(k, v)| (k.clone(), v.clone().to_core()))
						.collect::<BTreeMap<_, _>>();
					vars.append(&mut bindings.clone().to_core().0);
					kvs.process(query, &*session, Some(vars)).await?
				}
				None => unreachable!(),
			};
			let response = process(response);
			Ok(DbResponse::Query(response))
		}
		#[cfg(target_arch = "wasm32")]
		Method::Export | Method::Import => unreachable!(),
		#[cfg(not(target_arch = "wasm32"))]
		Method::Export => {
			let (tx, rx) = crate::channel::bounded(1);

			match (param.file, param.bytes_sender) {
				(Some(path), None) => {
					let (mut writer, mut reader) = io::duplex(10_240);

					// Write to channel.
					let export = export(kvs, session, tx, param.ml_config);

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
				}
				(None, Some(backup)) => {
					let kvs = kvs.clone();
					let session = session.clone();
					tokio::spawn(async move {
						let export = async {
							if let Err(error) = export(&kvs, &session, tx, param.ml_config).await {
								let _ = backup.send(Err(error)).await;
							}
						};

						let bridge = async {
							while let Ok(bytes) = rx.recv().await {
								if backup.send(Ok(bytes)).await.is_err() {
									break;
								}
							}
						};

						tokio::join!(export, bridge);
					});
				}
				_ => unreachable!(),
			}

			Ok(DbResponse::Other(Value::None))
		}
		#[cfg(not(target_arch = "wasm32"))]
		Method::Import => {
			let path = param.file.expect("file to import from");
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
			let responses = match param.ml_config {
				#[cfg(feature = "ml")]
				Some(MlConfig::Import) => {
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
					kvs.process(query, session, Some(vars.clone())).await?
				}
				_ => {
					let mut statements = String::new();
					if let Err(error) = file.read_to_string(&mut statements).await {
						return Err(Error::FileRead {
							path,
							error,
						}
						.into());
					}
					let vars = vars.iter().map(|(k, v)| (k.clone(), v.clone().to_core())).collect();
					kvs.execute(&statements, &*session, Some(vars)).await?
				}
			};
			for response in responses {
				response.result?;
			}
			Ok(DbResponse::Other(Value::None))
		}
		Method::Health => Ok(DbResponse::Other(Value::None)),
		Method::Version => Ok(DbResponse::Other(crate::env::VERSION.to_string().into())),
		Method::Set => {
			let (key, value) = match &mut params[..2] {
				[Value::String(key), value] => (mem::take(key), mem::take(value)),
				_ => unreachable!(),
			};

			let mut new_vars: BTreeMap<String, CoreValue> =
				vars.iter().map(|(k, v)| (k.clone(), v.clone().to_core())).collect();

			new_vars.insert(key.clone(), CoreValue::None);

			match kvs.compute(value.to_core(), &*session, Some(new_vars)).await? {
				CoreValue::None => {
					vars.remove(&key);
				}
				v => {
					let v = ToCore::from_core(v)
						.ok_or(crate::Error::Api(crate::api::Error::RecievedInvalidValue))?;
					vars.insert(key, v);
				}
			};
			Ok(DbResponse::Other(Value::None))
		}
		Method::Unset => {
			if let [Value::String(key)] = &params[..1] {
				vars.remove(key);
			}
			Ok(DbResponse::Other(Value::None))
		}
		Method::Live => {
			if let Some(sender) = param.notification_sender {
				if let [Value::Uuid(id)] = &params[..1] {
					live_queries.insert(*id, sender);
				}
			}
			Ok(DbResponse::Other(Value::None))
		}
		Method::Kill => {
			let id = match &params[..] {
				[Value::Uuid(id)] => *id,
				_ => unreachable!(),
			};
			live_queries.remove(&id);
			let value = kill_live_query(kvs, id, session, vars.clone()).await?;
			Ok(DbResponse::Other(value))
		}
	}
}
