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

use crate::api::conn::DbResponse;
use crate::api::conn::Method;
#[cfg(not(target_arch = "wasm32"))]
use crate::api::conn::MlConfig;
use crate::api::conn::Param;
use crate::api::engine::create_statement;
use crate::api::engine::delete_statement;
use crate::api::engine::insert_statement;
use crate::api::engine::merge_statement;
use crate::api::engine::patch_statement;
use crate::api::engine::select_statement;
use crate::api::engine::update_statement;
#[cfg(not(target_arch = "wasm32"))]
use crate::api::err::Error;
use crate::api::Connect;
use crate::api::Response as QueryResponse;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Notification;
use crate::dbs::Response;
use crate::dbs::Session;
#[cfg(feature = "ml")]
#[cfg(not(target_arch = "wasm32"))]
use crate::iam::check::check_ns_db;
#[cfg(feature = "ml")]
#[cfg(not(target_arch = "wasm32"))]
use crate::iam::Action;
#[cfg(feature = "ml")]
#[cfg(not(target_arch = "wasm32"))]
use crate::iam::ResourceKind;
use crate::kvs::Datastore;
#[cfg(feature = "ml")]
#[cfg(not(target_arch = "wasm32"))]
use crate::kvs::{LockType, TransactionType};
use crate::method::Stats;
#[cfg(feature = "ml")]
#[cfg(not(target_arch = "wasm32"))]
use crate::ml::storage::surml_file::SurMlFile;
use crate::opt::IntoEndpoint;
#[cfg(feature = "ml")]
#[cfg(not(target_arch = "wasm32"))]
use crate::sql::statements::DefineModelStatement;
#[cfg(feature = "ml")]
#[cfg(not(target_arch = "wasm32"))]
use crate::sql::statements::DefineStatement;
use crate::sql::statements::KillStatement;
use crate::sql::Query;
use crate::sql::Statement;
use crate::sql::Uuid;
use crate::sql::Value;
use channel::Sender;
#[cfg(feature = "ml")]
#[cfg(not(target_arch = "wasm32"))]
use futures::StreamExt;
use indexmap::IndexMap;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use tokio::fs::OpenOptions;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io::AsyncReadExt;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io::AsyncWriteExt;

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

/// SpeeDB database
///
/// # Examples
///
/// Instantiating a SpeeDB-backed instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::SpeeDb;
///
/// let db = Surreal::new::<SpeeDb>("path/to/database-folder").await?;
/// # Ok(())
/// # }
/// ```
///
/// Instantiating a SpeeDB-backed strict instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::opt::Config;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::SpeeDb;
///
/// let config = Config::default().strict();
/// let db = Surreal::new::<SpeeDb>(("path/to/database-folder", config)).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-speedb")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-speedb")))]
#[derive(Debug)]
pub struct SpeeDb;

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
			client: PhantomData,
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
				[Value::Strand(ns), Value::Strand(db)] => {
					session.ns = Some(mem::take(&mut ns.0));
					session.db = Some(mem::take(&mut db.0));
				}
				[Value::Strand(ns), Value::None] => {
					session.ns = Some(mem::take(&mut ns.0));
				}
				[Value::None, Value::Strand(db)] => {
					session.db = Some(mem::take(&mut db.0));
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
			let response = crate::iam::signup::signup(kvs, session, credentials).await?;
			Ok(DbResponse::Other(response.into()))
		}
		Method::Signin => {
			let credentials = match &mut params[..] {
				[Value::Object(credentials)] => mem::take(credentials),
				_ => unreachable!(),
			};
			let response = crate::iam::signin::signin(kvs, session, credentials).await?;
			Ok(DbResponse::Other(response.into()))
		}
		Method::Authenticate => {
			let token = match &mut params[..] {
				[Value::Strand(token)] => mem::take(&mut token.0),
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
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(true, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Update => {
			let mut query = Query::default();
			let (one, statement) = update_statement(&mut params);
			query.0 .0 = vec![Statement::Update(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Insert => {
			let mut query = Query::default();
			let (one, statement) = insert_statement(&mut params);
			query.0 .0 = vec![Statement::Insert(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Patch => {
			let mut query = Query::default();
			let (one, statement) = patch_statement(&mut params);
			query.0 .0 = vec![Statement::Update(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Merge => {
			let mut query = Query::default();
			let (one, statement) = merge_statement(&mut params);
			query.0 .0 = vec![Statement::Update(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Select => {
			let mut query = Query::default();
			let (one, statement) = select_statement(&mut params);
			query.0 .0 = vec![Statement::Select(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Delete => {
			let mut query = Query::default();
			let (one, statement) = delete_statement(&mut params);
			query.0 .0 = vec![Statement::Delete(statement)];
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Query => {
			let response = match param.query {
				Some((query, mut bindings)) => {
					let mut vars = vars.clone();
					vars.append(&mut bindings);
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
					kvs.execute(&statements, &*session, Some(vars.clone())).await?
				}
			};
			for response in responses {
				response.result?;
			}
			Ok(DbResponse::Other(Value::None))
		}
		Method::Health => Ok(DbResponse::Other(Value::None)),
		Method::Version => Ok(DbResponse::Other(crate::env::VERSION.into())),
		Method::Set => {
			let (key, value) = match &mut params[..2] {
				[Value::Strand(key), value] => (mem::take(&mut key.0), mem::take(value)),
				_ => unreachable!(),
			};
			let var = Some(crate::map! {
				key.clone() => Value::None,
				=> vars
			});
			match kvs.compute(value, &*session, var).await? {
				Value::None => vars.remove(&key),
				v => vars.insert(key, v),
			};
			Ok(DbResponse::Other(Value::None))
		}
		Method::Unset => {
			if let [Value::Strand(key)] = &params[..1] {
				vars.remove(&key.0);
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
