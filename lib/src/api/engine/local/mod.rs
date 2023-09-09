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
use crate::api::conn::Param;
use crate::api::engine::create_statement;
use crate::api::engine::delete_statement;
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
#[cfg(not(target_arch = "wasm32"))]
use crate::channel;
use crate::dbs::Response;
use crate::dbs::Session;
use crate::kvs::Datastore;
use crate::opt::IntoEndpoint;
use crate::sql::Array;
use crate::sql::Query;
use crate::sql::Statement;
use crate::sql::Statements;
use crate::sql::Strand;
use crate::sql::Value;
use indexmap::IndexMap;
use std::collections::BTreeMap;
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
/// let db = Surreal::new::<File>("temp.db").await?;
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
/// let db = Surreal::new::<File>(("temp.db", config)).await?;
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
/// let db = Surreal::new::<RocksDb>("temp.db").await?;
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
/// let db = Surreal::new::<RocksDb>(("temp.db", config)).await?;
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
/// let db = Surreal::new::<SpeeDb>("temp.db").await?;
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
/// let db = Surreal::new::<SpeeDb>(("temp.db", config)).await?;
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
/// let db = Surreal::new::<IndxDb>("MyDatabase").await?;
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
/// let db = Surreal::new::<IndxDb>(("MyDatabase", config)).await?;
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
/// let db = Surreal::new::<FDb>("fdb.cluster").await?;
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
/// let db = Surreal::new::<FDb>(("fdb.cluster", config)).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-fdb")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-fdb")))]
#[derive(Debug)]
pub struct FDb;

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
			address: address.into_endpoint(),
			capacity: 0,
			client: PhantomData,
			response_type: PhantomData,
		}
	}
}

fn process(responses: Vec<Response>) -> Result<QueryResponse> {
	let mut map = IndexMap::with_capacity(responses.len());
	for (index, response) in responses.into_iter().enumerate() {
		match response.result {
			Ok(value) => match value {
				Value::Array(Array(array)) => map.insert(index, Ok(array)),
				Value::None | Value::Null => map.insert(index, Ok(vec![])),
				value => map.insert(index, Ok(vec![value])),
			},
			Err(error) => map.insert(index, Err(error.into())),
		};
	}
	Ok(QueryResponse(map))
}

async fn take(one: bool, responses: Vec<Response>) -> Result<Value> {
	if let Some(result) = process(responses)?.0.remove(&0) {
		let mut vec = result?;
		match one {
			true => match vec.pop() {
				Some(Value::Array(Array(mut vec))) => {
					if let [value] = &mut vec[..] {
						return Ok(mem::take(value));
					}
				}
				Some(Value::None | Value::Null) | None => {}
				Some(value) => {
					return Ok(value);
				}
			},
			false => {
				return Ok(Value::Array(Array(vec)));
			}
		}
	}
	match one {
		true => Ok(Value::None),
		false => Ok(Value::Array(Array(vec![]))),
	}
}

#[cfg(not(target_arch = "wasm32"))]
async fn export(
	kvs: &Datastore,
	sess: &Session,
	ns: String,
	db: String,
	chn: channel::Sender<Vec<u8>>,
) -> Result<()> {
	if let Err(error) = kvs.export(sess, ns, db, chn).await?.await {
		if let crate::error::Db::Channel(message) = error {
			// This is not really an error. Just logging it for improved visibility.
			trace!("{message}");
			return Ok(());
		}
		return Err(error.into());
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

async fn router(
	(_, method, param): (i64, Method, Param),
	kvs: &Arc<Datastore>,
	session: &mut Session,
	vars: &mut BTreeMap<String, Value>,
) -> Result<DbResponse> {
	let mut params = param.other;

	match method {
		Method::Use => {
			match &mut params[..] {
				[Value::Strand(Strand(ns)), Value::Strand(Strand(db))] => {
					session.ns = Some(mem::take(ns));
					session.db = Some(mem::take(db));
				}
				[Value::Strand(Strand(ns)), Value::None] => {
					session.ns = Some(mem::take(ns));
				}
				[Value::None, Value::Strand(Strand(db))] => {
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
				[Value::Strand(Strand(token))] => mem::take(token),
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
			let statement = create_statement(&mut params);
			let query = Query(Statements(vec![Statement::Create(statement)]));
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(true, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Update => {
			let (one, statement) = update_statement(&mut params);
			let query = Query(Statements(vec![Statement::Update(statement)]));
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Patch => {
			let (one, statement) = patch_statement(&mut params);
			let query = Query(Statements(vec![Statement::Update(statement)]));
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Merge => {
			let (one, statement) = merge_statement(&mut params);
			let query = Query(Statements(vec![Statement::Update(statement)]));
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Select => {
			let (one, statement) = select_statement(&mut params);
			let query = Query(Statements(vec![Statement::Select(statement)]));
			let response = kvs.process(query, &*session, Some(vars.clone())).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Delete => {
			let (one, statement) = delete_statement(&mut params);
			let query = Query(Statements(vec![Statement::Delete(statement)]));
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
			let response = process(response)?;
			Ok(DbResponse::Query(response))
		}
		#[cfg(target_arch = "wasm32")]
		Method::Export | Method::Import => unreachable!(),
		#[cfg(not(target_arch = "wasm32"))]
		Method::Export => {
			let ns = session.ns.clone().unwrap_or_default();
			let db = session.db.clone().unwrap_or_default();
			let (tx, rx) = channel::new(1);

			match (param.file, param.sender) {
				(Some(path), None) => {
					let (mut writer, mut reader) = io::duplex(10_240);

					// Write to channel.
					let export = export(kvs, session, ns, db, tx);

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
							if let Err(error) = export(&kvs, &session, ns, db, tx).await {
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
		Method::Health => Ok(DbResponse::Other(Value::None)),
		Method::Version => Ok(DbResponse::Other(crate::env::VERSION.into())),
		Method::Set => {
			let (key, value) = match &mut params[..2] {
				[Value::Strand(Strand(key)), value] => (mem::take(key), mem::take(value)),
				_ => unreachable!(),
			};
			match kvs.compute(value, &*session, Some(vars.clone())).await? {
				Value::None => vars.remove(&key),
				v => vars.insert(key, v),
			};
			Ok(DbResponse::Other(Value::None))
		}
		Method::Unset => {
			if let [Value::Strand(Strand(key))] = &params[..1] {
				vars.remove(key);
			}
			Ok(DbResponse::Other(Value::None))
		}
		Method::Live => {
			let table = match &mut params[..] {
				[value] => mem::take(value),
				_ => unreachable!(),
			};
			let mut vars = BTreeMap::new();
			vars.insert("table".to_owned(), table);
			let response = kvs
				.execute("LIVE SELECT * FROM type::table($table)", &*session, Some(vars))
				.await?;
			let value = take(true, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Kill => {
			let id = match &mut params[..] {
				[value] => mem::take(value),
				_ => unreachable!(),
			};
			let mut vars = BTreeMap::new();
			vars.insert("id".to_owned(), id);
			let response = kvs.execute("KILL type::string($id)", &*session, Some(vars)).await?;
			let value = take(true, response).await?;
			Ok(DbResponse::Other(value))
		}
	}
}
