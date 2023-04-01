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
use tokio::fs::OpenOptions;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io::AsyncReadExt;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io::AsyncWrite;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io::AsyncWriteExt;

const LOG: &str = "surrealdb::api::engine::local";

/// In-memory database
///
/// # Examples
///
/// Instantiating a global instance
///
/// ```
/// use surrealdb::{Result, Surreal};
/// use surrealdb::engine::local::Db;
/// use surrealdb::engine::local::Mem;
///
/// static DB: Surreal<Db> = Surreal::init();
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
/// use surrealdb::opt::Strict;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::Mem;
///
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let db = Surreal::new::<Mem>(Strict).await?;
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
/// use surrealdb::opt::Strict;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::File;
///
/// let db = Surreal::new::<File>(("temp.db", Strict)).await?;
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
/// use surrealdb::opt::Strict;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::RocksDb;
///
/// let db = Surreal::new::<RocksDb>(("temp.db", Strict)).await?;
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
/// use surrealdb::opt::Strict;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::IndxDb;
///
/// let db = Surreal::new::<IndxDb>(("MyDatabase", Strict)).await?;
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
/// use surrealdb::opt::Strict;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::TiKv;
///
/// let db = Surreal::new::<TiKv>(("localhost:2379", Strict)).await?;
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
/// use surrealdb::opt::Strict;
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
/// use surrealdb::opt::Strict;
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::FDb;
///
/// let db = Surreal::new::<FDb>(("fdb.cluster", Strict)).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-fdb")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-fdb")))]
#[derive(Debug)]
pub struct FDb;

/// An embedded database
///
/// Authentication methods (`signup`, `signin`, `authentication` and `invalidate`) are not availabe
/// on `Db`
#[derive(Debug, Clone)]
pub struct Db {
	pub(crate) method: crate::api::conn::Method,
}

impl Surreal<Db> {
	/// Connects to a specific database endpoint, saving the connection on the static client
	pub fn connect<P>(
		&'static self,
		address: impl IntoEndpoint<P, Client = Db>,
	) -> Connect<Db, ()> {
		Connect {
			router: Some(&self.router),
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

async fn router(
	(_, method, param): (i64, Method, Param),
	kvs: &Datastore,
	session: &mut Session,
	vars: &mut BTreeMap<String, Value>,
	strict: bool,
) -> Result<DbResponse> {
	let mut params = param.other;

	match method {
		Method::Use => {
			let (ns, db) = match &mut params[..] {
				[Value::Strand(Strand(ns)), Value::Strand(Strand(db))] => {
					(mem::take(ns), mem::take(db))
				}
				_ => unreachable!(),
			};
			session.ns = Some(ns);
			session.db = Some(db);
			Ok(DbResponse::Other(Value::None))
		}
		Method::Signin | Method::Signup | Method::Authenticate | Method::Invalidate => {
			unreachable!()
		}
		Method::Create => {
			let statement = create_statement(&mut params);
			let query = Query(Statements(vec![Statement::Create(statement)]));
			let response = kvs.process(query, &*session, Some(vars.clone()), strict).await?;
			let value = take(true, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Update => {
			let (one, statement) = update_statement(&mut params);
			let query = Query(Statements(vec![Statement::Update(statement)]));
			let response = kvs.process(query, &*session, Some(vars.clone()), strict).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Patch => {
			let (one, statement) = patch_statement(&mut params);
			let query = Query(Statements(vec![Statement::Update(statement)]));
			let response = kvs.process(query, &*session, Some(vars.clone()), strict).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Merge => {
			let (one, statement) = merge_statement(&mut params);
			let query = Query(Statements(vec![Statement::Update(statement)]));
			let response = kvs.process(query, &*session, Some(vars.clone()), strict).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Select => {
			let (one, statement) = select_statement(&mut params);
			let query = Query(Statements(vec![Statement::Select(statement)]));
			let response = kvs.process(query, &*session, Some(vars.clone()), strict).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Delete => {
			let (one, statement) = delete_statement(&mut params);
			let query = Query(Statements(vec![Statement::Delete(statement)]));
			let response = kvs.process(query, &*session, Some(vars.clone()), strict).await?;
			let value = take(one, response).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Query => {
			let response = match param.query {
				Some((query, mut bindings)) => {
					let mut vars = vars.clone();
					vars.append(&mut bindings);
					kvs.process(query, &*session, Some(vars), strict).await?
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
			let (tx, rx) = channel::new::<Vec<u8>>(1);
			let ns = session.ns.clone().unwrap_or_default();
			let db = session.db.clone().unwrap_or_default();
			let (mut writer, mut reader) = io::duplex(10_240);
			tokio::spawn(async move {
				while let Ok(value) = rx.recv().await {
					if let Err(error) = writer.write_all(&value).await {
						error!(target: LOG, "{error}");
					}
				}
			});
			if let Err(error) = kvs.export(ns, db, tx).await {
				error!(target: LOG, "{error}");
			}
			let path = param.file.expect("file to export into");
			let mut writer: Box<dyn AsyncWrite + Unpin + Send> = match path.to_str().unwrap() {
				"-" => Box::new(io::stdout()),
				_ => {
					let file = match OpenOptions::new()
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
					Box::new(file)
				}
			};
			if let Err(error) = io::copy(&mut reader, &mut writer).await {
				return Err(Error::FileRead {
					path,
					error,
				}
				.into());
			};
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
			let responses = kvs.execute(&statements, &*session, Some(vars.clone()), strict).await?;
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
			vars.insert(key, value);
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
				.execute("LIVE SELECT * FROM type::table($table)", &*session, Some(vars), strict)
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
			let response =
				kvs.execute("KILL type::string($id)", &*session, Some(vars), strict).await?;
			let value = take(true, response).await?;
			Ok(DbResponse::Other(value))
		}
	}
}
