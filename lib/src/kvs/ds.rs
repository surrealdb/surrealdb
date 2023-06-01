use super::tx::Transaction;
use crate::ctx::Context;
use crate::dbs::cl::Timestamp;
use crate::dbs::Attach;
use crate::dbs::Executor;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Response;
use crate::dbs::Session;
use crate::dbs::Variables;
use crate::err::Error;
use crate::kvs::LOG;
use crate::sql;
use crate::sql::Query;
use crate::sql::Value;
use channel::Receiver;
use channel::Sender;
use futures::lock::Mutex;
use std::fmt;
use std::sync::Arc;
use tracing::callsite::register;
use tracing::instrument;
use uuid::Uuid;

/// The underlying datastore instance which stores the dataset.
#[allow(dead_code)]
pub struct Datastore {
	pub(super) id: Uuid,
	pub(super) inner: Inner,
	pub(super) send: Sender<Notification>,
	pub(super) recv: Receiver<Notification>,
}

#[allow(clippy::large_enum_variant)]
pub(super) enum Inner {
	#[cfg(feature = "kv-mem")]
	Mem(super::mem::Datastore),
	#[cfg(feature = "kv-rocksdb")]
	RocksDB(super::rocksdb::Datastore),
	#[cfg(feature = "kv-indxdb")]
	IndxDB(super::indxdb::Datastore),
	#[cfg(feature = "kv-tikv")]
	TiKV(super::tikv::Datastore),
	#[cfg(feature = "kv-fdb")]
	FDB(super::fdb::Datastore),
}

impl fmt::Display for Datastore {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		#![allow(unused_variables)]
		match &self.inner {
			#[cfg(feature = "kv-mem")]
			Inner::Mem(_) => write!(f, "memory"),
			#[cfg(feature = "kv-rocksdb")]
			Inner::RocksDB(_) => write!(f, "rocksdb"),
			#[cfg(feature = "kv-indxdb")]
			Inner::IndxDB(_) => write!(f, "indexdb"),
			#[cfg(feature = "kv-tikv")]
			Inner::TiKV(_) => write!(f, "tikv"),
			#[cfg(feature = "kv-fdb")]
			Inner::FDB(_) => write!(f, "fdb"),
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}
}

impl Datastore {
	/// Creates a new datastore instance
	///
	/// # Examples
	///
	/// ```rust,no_run
	/// # use surrealdb::kvs::Datastore;
	/// # use surrealdb::err::Error;
	/// # #[tokio::main]
	/// # async fn main() -> Result<(), Error> {
	/// let ds = Datastore::new("memory").await?;
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Or to create a file-backed store:
	///
	/// ```rust,no_run
	/// # use surrealdb::kvs::Datastore;
	/// # use surrealdb::err::Error;
	/// # #[tokio::main]
	/// # async fn main() -> Result<(), Error> {
	/// let ds = Datastore::new("file://temp.db").await?;
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Or to connect to a tikv-backed distributed store:
	///
	/// ```rust,no_run
	/// # use surrealdb::kvs::Datastore;
	/// # use surrealdb::err::Error;
	/// # #[tokio::main]
	/// # async fn main() -> Result<(), Error> {
	/// let ds = Datastore::new("tikv://127.0.0.1:2379").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		// Create a live query notification channel
		let (send, recv) = channel::bounded(100);
		// Initiate the desired datastore
		match path {
			"memory" => {
				#[cfg(feature = "kv-mem")]
				{
					info!(target: LOG, "Starting kvs store in {}", path);
					let ds = Datastore {
						id: Uuid::new_v4(),
						inner: Inner::Mem(super::mem::Datastore::new().await?),
						send,
						recv,
					};
					info!(target: LOG, "Started kvs store at {}", path);
					Ok(ds)
				}
				#[cfg(not(feature = "kv-mem"))]
                return Err(Error::Ds("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate an File database
			s if s.starts_with("file:") => {
				#[cfg(feature = "kv-rocksdb")]
				{
					info!(target: LOG, "Starting kvs store at {}", path);
					let s = s.trim_start_matches("file://");
					let s = s.trim_start_matches("file:");
					let ds = Datastore {
						id: Uuid::new_v4(),
						inner: Inner::RocksDB(super::rocksdb::Datastore::new(s).await?),
						send,
						recv,
					};
					info!(target: LOG, "Started kvs store at {}", path);
					Ok(ds)
				}
				#[cfg(not(feature = "kv-rocksdb"))]
                return Err(Error::Ds("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate an RocksDB database
			s if s.starts_with("rocksdb:") => {
				#[cfg(feature = "kv-rocksdb")]
				{
					info!(target: LOG, "Starting kvs store at {}", path);
					let s = s.trim_start_matches("rocksdb://");
					let s = s.trim_start_matches("rocksdb:");
					let ds = Datastore {
						id: Uuid::new_v4(),
						inner: Inner::RocksDB(super::rocksdb::Datastore::new(s).await?),
						send,
						recv,
					};
					info!(target: LOG, "Started kvs store at {}", path);
					Ok(ds)
				}
				#[cfg(not(feature = "kv-rocksdb"))]
                return Err(Error::Ds("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate an IndxDB database
			s if s.starts_with("indxdb:") => {
				#[cfg(feature = "kv-indxdb")]
				{
					info!(target: LOG, "Starting kvs store at {}", path);
					let s = s.trim_start_matches("indxdb://");
					let s = s.trim_start_matches("indxdb:");
					let ds = Datastore {
						id: Uuid::new_v4(),
						inner: Inner::IndxDB(super::indxdb::Datastore::new(s).await?),
						send,
						recv,
					};
					info!(target: LOG, "Started kvs store at {}", path);
					Ok(ds)
				}
				#[cfg(not(feature = "kv-indxdb"))]
                return Err(Error::Ds("Cannot connect to the `indxdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a TiKV database
			s if s.starts_with("tikv:") => {
				#[cfg(feature = "kv-tikv")]
				{
					info!(target: LOG, "Connecting to kvs store at {}", path);
					let s = s.trim_start_matches("tikv://");
					let s = s.trim_start_matches("tikv:");
					let ds = Datastore {
						id: Uuid::new_v4(),
						inner: Inner::TiKV(super::tikv::Datastore::new(s).await?),
						send,
						recv,
					};
					info!(target: LOG, "Started kvs store at {}", path);
					Ok(ds)
				}
				#[cfg(not(feature = "kv-tikv"))]
                return Err(Error::Ds("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a FoundationDB database
			s if s.starts_with("fdb:") => {
				#[cfg(feature = "kv-fdb")]
				{
					info!(target: LOG, "Connecting to kvs store at {}", path);
					let s = s.trim_start_matches("fdb://");
					let s = s.trim_start_matches("fdb:");
					let ds = Datastore {
						id: Uuid::new_v4(),
						inner: Inner::FDB(super::fdb::Datastore::new(s).await?),
						send,
						recv,
					};
					info!(target: LOG, "Started kvs store at {}", path);
					Ok(ds)
				}
				#[cfg(not(feature = "kv-fdb"))]
                return Err(Error::Ds("Cannot connect to the `foundationdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// The datastore path is not valid
			_ => {
				info!(target: LOG, "Unable to load the specified datastore {}", path);
				Err(Error::Ds("Unable to load the specified datastore".into()))
			}
		}
	}

	/// Creates a new datastore instance
	///
	/// Use this for clustered environments.
	pub async fn new_with_bootstrap(path: &str) -> Result<Datastore, Error> {
		let ds = Datastore::new(path).await?;
		ds.bootstrap().await?;
		Ok(ds)
	}

	// Initialise bootstrap with implicit values intended for runtime
	pub async fn bootstrap(&self) -> Result<(), Error> {
		self.bootstrap_full(&self.id).await
	}

	// Initialise bootstrap with artificial values, intended for testing
	pub async fn bootstrap_full(&self, node_id: &Uuid) -> Result<(), Error> {
		let mut tx = self.transaction(true, false).await?;
		let now = tx.clock();
		let archived = self.register_remove_and_archive(&mut tx, node_id, now).await?;
		tx.commit().await?;

		let mut tx = self.transaction(true, false).await?;
		self.remove_archived_lq(&mut tx, archived).await?;
		Ok(tx.commit().await?)
	}

	// Node registration + "mark" stage of mark-and-sweep gc
	pub async fn register_remove_and_archive(
		&self,
		tx: &mut Transaction,
		node_id: &Uuid,
		timestamp: Timestamp,
	) -> Result<Vec<Uuid>, Error> {
		self.register_membership(tx, node_id, timestamp).await?;
		self.remove_dead_nodes(tx, node_id).await?;
		Ok(self.archive_dead_lqs(tx, node_id).await?)
	}

	// Adds entries to the KV store indicating membership information
	pub async fn register_membership(
		&self,
		tx: &mut Transaction,
		node_id: &Uuid,
		timestamp: Timestamp,
	) -> Result<(), Error> {
		tx.set_cl(sql::Uuid::from(node_id.clone())).await?;
		tx.set_hb(timestamp, sql::Uuid::from(node_id.clone())).await?;
		Ok(())
	}

	pub async fn remove_dead_nodes(
		&self,
		_tx: &mut Transaction,
		_node_id: &Uuid,
	) -> Result<(), Error> {
		Ok(())
	}

	pub async fn archive_dead_lqs(
		&self,
		_tx: &mut Transaction,
		_node_id: &Uuid,
	) -> Result<Vec<Uuid>, Error> {
		Ok(vec![])
	}

	pub async fn remove_archived_lq(
		&self,
		_tx: &mut Transaction,
		_archived: Vec<Uuid>,
	) -> Result<(), Error> {
		Ok(())
	}

	// Creates a heartbeat entry for the member indicating to the cluster
	// that the node is alive.
	pub async fn heartbeat(&self) -> Result<(), Error> {
		let mut tx = self.transaction(true, false).await?;
		let timestamp = tx.clock();
		self.heartbeat_full(&mut tx, timestamp, &self.id).await?;
		Ok(tx.commit().await?)
	}

	// Creates a heartbeat entry for the member indicating to the cluster
	// that the node is alive. Intended for testing.
	pub async fn heartbeat_full(
		&self,
		tx: &mut Transaction,
		timestamp: Timestamp,
		node_id: &Uuid,
	) -> Result<(), Error> {
		Ok(tx.set_hb(timestamp, sql::Uuid::from(node_id.clone())).await?)
	}

	/// Create a new transaction on this datastore
	///
	/// ```rust,no_run
	/// use surrealdb::kvs::Datastore;
	/// use surrealdb::err::Error;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Error> {
	///     let ds = Datastore::new("file://database.db").await?;
	///     let mut tx = ds.transaction(true, false).await?;
	///     tx.cancel().await?;
	///     Ok(())
	/// }
	/// ```
	pub async fn transaction(&self, write: bool, lock: bool) -> Result<Transaction, Error> {
		#![allow(unused_variables)]
		let inner = match &self.inner {
			#[cfg(feature = "kv-mem")]
			Inner::Mem(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::Mem(tx)
			}
			#[cfg(feature = "kv-rocksdb")]
			Inner::RocksDB(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::RocksDB(tx)
			}
			#[cfg(feature = "kv-indxdb")]
			Inner::IndxDB(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::IndxDB(tx)
			}
			#[cfg(feature = "kv-tikv")]
			Inner::TiKV(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::TiKV(tx)
			}
			#[cfg(feature = "kv-fdb")]
			Inner::FDB(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::FDB(tx)
			}
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		};

		#[allow(unreachable_code)]
		Ok(Transaction {
			inner,
			cache: super::cache::Cache::default(),
		})
	}

	/// Parse and execute an SQL query
	///
	/// ```rust,no_run
	/// use surrealdb::kvs::Datastore;
	/// use surrealdb::err::Error;
	/// use surrealdb::dbs::Session;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Error> {
	///     let ds = Datastore::new("memory").await?;
	///     let ses = Session::for_kv();
	///     let ast = "USE NS test DB test; SELECT * FROM person;";
	///     let res = ds.execute(ast, &ses, None, false).await?;
	///     Ok(())
	/// }
	/// ```
	#[instrument(skip_all)]
	pub async fn execute(
		&self,
		txt: &str,
		sess: &Session,
		vars: Variables,
		strict: bool,
	) -> Result<Vec<Response>, Error> {
		// Create a new query options
		let mut opt = Options::new(self.id.clone(), self.send.clone());
		// Create a new query executor
		let mut exe = Executor::new(self);
		// Create a default context
		let ctx = Context::default();
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx)?;
		// Parse the SQL query text
		let ast = sql::parse(txt)?;
		// Setup the notification channel
		opt.sender = self.send.clone();
		// Setup the auth options
		opt.auth = sess.au.clone();
		// Setup the live options
		opt.live = sess.rt;
		// Set current NS and DB
		opt.ns = sess.ns();
		opt.db = sess.db();
		// Set strict config
		opt.strict = strict;
		// Process all statements
		exe.execute(ctx, opt, ast).await
	}

	/// Execute a pre-parsed SQL query
	///
	/// ```rust,no_run
	/// use surrealdb::kvs::Datastore;
	/// use surrealdb::err::Error;
	/// use surrealdb::dbs::Session;
	/// use surrealdb::sql::parse;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Error> {
	///     let ds = Datastore::new("memory").await?;
	///     let ses = Session::for_kv();
	///     let ast = parse("USE NS test DB test; SELECT * FROM person;")?;
	///     let res = ds.process(ast, &ses, None, false).await?;
	///     Ok(())
	/// }
	/// ```
	#[instrument(skip_all)]
	pub async fn process(
		&self,
		ast: Query,
		sess: &Session,
		vars: Variables,
		strict: bool,
	) -> Result<Vec<Response>, Error> {
		// Create a new query options
		let mut opt = Options::new(self.id.clone(), self.send.clone());
		// Create a new query executor
		let mut exe = Executor::new(self);
		// Create a default context
		let ctx = Context::default();
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx)?;
		// Setup the notification channel
		opt.sender = self.send.clone();
		// Setup the auth options
		opt.auth = sess.au.clone();
		// Setup the live options
		opt.live = sess.rt;
		// Set current NS and DB
		opt.ns = sess.ns();
		opt.db = sess.db();
		// Set strict config
		opt.strict = strict;
		// Process all statements
		exe.execute(ctx, opt, ast).await
	}

	/// Ensure a SQL [`Value`] is fully computed
	///
	/// ```rust,no_run
	/// use surrealdb::kvs::Datastore;
	/// use surrealdb::err::Error;
	/// use surrealdb::dbs::Session;
	/// use surrealdb::sql::Future;
	/// use surrealdb::sql::Value;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Error> {
	///     let ds = Datastore::new("memory").await?;
	///     let ses = Session::for_kv();
	///     let val = Value::Future(Box::new(Future::from(Value::Bool(true))));
	///     let res = ds.compute(val, &ses, None, false).await?;
	///     Ok(())
	/// }
	/// ```
	#[instrument(skip_all)]
	pub async fn compute(
		&self,
		val: Value,
		sess: &Session,
		vars: Variables,
		strict: bool,
	) -> Result<Value, Error> {
		// Create a new query options
		let mut opt = Options::new(self.id.clone(), self.send.clone());
		// Start a new transaction
		let txn = self.transaction(val.writeable(), false).await?;
		//
		let txn = Arc::new(Mutex::new(txn));
		// Create a default context
		let ctx = Context::default();
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx)?;
		// Setup the notification channel
		opt.sender = self.send.clone();
		// Setup the auth options
		opt.auth = sess.au.clone();
		// Set current NS and DB
		opt.ns = sess.ns();
		opt.db = sess.db();
		// Set strict config
		opt.strict = strict;
		// Compute the value
		let res = val.compute(&ctx, &opt, &txn, None).await?;
		// Store any data
		match val.writeable() {
			true => txn.lock().await.commit().await?,
			false => txn.lock().await.cancel().await?,
		};
		// Return result
		Ok(res)
	}

	/// Subscribe to live notifications
	///
	/// ```rust,no_run
	/// use surrealdb::kvs::Datastore;
	/// use surrealdb::err::Error;
	/// use surrealdb::dbs::Session;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Error> {
	///     let ds = Datastore::new("memory").await?;
	///     let ses = Session::for_kv();
	///     while let Ok(v) = ds.notifications().recv().await {
	///         println!("Received notification: {v}");
	///     }
	///     Ok(())
	/// }
	/// ```
	#[instrument(skip_all)]
	pub fn notifications(&self) -> Receiver<Notification> {
		self.recv.clone()
	}

	/// Performs a full database export as SQL
	#[instrument(skip(self, chn))]
	pub async fn export(&self, ns: String, db: String, chn: Sender<Vec<u8>>) -> Result<(), Error> {
		// Start a new transaction
		let mut txn = self.transaction(false, false).await?;
		// Process the export
		txn.export(&ns, &db, chn).await?;
		// Everything ok
		Ok(())
	}
}
