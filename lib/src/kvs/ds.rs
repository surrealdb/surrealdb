use super::tx::Transaction;
use crate::ctx::Context;
use crate::dbs::Attach;
use crate::dbs::Executor;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Response;
use crate::dbs::Session;
use crate::dbs::Variables;
use crate::err::Error;
use crate::sql;
use crate::sql::Query;
use crate::sql::Value;
use channel::Receiver;
use channel::Sender;
use futures::lock::Mutex;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument;
use uuid::Uuid;

/// The underlying datastore instance which stores the dataset.
#[allow(dead_code)]
pub struct Datastore {
	// The inner datastore type
	inner: Inner,
	// The unique id of this datastore, used in notifications
	id: Uuid,
	// Whether this datastore runs in strict mode by default
	strict: bool,
	// The maximum duration timeout for running multiple statements in a query
	query_timeout: Option<Duration>,
	// The maximum duration timeout for running multiple statements in a transaction
	transaction_timeout: Option<Duration>,
	// Whether this datastore enables live query notifications to subscribers
	notification_channel: Option<(Sender<Notification>, Receiver<Notification>)>,
}

#[allow(clippy::large_enum_variant)]
pub(super) enum Inner {
	#[cfg(feature = "kv-mem")]
	Mem(super::mem::Datastore),
	#[cfg(feature = "kv-rocksdb")]
	RocksDB(super::rocksdb::Datastore),
	#[cfg(feature = "kv-speedb")]
	SpeeDB(super::speedb::Datastore),
	#[cfg(feature = "kv-indxdb")]
	IndxDB(super::indxdb::Datastore),
	#[cfg(feature = "kv-tikv")]
	TiKV(super::tikv::Datastore),
	#[cfg(feature = "kv-fdb")]
	FoundationDB(super::fdb::Datastore),
}

impl fmt::Display for Datastore {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		#![allow(unused_variables)]
		match &self.inner {
			#[cfg(feature = "kv-mem")]
			Inner::Mem(_) => write!(f, "memory"),
			#[cfg(feature = "kv-rocksdb")]
			Inner::RocksDB(_) => write!(f, "rocksdb"),
			#[cfg(feature = "kv-speedb")]
			Inner::SpeeDB(_) => write!(f, "speedb"),
			#[cfg(feature = "kv-indxdb")]
			Inner::IndxDB(_) => write!(f, "indxdb"),
			#[cfg(feature = "kv-tikv")]
			Inner::TiKV(_) => write!(f, "tikv"),
			#[cfg(feature = "kv-fdb")]
			Inner::FoundationDB(_) => write!(f, "fdb"),
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
		let inner = match path {
			"memory" => {
				#[cfg(feature = "kv-mem")]
				{
					info!("Starting kvs store in {}", path);
					let v = super::mem::Datastore::new().await.map(Inner::Mem);
					info!("Started kvs store in {}", path);
					v
				}
				#[cfg(not(feature = "kv-mem"))]
				return Err(Error::Ds("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate an File database
			s if s.starts_with("file:") => {
				#[cfg(feature = "kv-rocksdb")]
				{
					info!("Starting kvs store at {}", path);
					let s = s.trim_start_matches("file://");
					let s = s.trim_start_matches("file:");
					let v = super::rocksdb::Datastore::new(s).await.map(Inner::RocksDB);
					info!("Started kvs store at {}", path);
					v
				}
				#[cfg(not(feature = "kv-rocksdb"))]
				return Err(Error::Ds("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate an RocksDB database
			s if s.starts_with("rocksdb:") => {
				#[cfg(feature = "kv-rocksdb")]
				{
					info!("Starting kvs store at {}", path);
					let s = s.trim_start_matches("rocksdb://");
					let s = s.trim_start_matches("rocksdb:");
					let v = super::rocksdb::Datastore::new(s).await.map(Inner::RocksDB);
					info!("Started kvs store at {}", path);
					v
				}
				#[cfg(not(feature = "kv-rocksdb"))]
				return Err(Error::Ds("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate an SpeeDB database
			s if s.starts_with("speedb:") => {
				#[cfg(feature = "kv-speedb")]
				{
					info!("Starting kvs store at {}", path);
					let s = s.trim_start_matches("speedb://");
					let s = s.trim_start_matches("speedb:");
					let v = super::speedb::Datastore::new(s).await.map(Inner::SpeeDB);
					info!("Started kvs store at {}", path);
					v
				}
				#[cfg(not(feature = "kv-speedb"))]
				return Err(Error::Ds("Cannot connect to the `speedb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate an IndxDB database
			s if s.starts_with("indxdb:") => {
				#[cfg(feature = "kv-indxdb")]
				{
					info!("Starting kvs store at {}", path);
					let s = s.trim_start_matches("indxdb://");
					let s = s.trim_start_matches("indxdb:");
					let v = super::indxdb::Datastore::new(s).await.map(Inner::IndxDB);
					info!("Started kvs store at {}", path);
					v
				}
				#[cfg(not(feature = "kv-indxdb"))]
				return Err(Error::Ds("Cannot connect to the `indxdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a TiKV database
			s if s.starts_with("tikv:") => {
				#[cfg(feature = "kv-tikv")]
				{
					info!("Connecting to kvs store at {}", path);
					let s = s.trim_start_matches("tikv://");
					let s = s.trim_start_matches("tikv:");
					let v = super::tikv::Datastore::new(s).await.map(Inner::TiKV);
					info!("Connected to kvs store at {}", path);
					v
				}
				#[cfg(not(feature = "kv-tikv"))]
				return Err(Error::Ds("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a FoundationDB database
			s if s.starts_with("fdb:") => {
				#[cfg(feature = "kv-fdb")]
				{
					info!("Connecting to kvs store at {}", path);
					let s = s.trim_start_matches("fdb://");
					let s = s.trim_start_matches("fdb:");
					let v = super::fdb::Datastore::new(s).await.map(Inner::FoundationDB);
					info!("Connected to kvs store at {}", path);
					v
				}
				#[cfg(not(feature = "kv-fdb"))]
				return Err(Error::Ds("Cannot connect to the `foundationdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// The datastore path is not valid
			_ => {
				info!("Unable to load the specified datastore {}", path);
				Err(Error::Ds("Unable to load the specified datastore".into()))
			}
		};
		// Set the properties on the datastore
		inner.map(|inner| Self {
			id: Uuid::default(),
			inner,
			strict: false,
			query_timeout: None,
			transaction_timeout: None,
			notification_channel: None,
		})
	}

	/// Specify whether this Datastore should run in strict mode
	pub fn with_strict_mode(mut self, strict: bool) -> Self {
		self.strict = strict;
		self
	}

	/// Specify whether this datastore should enable live query notifications
	pub fn with_notifications(mut self) -> Self {
		self.notification_channel = Some(channel::bounded(100));
		self
	}

	/// Set a global query timeout for this Datastore
	pub fn with_query_timeout(mut self, duration: Option<Duration>) -> Self {
		self.query_timeout = duration;
		self
	}

	/// Set a global transaction timeout for this Datastore
	pub fn with_transaction_timeout(mut self, duration: Option<Duration>) -> Self {
		self.transaction_timeout = duration;
		self
	}

	// Adds entries to the KV store indicating membership information
	pub async fn register_membership(&self) -> Result<(), Error> {
		let mut tx = self.transaction(true, false).await?;
		tx.set_cl(self.id).await?;
		tx.set_hb(self.id).await?;
		tx.commit().await?;
		Ok(())
	}

	// Creates a heartbeat entry for the member indicating to the cluster
	// that the node is alive
	pub async fn heartbeat(&self) -> Result<(), Error> {
		let mut tx = self.transaction(true, false).await?;
		tx.set_hb(self.id).await?;
		tx.commit().await?;
		Ok(())
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
			#[cfg(feature = "kv-speedb")]
			Inner::SpeeDB(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::SpeeDB(tx)
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
			Inner::FoundationDB(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::FoundationDB(tx)
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
	///     let res = ds.execute(ast, &ses, None).await?;
	///     Ok(())
	/// }
	/// ```
	#[instrument(skip_all)]
	pub async fn execute(
		&self,
		txt: &str,
		sess: &Session,
		vars: Variables,
	) -> Result<Vec<Response>, Error> {
		// Parse the SQL query text
		let ast = sql::parse(txt)?;
		// Process the AST
		self.process(ast, sess, vars).await
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
	///     let res = ds.process(ast, &ses, None).await?;
	///     Ok(())
	/// }
	/// ```
	#[instrument(skip_all)]
	pub async fn process(
		&self,
		ast: Query,
		sess: &Session,
		vars: Variables,
	) -> Result<Vec<Response>, Error> {
		// Create a new query options
		let opt = Options::default()
			.with_id(self.id)
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_live(sess.live())
			.with_auth(sess.au.clone())
			.with_strict(self.strict);
		// Create a new query executor
		let mut exe = Executor::new(self);
		// Create a default context
		let mut ctx = Context::default();
		// Set the global query timeout
		if let Some(timeout) = self.query_timeout {
			ctx.add_timeout(timeout);
		}
		// Setup the notification channel
		if let Some(channel) = &self.notification_channel {
			ctx.add_notifications(Some(&channel.0));
		}
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx)?;
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
	///     let res = ds.compute(val, &ses, None).await?;
	///     Ok(())
	/// }
	/// ```
	#[instrument(skip_all)]
	pub async fn compute(
		&self,
		val: Value,
		sess: &Session,
		vars: Variables,
	) -> Result<Value, Error> {
		// Create a new query options
		let opt = Options::default()
			.with_id(self.id)
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_live(sess.live())
			.with_auth(sess.au.clone())
			.with_strict(self.strict);
		// Start a new transaction
		let txn = self.transaction(val.writeable(), false).await?;
		//
		let txn = Arc::new(Mutex::new(txn));
		// Create a default context
		let mut ctx = Context::default();
		// Add the transaction
		ctx.add_transaction(Some(&txn));
		// Set the global query timeout
		if let Some(timeout) = self.query_timeout {
			ctx.add_timeout(timeout);
		}
		// Setup the notification channel
		if let Some(channel) = &self.notification_channel {
			ctx.add_notifications(Some(&channel.0));
		}
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx)?;
		// Compute the value
		let res = val.compute(&ctx, &opt).await?;
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
	///     let ds = Datastore::new("memory").await?.with_notifications();
	///     let ses = Session::for_kv();
	/// 	if let Some(channel) = ds.notifications() {
	///     	while let Ok(v) = channel.recv().await {
	///     	    println!("Received notification: {v}");
	///     	}
	/// 	}
	///     Ok(())
	/// }
	/// ```
	#[instrument(skip_all)]
	pub fn notifications(&self) -> Option<Receiver<Notification>> {
		self.notification_channel.as_ref().map(|v| v.1.clone())
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
