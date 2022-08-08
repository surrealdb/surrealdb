use super::tx::Transaction;
use crate::ctx::Context;
use crate::dbs::Attach;
use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Response;
use crate::dbs::Session;
use crate::dbs::Variables;
use crate::err::Error;
use crate::kvs::LOG;
use crate::sql;
use crate::sql::Query;
use crate::sql::Value;
use channel::Sender;
use futures::lock::Mutex;
use std::sync::Arc;

/// The underlying datastore instance which stores the dataset.
pub struct Datastore {
	pub(super) inner: Inner,
}

#[allow(clippy::large_enum_variant)]
pub(super) enum Inner {
	#[cfg(feature = "kv-echodb")]
	Mem(super::mem::Datastore),
	#[cfg(feature = "kv-indxdb")]
	IxDB(super::ixdb::Datastore),
	#[cfg(feature = "kv-yokudb")]
	File(super::file::Datastore),
	#[cfg(feature = "kv-tikv")]
	TiKV(super::tikv::Datastore),
}

impl Datastore {
	/// Creates a new datastore instance
	///
	/// # Examples
	///
	/// ```rust,no_run
	/// # use surrealdb::Datastore;
	/// # use surrealdb::Error;
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
	/// # use surrealdb::Datastore;
	/// # use surrealdb::Error;
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
	/// # use surrealdb::Datastore;
	/// # use surrealdb::Error;
	/// # #[tokio::main]
	/// # async fn main() -> Result<(), Error> {
	/// let ds = Datastore::new("tikv://127.0.0.1:2379").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		match path {
			#[cfg(feature = "kv-echodb")]
			"memory" => {
				info!(target: LOG, "Starting kvs store in {}", path);
				let v = super::mem::Datastore::new().await.map(|v| Datastore {
					inner: Inner::Mem(v),
				});
				info!(target: LOG, "Started kvs store in {}", path);
				v
			}
			// Parse and initiate an IxDB database
			#[cfg(feature = "kv-indxdb")]
			s if s.starts_with("ixdb:") => {
				info!(target: LOG, "Starting kvs store at {}", path);
				let s = s.trim_start_matches("ixdb://");
				let v = super::ixdb::Datastore::new(s).await.map(|v| Datastore {
					inner: Inner::IxDB(v),
				});
				info!(target: LOG, "Started kvs store at {}", path);
				v
			}
			// Parse and initiate an File database
			#[cfg(feature = "kv-yokudb")]
			s if s.starts_with("file:") => {
				info!(target: LOG, "Starting kvs store at {}", path);
				let s = s.trim_start_matches("file://");
				let v = super::file::Datastore::new(s).await.map(|v| Datastore {
					inner: Inner::File(v),
				});
				info!(target: LOG, "Started kvs store at {}", path);
				v
			}
			// Parse and initiate an TiKV database
			#[cfg(feature = "kv-tikv")]
			s if s.starts_with("tikv:") => {
				info!(target: LOG, "Connecting to kvs store at {}", path);
				let s = s.trim_start_matches("tikv://");
				let v = super::tikv::Datastore::new(s).await.map(|v| Datastore {
					inner: Inner::TiKV(v),
				});
				info!(target: LOG, "Connected to kvs store at {}", path);
				v
			}
			// The datastore path is not valid
			_ => unreachable!(),
		}
	}

	/// Create a new transaction on this datastore
	pub async fn transaction(&self, write: bool, lock: bool) -> Result<Transaction, Error> {
		match &self.inner {
			#[cfg(feature = "kv-echodb")]
			Inner::Mem(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction {
					inner: super::tx::Inner::Mem(tx),
					cache: super::cache::Cache::default(),
				})
			}
			#[cfg(feature = "kv-indxdb")]
			Inner::IxDB(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction {
					inner: super::tx::Inner::IxDB(tx),
					cache: super::cache::Cache::default(),
				})
			}
			#[cfg(feature = "kv-yokudb")]
			Inner::File(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction {
					inner: super::tx::Inner::File(tx),
					cache: super::cache::Cache::default(),
				})
			}
			#[cfg(feature = "kv-tikv")]
			Inner::TiKV(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction {
					inner: super::tx::Inner::TiKV(tx),
					cache: super::cache::Cache::default(),
				})
			}
		}
	}

	/// Parse and execute an SQL query
	pub async fn execute(
		&self,
		txt: &str,
		sess: &Session,
		vars: Variables,
		strict: bool,
	) -> Result<Vec<Response>, Error> {
		// Create a new query options
		let mut opt = Options::default();
		// Create a new query executor
		let mut exe = Executor::new(self);
		// Create a default context
		let ctx = Context::default();
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx);
		// Parse the SQL query text
		let ast = sql::parse(txt)?;
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
	pub async fn process(
		&self,
		ast: Query,
		sess: &Session,
		vars: Variables,
		strict: bool,
	) -> Result<Vec<Response>, Error> {
		// Create a new query options
		let mut opt = Options::default();
		// Create a new query executor
		let mut exe = Executor::new(self);
		// Create a default context
		let ctx = Context::default();
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx);
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
	pub async fn compute(
		&self,
		val: Value,
		sess: &Session,
		vars: Variables,
		strict: bool,
	) -> Result<Value, Error> {
		// Start a new transaction
		let txn = self.transaction(val.writeable(), false).await?;
		//
		let txn = Arc::new(Mutex::new(txn));
		// Create a new query options
		let mut opt = Options::default();
		// Create a default context
		let ctx = Context::default();
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx);
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

	/// Performs a full database export as SQL
	pub async fn export(&self, ns: String, db: String, chn: Sender<Vec<u8>>) -> Result<(), Error> {
		// Start a new transaction
		let mut txn = self.transaction(false, false).await?;
		// Process the export
		txn.export(&ns, &db, chn).await?;
		// Everythign ok
		Ok(())
	}
}
