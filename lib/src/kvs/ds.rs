use super::tx::Transaction;
use crate::ctx::Context;
use crate::dbs::Attach;
use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Response;
use crate::dbs::Session;
use crate::dbs::Variables;
use crate::err::Error;
use crate::kvs::cache::moka::MokaCache;
use crate::kvs::DatastoreFacade;
use crate::kvs::{AVAILABLE_DATASTORE_METADATA, LOG};
use crate::sql;
use crate::sql::Query;
use crate::sql::Value;
use channel::Sender;
use futures::lock::Mutex;
use std::sync::Arc;
use tracing::{error, info, instrument};

/// The underlying datastore instance which stores the dataset.
#[allow(dead_code)]
pub struct Datastore {
	pub(super) inner: Box<dyn DatastoreFacade + Send + Sync>,
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
		for ds in AVAILABLE_DATASTORE_METADATA {
			if ds.connection_string_match_prefix(path) {
				info!(target: LOG, path = path, "Starting kvs store");
				let inner = ds.new(path).await?;
				info!(target: LOG, path = path, "Started kvs store");
				return Ok(Datastore {
					inner,
				});
			}
		}

		error!(target: LOG, path = path, "Unable to load the specified datastore");
		Err(Error::Ds("Unable to load the specified datastore".into()))
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
		let cache = Box::new(MokaCache::new());
		let tx = self.inner.transaction(write, lock).await?;
		Ok(Transaction {
			inner: tx,
			cache,
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
		let mut opt = Options::default();
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
		let mut opt = Options::default();
		// Create a new query executor
		let mut exe = Executor::new(self);
		// Create a default context
		let ctx = Context::default();
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx)?;
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
	///     let val = Value::Future(Box::new(Future::from(Value::True)));
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
		let ctx = vars.attach(ctx)?;
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
