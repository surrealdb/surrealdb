use crate::ctx::Context;
use crate::dbs::node::Timestamp;
use crate::dbs::Attach;
use crate::dbs::Executor;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Response;
use crate::dbs::Session;
use crate::dbs::Variables;
use crate::err::Error;
use crate::key::root::hb::Hb;
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
use tracing::trace;
use uuid::Uuid;

use super::tx::Transaction;

/// Used for cluster logic to move LQ data to LQ cleanup code
/// Not a stored struct; Used only in this module
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LqValue {
	pub nd: Uuid,
	pub ns: String,
	pub db: String,
	pub tb: String,
	pub lq: Uuid,
}

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
		let id = Uuid::new_v4();
		Self::new_full(path, id).await
	}

	// For testing
	pub async fn new_full(path: &str, node_id: Uuid) -> Result<Datastore, Error> {
		// Initiate the desired datastore
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
			id: node_id,
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
		trace!("Bootstrapping {}", self.id);
		let mut tx = self.transaction(true, false).await?;
		let now = tx.clock();
		let archived = self.register_remove_and_archive(&mut tx, node_id, now).await?;
		tx.commit().await?;

		let mut tx = self.transaction(true, false).await?;
		self.remove_archived(&mut tx, archived).await?;
		tx.commit().await
	}

	// Node registration + "mark" stage of mark-and-sweep gc
	pub async fn register_remove_and_archive(
		&self,
		tx: &mut Transaction,
		node_id: &Uuid,
		timestamp: Timestamp,
	) -> Result<Vec<LqValue>, Error> {
		trace!("Registering node {}", node_id);
		self.register_membership(tx, node_id, &timestamp).await?;
		// Determine the timeout for when a cluster node is expired
		let ts_expired = (timestamp.clone() - std::time::Duration::from_secs(5))?;
		let dead = self.remove_dead_nodes(tx, &ts_expired).await?;
		self.archive_dead_lqs(tx, &dead, node_id).await
	}

	// Adds entries to the KV store indicating membership information
	pub async fn register_membership(
		&self,
		tx: &mut Transaction,
		node_id: &Uuid,
		timestamp: &Timestamp,
	) -> Result<(), Error> {
		tx.set_nd(*node_id).await?;
		tx.set_hb(timestamp.clone(), *node_id).await?;
		Ok(())
	}

	/// Delete dead heartbeats and nodes
	/// Returns node IDs
	pub async fn remove_dead_nodes(
		&self,
		tx: &mut Transaction,
		ts: &Timestamp,
	) -> Result<Vec<Uuid>, Error> {
		let hbs = self.delete_dead_heartbeats(tx, ts).await?;
		let mut nodes = vec![];
		for hb in hbs {
			trace!("Deleting node {}", &hb.nd);
			tx.del_nd(hb.nd).await?;
			nodes.push(hb.nd);
		}
		Ok(nodes)
	}

	/// Accepts cluster IDs
	/// Archives related live queries
	/// Returns live query keys that can be used for deletes
	///
	/// The reason we archive first is to stop other nodes from picking it up for further updates
	/// This means it will be easier to wipe the range in a subsequent transaction
	pub async fn archive_dead_lqs(
		&self,
		tx: &mut Transaction,
		nodes: &[Uuid],
		this_node_id: &Uuid,
	) -> Result<Vec<LqValue>, Error> {
		let mut archived = vec![];
		for nd in nodes.iter() {
			trace!("Archiving node {}", &nd);
			// Scan on node prefix for LQ space
			let node_lqs = tx.scan_ndlq(nd, 1000).await?;
			trace!("Found {} LQ entries for {:?}", node_lqs.len(), nd);
			for lq in node_lqs {
				trace!("Archiving query {:?}", &lq);
				let node_archived_lqs = self.archive_lv_for_node(tx, &lq.nd, this_node_id).await?;
				for lq_value in node_archived_lqs {
					archived.push(lq_value);
				}
			}
		}
		Ok(archived)
	}

	pub async fn remove_archived(
		&self,
		tx: &mut Transaction,
		archived: Vec<LqValue>,
	) -> Result<(), Error> {
		for lq in archived {
			// Delete the cluster key, used for finding LQ associated with a node
			let key = crate::key::node::lq::new(lq.nd, &lq.ns, &lq.db, lq.lq);
			tx.del(key).await?;
			// Delete the table key, used for finding LQ associated with a table
			let key = crate::key::table::lq::new(&lq.ns, &lq.db, &lq.tb, lq.lq);
			tx.del(key).await?;
		}
		Ok(())
	}

	pub async fn _garbage_collect(
		// TODO not invoked
		// But this is garbage collection outside of bootstrap
		&self,
		tx: &mut Transaction,
		watermark: &Timestamp,
		this_node_id: &Uuid,
	) -> Result<(), Error> {
		let dead_heartbeats = self.delete_dead_heartbeats(tx, watermark).await?;
		trace!("Found dead hbs: {:?}", dead_heartbeats);
		let mut archived: Vec<LqValue> = vec![];
		for hb in dead_heartbeats {
			let new_archived = self.archive_lv_for_node(tx, &hb.nd, this_node_id).await?;
			tx.del_nd(hb.nd).await?;
			trace!("Deleted node {}", hb.nd);
			for lq_value in new_archived {
				archived.push(lq_value);
			}
		}
		Ok(())
	}

	// Garbage collection task to run when a client disconnects from a surrealdb node
	// i.e. we know the node, we are not performing a full wipe on the node
	// and the wipe must be fully performed by this node
	pub async fn garbage_collect_dead_session(&self, live_queries: &[Uuid]) -> Result<(), Error> {
		let mut tx = self.transaction(true, false).await?;

		// Find all the LQs we own, so that we can get the ns/ds from provided uuids
		// We may improve this in future by tracking in web layer
		let lqs = tx.scan_ndlq(&self.id, 1000).await?;
		let mut hits = vec![];
		for lq_value in lqs {
			if live_queries.contains(&lq_value.lq) {
				hits.push(lq_value.clone());
				let lq = crate::key::node::lq::Lq::new(
					lq_value.nd.clone(),
					lq_value.ns.as_str(),
					lq_value.db.as_str(),
					lq_value.lq.clone(),
				);
				tx.del(lq).await?;
				trace!("Deleted lq {:?} as part of session garbage collection", lq_value.clone());
			}
		}

		// Now delete the table entries for the live queries
		for lq in hits {
			let lv =
				crate::key::table::lq::new(lq.ns.as_str(), lq.db.as_str(), lq.tb.as_str(), self.id);
			tx.del(lv.clone()).await?;
			trace!("Deleted lv {:?} as part of session garbage collection", lv);
		}
		tx.commit().await
	}

	// Returns a list of live query IDs
	pub async fn archive_lv_for_node(
		&self,
		tx: &mut Transaction,
		nd: &Uuid,
		this_node_id: &Uuid,
	) -> Result<Vec<LqValue>, Error> {
		let lqs = tx.all_lq(nd).await?;
		trace!("Archiving lqs and found {} LQ entries for {}", lqs.len(), nd);
		let mut ret = vec![];
		for lq in lqs {
			let lvs = tx.get_lv(lq.ns.as_str(), lq.db.as_str(), lq.tb.as_str(), &lq.lq).await?;
			let archived_lvs = lvs.clone().archive(*this_node_id);
			tx.putc_tblq(&lq.ns, &lq.db, &lq.tb, archived_lvs, Some(lvs)).await?;
			ret.push(lq);
		}
		Ok(ret)
	}

	/// Given a timestamp, delete all the heartbeats that have expired
	/// Return the removed heartbeats as they will contain node information
	pub async fn delete_dead_heartbeats(
		&self,
		tx: &mut Transaction,
		ts: &Timestamp,
	) -> Result<Vec<Hb>, Error> {
		let limit = 1000;
		let dead = tx.scan_hb(ts, limit).await?;
		tx.delr_hb(dead.clone(), 1000).await?;
		for dead_node in dead.clone() {
			tx.del_nd(dead_node.nd).await?;
		}
		Ok::<Vec<Hb>, Error>(dead)
	}

	// Creates a heartbeat entry for the member indicating to the cluster
	// that the node is alive.
	// This is the preferred way of creating heartbeats inside the database, so try to use this.
	pub async fn heartbeat(&self) -> Result<(), Error> {
		let mut tx = self.transaction(true, false).await?;
		let timestamp = tx.clock();
		self.heartbeat_full(&mut tx, timestamp, self.id).await?;
		tx.commit().await
	}

	// Creates a heartbeat entry for the member indicating to the cluster
	// that the node is alive. Intended for testing.
	// This includes all dependencies that are hard to control and is done in such a way for testing.
	// Inside the database, try to use the heartbeat() function instead.
	pub async fn heartbeat_full(
		&self,
		tx: &mut Transaction,
		timestamp: Timestamp,
		node_id: Uuid,
	) -> Result<(), Error> {
		tx.set_hb(timestamp, node_id).await
	}

	// -----
	// End cluster helpers, storage functions here
	// -----

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
