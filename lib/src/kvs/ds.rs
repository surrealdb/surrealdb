use super::tx::Transaction;
use crate::cf;
use crate::ctx::Context;
use crate::dbs::node::Timestamp;
use crate::dbs::Attach;
use crate::dbs::Capabilities;
use crate::dbs::Executor;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Response;
use crate::dbs::Session;
use crate::dbs::Variables;
use crate::err::Error;
use crate::iam::ResourceKind;
use crate::iam::{Action, Auth, Error as IamError, Role};
use crate::key::root::hb::Hb;
use crate::opt::auth::Root;
use crate::sql;
use crate::sql::statements::DefineUserStatement;
use crate::sql::Base;
use crate::sql::Value;
use crate::sql::{Query, Uuid};
use crate::vs;
use channel::Receiver;
use channel::Sender;
use futures::lock::Mutex;
use futures::Future;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::instrument;
use tracing::trace;
#[cfg(target_arch = "wasm32")]
use wasmtimer::std::{SystemTime, UNIX_EPOCH};

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
	// The versionstamp oracle for this datastore.
	// Used only in some datastores, such as tikv.
	vso: Arc<Mutex<vs::Oracle>>,
	// Whether this datastore enables live query notifications to subscribers
	notification_channel: Option<(Sender<Notification>, Receiver<Notification>)>,
	// Whether this datastore authentication is enabled. When disabled, anonymous actors have owner-level access.
	auth_enabled: bool,
	// Capabilities for this datastore
	capabilities: Capabilities,
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
			vso: Arc::new(Mutex::new(vs::Oracle::systime_counter())),
			notification_channel: None,
			auth_enabled: false,
			capabilities: Capabilities::default(),
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

	/// Enabled authentication for this Datastore?
	pub fn with_auth_enabled(mut self, enabled: bool) -> Self {
		self.auth_enabled = enabled;
		self
	}

	pub fn is_auth_enabled(&self) -> bool {
		self.auth_enabled
	}

	/// Setup the initial credentials
	pub async fn setup_initial_creds(&self, creds: Root<'_>) -> Result<(), Error> {
		let txn = Arc::new(Mutex::new(self.transaction(true, false).await?));
		let root_users = txn.lock().await.all_root_users().await;
		match root_users {
			Ok(val) if val.is_empty() => {
				info!(
					"Initial credentials were provided and no existing root-level users were found: create the initial user '{}'.", creds.username
				);
				let stm = DefineUserStatement::from((Base::Root, creds.username, creds.password));
				let ctx = Context::default();
				let opt = Options::new().with_auth(Arc::new(Auth::for_root(Role::Owner)));
				let _ = stm.compute(&ctx, &opt, &txn, None).await?;
				txn.lock().await.commit().await?;
				Ok(())
			}
			Ok(_) => {
				warn!("Initial credentials were provided but existing root-level users were found. Skip the initial user creation.");
				warn!("Consider removing the --user/--pass arguments from the server start.");
				txn.lock().await.commit().await?;
				Ok(())
			}
			Err(e) => {
				txn.lock().await.cancel().await?;
				Err(e)
			}
		}
	}

	/// Set a global transaction timeout for this Datastore
	pub fn with_transaction_timeout(mut self, duration: Option<Duration>) -> Self {
		self.transaction_timeout = duration;
		self
	}

	/// Configure Datastore capabilities
	pub fn with_capabilities(mut self, caps: Capabilities) -> Self {
		self.capabilities = caps;
		self
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
		let archived = match self.register_remove_and_archive(&mut tx, node_id, now).await {
			Ok(archived) => {
				tx.commit().await?;
				archived
			}
			Err(e) => {
				error!("Error bootstrapping mark phase: {:?}", e);
				tx.cancel().await?;
				return Err(e);
			}
		};

		let mut tx = self.transaction(true, false).await?;
		match self.remove_archived(&mut tx, archived).await {
			Ok(_) => tx.commit().await,
			Err(e) => {
				error!("Error bootstrapping sweep phase: {:?}", e);
				match tx.cancel().await {
					Ok(_) => Err(e),
					Err(e) => {
						// We have a nested error
						Err(Error::Tx(format!("Error bootstrapping sweep phase: {:?} and error cancelling transaction: {:?}", e, e)))
					}
				}
			}
		}
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
		tx.set_nd(node_id.0).await?;
		tx.set_hb(timestamp.clone(), node_id.0).await?;
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
			// TODO should be delr in case of nested entries
			tx.del_nd(hb.nd).await?;
			nodes.push(crate::sql::uuid::Uuid::from(hb.nd));
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
				let node_archived_lqs =
					self.archive_lv_for_node(tx, &lq.nd, this_node_id.clone()).await?;
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
			let key = crate::key::node::lq::new(lq.nd.0, lq.lq.0, &lq.ns, &lq.db);
			tx.del(key).await?;
			// Delete the table key, used for finding LQ associated with a table
			let key = crate::key::table::lq::new(&lq.ns, &lq.db, &lq.tb, lq.lq.0);
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
			let new_archived = self
				.archive_lv_for_node(tx, &crate::sql::uuid::Uuid::from(hb.nd), this_node_id.clone())
				.await?;
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
	pub async fn garbage_collect_dead_session(
		&self,
		live_queries: &[uuid::Uuid],
	) -> Result<(), Error> {
		let mut tx = self.transaction(true, false).await?;

		// Find all the LQs we own, so that we can get the ns/ds from provided uuids
		// We may improve this in future by tracking in web layer
		let lqs = tx.scan_ndlq(&self.id, 1000).await?;
		let mut hits = vec![];
		for lq_value in lqs {
			if live_queries.contains(&lq_value.lq) {
				hits.push(lq_value.clone());
				let lq = crate::key::node::lq::Lq::new(
					lq_value.nd.0,
					lq_value.lq.0,
					lq_value.ns.as_str(),
					lq_value.db.as_str(),
				);
				tx.del(lq).await?;
				trace!("Deleted lq {:?} as part of session garbage collection", lq_value.clone());
			}
		}

		// Now delete the table entries for the live queries
		for lq in hits {
			let lv =
				crate::key::table::lq::new(lq.ns.as_str(), lq.db.as_str(), lq.tb.as_str(), lq.lq.0);
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
		this_node_id: Uuid,
	) -> Result<Vec<LqValue>, Error> {
		let lqs = tx.all_lq(nd).await?;
		trace!("Archiving lqs and found {} LQ entries for {}", lqs.len(), nd);
		let mut ret = vec![];
		for lq in lqs {
			let lvs =
				tx.get_tb_live(lq.ns.as_str(), lq.db.as_str(), lq.tb.as_str(), &lq.lq).await?;
			let archived_lvs = lvs.clone().archive(this_node_id.clone());
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
		// Delete the heartbeat and everything nested
		tx.delr_hb(dead.clone(), 1000).await?;
		for dead_node in dead.clone() {
			tx.del_nd(dead_node.nd).await?;
		}
		Ok::<Vec<Hb>, Error>(dead)
	}

	// tick is called periodically to perform maintenance tasks.
	// This is called every TICK_INTERVAL.
	pub async fn tick(&self) -> Result<(), Error> {
		let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|e| {
			Error::Internal(format!("Clock may have gone backwards: {:?}", e.duration()))
		})?;
		let ts = now.as_secs();
		self.tick_at(ts).await?;
		Ok(())
	}

	// tick_at is the utility function that is called by tick.
	// It is handy for testing, because it allows you to specify the timestamp,
	// without depending on a system clock.
	pub async fn tick_at(&self, ts: u64) -> Result<(), Error> {
		self.save_timestamp_for_versionstamp(ts).await?;
		self.garbage_collect_stale_change_feeds(ts).await?;
		// TODO Add LQ GC
		// TODO Add Node GC?
		Ok(())
	}

	// save_timestamp_for_versionstamp saves the current timestamp for the each database's current versionstamp.
	pub async fn save_timestamp_for_versionstamp(&self, ts: u64) -> Result<(), Error> {
		let mut tx = self.transaction(true, false).await?;
		let nses = tx.all_ns().await?;
		let nses = nses.as_ref();
		for ns in nses {
			let ns = ns.name.as_str();
			let dbs = tx.all_db(ns).await?;
			let dbs = dbs.as_ref();
			for db in dbs {
				let db = db.name.as_str();
				tx.set_timestamp_for_versionstamp(ts, ns, db, true).await?;
			}
		}
		tx.commit().await?;
		Ok(())
	}

	// garbage_collect_stale_change_feeds deletes all change feed entries that are older than the watermarks.
	pub async fn garbage_collect_stale_change_feeds(&self, ts: u64) -> Result<(), Error> {
		let mut tx = self.transaction(true, false).await?;
		// TODO Make gc batch size/limit configurable?
		crate::cf::gc_all_at(&mut tx, ts, Some(100)).await?;
		tx.commit().await?;
		Ok(())
	}

	// Creates a heartbeat entry for the member indicating to the cluster
	// that the node is alive.
	// This is the preferred way of creating heartbeats inside the database, so try to use this.
	pub async fn heartbeat(&self) -> Result<(), Error> {
		let mut tx = self.transaction(true, false).await?;
		let timestamp = tx.clock();
		self.heartbeat_full(&mut tx, timestamp, self.id.clone()).await?;
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
		tx.set_hb(timestamp, node_id.0).await
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
		#[cfg(debug_assertions)]
		if lock {
			warn!("There are issues with pessimistic locking in TiKV");
		}
		self.transaction_inner(write, lock).await
	}

	pub async fn transaction_inner(&self, write: bool, lock: bool) -> Result<Transaction, Error> {
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
			cf: cf::Writer::new(),
			write_buffer: HashMap::new(),
			vso: self.vso.clone(),
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
	///     let ses = Session::owner();
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
	///     let ses = Session::owner();
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
		// Check if anonymous actors can execute queries when auth is enabled
		// TODO(sgirones): Check this as part of the authoritzation layer
		if self.auth_enabled && sess.au.is_anon() && !self.capabilities.allows_guest_access() {
			return Err(IamError::NotAllowed {
				actor: "anonymous".to_string(),
				action: "process".to_string(),
				resource: "query".to_string(),
			}
			.into());
		}

		// Create a new query options
		let opt = Options::default()
			.with_id(self.id.0)
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_live(sess.live())
			.with_auth(sess.au.clone())
			.with_auth_enabled(self.auth_enabled)
			.with_strict(self.strict);
		// Create a new query executor
		let mut exe = Executor::new(self);
		// Create a default context
		let mut ctx = Context::default();
		ctx.add_capabilities(self.capabilities.clone());
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
	///     let ses = Session::owner();
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
		// Check if anonymous actors can compute values when auth is enabled
		// TODO(sgirones): Check this as part of the authoritzation layer
		if self.auth_enabled && !self.capabilities.allows_guest_access() {
			return Err(IamError::NotAllowed {
				actor: "anonymous".to_string(),
				action: "compute".to_string(),
				resource: "value".to_string(),
			}
			.into());
		}

		// Create a new query options
		let opt = Options::default()
			.with_id(self.id.0)
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_live(sess.live())
			.with_auth(sess.au.clone())
			.with_auth_enabled(self.auth_enabled)
			.with_strict(self.strict);
		// Start a new transaction
		let txn = self.transaction(val.writeable(), false).await?;
		//
		let txn = Arc::new(Mutex::new(txn));
		// Create a default context
		let mut ctx = Context::default();
		// Set context capabilities
		ctx.add_capabilities(self.capabilities.clone());
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
	///     let ses = Session::owner();
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
	#[instrument(skip(self, sess, chn))]
	pub async fn prepare_export(
		&self,
		sess: &Session,
		ns: String,
		db: String,
		chn: Sender<Vec<u8>>,
	) -> Result<impl Future<Output = Result<(), Error>>, Error> {
		let mut txn = self.transaction(false, false).await?;

		// Skip auth for Anonymous users if auth is disabled
		let skip_auth = !self.is_auth_enabled() && sess.au.is_anon();
		if !skip_auth {
			sess.au.is_allowed(Action::View, &ResourceKind::Any.on_db(&ns, &db))?;
		}

		Ok(async move {
			// Start a new transaction
			// Process the export
			let ns = ns.to_owned();
			let db = db.to_owned();
			txn.export(&ns, &db, chn).await?;
			// Everything ok
			Ok(())
		})
	}

	/// Performs a database import from SQL
	#[instrument(skip(self, sess, sql))]
	pub async fn import(&self, sql: &str, sess: &Session) -> Result<Vec<Response>, Error> {
		// Skip auth for Anonymous users if auth is disabled
		let skip_auth = !self.is_auth_enabled() && sess.au.is_anon();
		if !skip_auth {
			sess.au.is_allowed(
				Action::Edit,
				&ResourceKind::Any.on_level(sess.au.level().to_owned()),
			)?;
		}

		self.execute(sql, sess, None).await
	}
}
