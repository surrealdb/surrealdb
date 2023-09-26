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
use crate::kvs::{LockType, LockType::*, TransactionType, TransactionType::*};
use crate::opt::auth::Root;
use crate::sql;
use crate::sql::statements::DefineUserStatement;
use crate::sql::Base;
use crate::sql::Value;
use crate::sql::{Query, Uuid};
use crate::vs::Oracle;
use channel::Receiver;
use channel::Sender;
use futures::lock::Mutex;
use futures::Future;
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
	// Whether authentication is enabled on this datastore.
	auth_enabled: bool,
	// The maximum duration timeout for running multiple statements in a query
	query_timeout: Option<Duration>,
	// The maximum duration timeout for running multiple statements in a transaction
	transaction_timeout: Option<Duration>,
	// Capabilities for this datastore
	capabilities: Capabilities,
	// The versionstamp oracle for this datastore.
	// Used only in some datastores, such as tikv.
	versionstamp_oracle: Arc<Mutex<Oracle>>,
	// Whether this datastore enables live query notifications to subscribers
	notification_channel: Option<(Sender<Notification>, Receiver<Notification>)>,
}

/// We always want to be circulating the live query information
/// And we will sometimes have an error attached but still not want to lose the LQ.
pub(crate) type BootstrapOperationResult = (LqValue, Option<Error>);

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
			id: Uuid::new_v4(),
			inner,
			strict: false,
			auth_enabled: false,
			query_timeout: None,
			transaction_timeout: None,
			notification_channel: None,
			capabilities: Capabilities::default(),
			versionstamp_oracle: Arc::new(Mutex::new(Oracle::systime_counter())),
		})
	}

	/// Specify whether this Datastore should run in strict mode
	pub fn with_node_id(mut self, id: Uuid) -> Self {
		self.id = id;
		self
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

	/// Set whether authentication is enabled for this Datastore
	pub fn with_auth_enabled(mut self, enabled: bool) -> Self {
		self.auth_enabled = enabled;
		self
	}

	/// Set specific capabilities for this Datastore
	pub fn with_capabilities(mut self, caps: Capabilities) -> Self {
		self.capabilities = caps;
		self
	}

	/// Is authentication enabled for this Datastore?
	pub fn is_auth_enabled(&self) -> bool {
		self.auth_enabled
	}

	/// Setup the initial credentials
	/// Trigger the `unreachable definition` compilation error, probably due to this issue:
	/// https://github.com/rust-lang/rust/issues/111370
	#[allow(unreachable_code, unused_variables)]
	pub async fn setup_initial_creds(&self, creds: Root<'_>) -> Result<(), Error> {
		// Start a new writeable transaction
		let txn = self.transaction(Write, Optimistic).await?.rollback_with_panic().enclose();
		// Fetch the root users from the storage
		let users = txn.lock().await.all_root_users().await;
		// Process credentials, depending on existing users
		match users {
			Ok(v) if v.is_empty() => {
				// Display information in the logs
				info!("Credentials were provided, and no root users were found. The root user '{}' will be created", creds.username);
				// Create and save a new root users
				let stm = DefineUserStatement::from((Base::Root, creds.username, creds.password));
				let ctx = Context::default();
				let opt = Options::new().with_auth(Arc::new(Auth::for_root(Role::Owner)));
				let _ = stm.compute(&ctx, &opt, &txn, None).await?;
				// We added a new user, so commit the transaction
				txn.lock().await.commit().await?;
				// Everything ok
				Ok(())
			}
			Ok(_) => {
				// Display warnings in the logs
				warn!("Credentials were provided, but existing root users were found. The root user '{}' will not be created", creds.username);
				warn!("Consider removing the --user and --pass arguments from the server start command");
				// We didn't write anything, so just rollback
				txn.lock().await.cancel().await?;
				// Everything ok
				Ok(())
			}
			Err(e) => {
				// There was an unexpected error, so rollback
				txn.lock().await.cancel().await?;
				// Return any error
				Err(e)
			}
		}
	}

	// Initialise bootstrap with implicit values intended for runtime
	// An error indicates that a failure happened, but that does not mean that the bootstrap
	// completely failed. It may have partially completed. It certainly has side-effects
	// that weren't reversed, as it tries to bootstrap and garbage collect to the best of its
	// ability.
	pub async fn bootstrap(&self) -> Result<(), Error> {
		trace!("Clearing cluster");
		let mut tx = self.transaction(Write, Optimistic).await?;
		match self.nuke_whole_cluster(&mut tx).await {
			Ok(_) => tx.commit().await,
			Err(e) => {
				error!("Error nuking cluster at bootstrap: {:?}", e);
				tx.cancel().await?;
				Err(Error::Tx(format!("Error nuking cluster at bootstrap: {:?}", e).to_owned()))
			}
		}?;

		trace!("Bootstrapping {}", self.id);
		let mut tx = self.transaction(Write, Optimistic).await?;
		let now = tx.clock();
		let archived = match self.register_remove_and_archive(&mut tx, &self.id, now).await {
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
		// Filtered includes all lqs that should be used in subsequent step
		// Currently that is all of them, no matter the error encountered
		let mut filtered: Vec<LqValue> = vec![];
		// err is used to aggregate all errors across all stages
		let mut err = vec![];
		for res in archived {
			match res {
				(lq, Some(e)) => {
					filtered.push(lq);
					err.push(e);
				}
				(lq, None) => {
					filtered.push(lq);
				}
			}
		}

		let mut tx = self.transaction(Write, Optimistic).await?;
		let val = self.remove_archived(&mut tx, filtered).await;
		let resolve_err = match val {
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
		};
		if resolve_err.is_err() {
			err.push(resolve_err.unwrap_err());
		}
		if !err.is_empty() {
			error!("Error bootstrapping sweep phase: {:?}", err);
			return Err(Error::Tx(format!("Error bootstrapping sweep phase: {:?}", err)));
		}
		Ok(())
	}

	// Node registration + "mark" stage of mark-and-sweep gc
	pub async fn register_remove_and_archive(
		&self,
		tx: &mut Transaction,
		node_id: &Uuid,
		timestamp: Timestamp,
	) -> Result<Vec<BootstrapOperationResult>, Error> {
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
		tx.set_cl(node_id.0).await?;
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
			tx.del_cl(hb.nd).await?;
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
	) -> Result<Vec<BootstrapOperationResult>, Error> {
		let mut archived = vec![];
		for nd in nodes.iter() {
			trace!("Archiving node {}", &nd);
			// Scan on node prefix for LQ space
			let node_lqs = tx.scan_ndlq(nd, 1000).await?;
			trace!("Found {} LQ entries for {:?}", node_lqs.len(), nd);
			for lq in node_lqs {
				trace!("Archiving query {:?}", &lq);
				let node_archived_lqs =
					match self.archive_lv_for_node(tx, &lq.nd, this_node_id.clone()).await {
						Ok(lq) => lq,
						Err(e) => {
							error!("Error archiving lqs during bootstrap phase: {:?}", e);
							vec![]
						}
					};
				// We need to add lv nodes not found so that they can be deleted in second stage
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
		trace!("Gone into removing archived");
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

	pub async fn nuke_whole_cluster(&self, tx: &mut Transaction) -> Result<(), Error> {
		// Scan nodes
		let cls = tx.scan_cl(1000).await?;
		trace!("Found {} nodes", cls.len());
		for cl in cls {
			tx.del_cl(
				uuid::Uuid::parse_str(&cl.name).map_err(|e| {
					Error::Unimplemented(format!("cluster id was not uuid: {:?}", e))
				})?,
			)
			.await?;
		}
		// Scan heartbeats
		let hbs = tx
			.scan_hb(
				&Timestamp {
					value: 0,
				},
				1000,
			)
			.await?;
		trace!("Found {} heartbeats", hbs.len());
		for hb in hbs {
			tx.del_hb(hb.hb, hb.nd).await?;
		}
		// Scan node live queries
		let ndlqs = tx.scan_ndlq(&self.id, 1000).await?;
		trace!("Found {} node live queries", ndlqs.len());
		for ndlq in ndlqs {
			tx.del_ndlq(&ndlq.nd).await?;
			// Scan table live queries
			let tblqs = tx.scan_tblq(&ndlq.ns, &ndlq.db, &ndlq.tb, 1000).await?;
			trace!("Found {} table live queries", tblqs.len());
			for tblq in tblqs {
				tx.del_tblq(&ndlq.ns, &ndlq.db, &ndlq.tb, tblq.lq.0).await?;
			}
		}
		trace!("Successfully completed nuke");
		Ok(())
	}

	// Garbage collection task to run when a client disconnects from a surrealdb node
	// i.e. we know the node, we are not performing a full wipe on the node
	// and the wipe must be fully performed by this node
	pub async fn garbage_collect_dead_session(
		&self,
		live_queries: &[uuid::Uuid],
	) -> Result<(), Error> {
		let mut tx = self.transaction(Write, Optimistic).await?;

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
	) -> Result<Vec<BootstrapOperationResult>, Error> {
		let lqs = tx.all_lq(nd).await?;
		trace!("Archiving lqs and found {} LQ entries for {}", lqs.len(), nd);
		let mut ret: Vec<BootstrapOperationResult> = vec![];
		for lq in lqs {
			let lv_res =
				tx.get_tb_live(lq.ns.as_str(), lq.db.as_str(), lq.tb.as_str(), &lq.lq).await;
			if let Err(e) = lv_res {
				error!("Error getting live query for node {}: {:?}", nd, e);
				ret.push((lq, Some(e)));
				continue;
			}
			let lv = lv_res.unwrap();
			let archived_lvs = lv.clone().archive(this_node_id.clone());
			tx.putc_tblq(&lq.ns, &lq.db, &lq.tb, archived_lvs, Some(lv)).await?;
			ret.push((lq, None));
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
			tx.del_cl(dead_node.nd).await?;
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
		let mut tx = self.transaction(Write, Optimistic).await?;
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
		let mut tx = self.transaction(Write, Optimistic).await?;
		// TODO Make gc batch size/limit configurable?
		crate::cf::gc_all_at(&mut tx, ts, Some(100)).await?;
		tx.commit().await?;
		Ok(())
	}

	// Creates a heartbeat entry for the member indicating to the cluster
	// that the node is alive.
	// This is the preferred way of creating heartbeats inside the database, so try to use this.
	pub async fn heartbeat(&self) -> Result<(), Error> {
		let mut tx = self.transaction(Write, Optimistic).await?;
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
	/// use surrealdb::kvs::{Datastore, TransactionType::*, LockType::*};
	/// use surrealdb::err::Error;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Error> {
	///     let ds = Datastore::new("file://database.db").await?;
	///     let mut tx = ds.transaction(Write, Optimistic).await?;
	///     tx.cancel().await?;
	///     Ok(())
	/// }
	/// ```
	pub async fn transaction(
		&self,
		write: TransactionType,
		lock: LockType,
	) -> Result<Transaction, Error> {
		#![allow(unused_variables)]
		let write = match write {
			TransactionType::Read => false,
			TransactionType::Write => true,
		};

		let lock = match lock {
			LockType::Pessimistic => true,
			LockType::Optimistic => false,
		};

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
			vso: self.versionstamp_oracle.clone(),
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
	#[instrument(level = "debug", skip_all)]
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
	#[instrument(level = "debug", skip_all)]
	pub async fn process(
		&self,
		ast: Query,
		sess: &Session,
		vars: Variables,
	) -> Result<Vec<Response>, Error> {
		// Check if anonymous actors can execute queries when auth is enabled
		// TODO(sgirones): Check this as part of the authorisation layer
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
			.with_strict(self.strict)
			.with_auth_enabled(self.auth_enabled);
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
	#[instrument(level = "debug", skip_all)]
	pub async fn compute(
		&self,
		val: Value,
		sess: &Session,
		vars: Variables,
	) -> Result<Value, Error> {
		// Check if anonymous actors can compute values when auth is enabled
		// TODO(sgirones): Check this as part of the authorisation layer
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
			.with_strict(self.strict)
			.with_auth_enabled(self.auth_enabled);
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
		// Start a new transaction
		let txn = self.transaction(val.writeable().into(), Optimistic).await?.enclose();
		// Compute the value
		let res = val.compute(&ctx, &opt, &txn, None).await;
		// Store any data
		match (res.is_ok(), val.writeable()) {
			// If the compute was successful, then commit if writeable
			(true, true) => txn.lock().await.commit().await?,
			// Cancel if the compute was an error, or if readonly
			(_, _) => txn.lock().await.cancel().await?,
		};
		// Return result
		res
	}

	/// Evaluates a SQL [`Value`] without checking authenticating config
	/// This is used in very specific cases, where we do not need to check
	/// whether authentication is enabled, or guest access is disabled.
	/// For example, this is used when processing a SCOPE SIGNUP or SCOPE
	/// SIGNIN clause, which still needs to work without guest access.
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
	///     let res = ds.evaluate(val, &ses, None).await?;
	///     Ok(())
	/// }
	/// ```
	#[instrument(level = "debug", skip_all)]
	pub async fn evaluate(
		&self,
		val: Value,
		sess: &Session,
		vars: Variables,
	) -> Result<Value, Error> {
		// Create a new query options
		let opt = Options::default()
			.with_id(self.id.0)
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_live(sess.live())
			.with_auth(sess.au.clone())
			.with_strict(self.strict)
			.with_auth_enabled(self.auth_enabled);
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
		// Start a new transaction
		let txn = self.transaction(val.writeable().into(), Optimistic).await?.enclose();
		// Compute the value
		let res = val.compute(&ctx, &opt, &txn, None).await;
		// Store any data
		match (res.is_ok(), val.writeable()) {
			// If the compute was successful, then commit if writeable
			(true, true) => txn.lock().await.commit().await?,
			// Cancel if the compute was an error, or if readonly
			(_, _) => txn.lock().await.cancel().await?,
		};
		// Return result
		res
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
	#[instrument(level = "debug", skip_all)]
	pub fn notifications(&self) -> Option<Receiver<Notification>> {
		self.notification_channel.as_ref().map(|v| v.1.clone())
	}

	/// Performs a full database export as SQL
	#[instrument(level = "debug", skip(self, sess, chn))]
	pub async fn export(
		&self,
		sess: &Session,
		ns: String,
		db: String,
		chn: Sender<Vec<u8>>,
	) -> Result<impl Future<Output = Result<(), Error>>, Error> {
		// Skip auth for Anonymous users if auth is disabled
		let skip_auth = !self.is_auth_enabled() && sess.au.is_anon();
		if !skip_auth {
			sess.au.is_allowed(Action::View, &ResourceKind::Any.on_db(&ns, &db))?;
		}
		// Create a new readonly transaction
		let mut txn = self.transaction(Read, Optimistic).await?;
		// Return an async export job
		Ok(async move {
			// Process the export
			txn.export(&ns, &db, chn).await?;
			// Everything ok
			Ok(())
		})
	}

	/// Performs a database import from SQL
	#[instrument(level = "debug", skip(self, sess, sql))]
	pub async fn import(&self, sql: &str, sess: &Session) -> Result<Vec<Response>, Error> {
		// Skip auth for Anonymous users if auth is disabled
		let skip_auth = !self.is_auth_enabled() && sess.au.is_anon();
		if !skip_auth {
			sess.au.is_allowed(
				Action::Edit,
				&ResourceKind::Any.on_level(sess.au.level().to_owned()),
			)?;
		}
		// Execute the SQL import
		self.execute(sql, sess, None).await
	}
}
