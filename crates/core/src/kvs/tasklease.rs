use crate::err::Error;
use crate::key::root::tl::Tl;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::{Key, KeyEncode, LockType, TransactionType};
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use rand::{Rng, thread_rng};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use tracing::trace;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) enum TaskLeaseType {
	/// Task for cleaning up old changefeed data
	ChangeFeedCleanup,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[non_exhaustive]
struct TaskLease {
	owner: Uuid,
	expiration: DateTime<Utc>,
}

impl TaskLeaseType {
	/// Attempts to acquire or check a lease for a specific task type.
	///
	/// This method tries to determine if the current node has a lease for the specified task.
	/// It uses exponential backoff with jitter to handle contention when multiple nodes
	/// attempt to acquire the same lease simultaneously.
	///
	/// # Parameters
	/// * `node` - The UUID of the node trying to acquire/check the lease
	/// * `tf` - Transaction factory for database operations
	/// * `lt` - Lease duration (how long the lease should be valid)
	///
	/// # Returns
	/// * `Ok(true)` - If the node successfully acquired or already owns the lease
	/// * `Ok(false)` - If another node owns the lease
	/// * `Err` - If the operation timed out or encountered other errors
	pub(super) async fn has_lease(
		&self,
		node: &Uuid,
		tf: &TransactionFactory,
		lt: &Duration,
	) -> Result<bool> {
		let key: Key = Tl::new(self).encode()?;
		// Initial backoff time in milliseconds
		let mut tempo = 4;
		// Maximum backoff time in milliseconds before giving up
		const MAX_BACKOFF: u64 = 32_768;

		// Loop until we have a successful allocation or reach max backoff
		// We check the timeout inherited from the context
		while tempo < MAX_BACKOFF {
			match self.check_lease(node, tf, &key, lt).await {
				Ok(r) => return Ok(r),
				Err(e) => {
					trace!("Tolerated error while getting a lease for {self:?}: {e}");
				}
			}
			// Apply exponential backoff with full jitter to reduce contention
			// This randomizes sleep time between 1 and current tempo value
			let sleep_ms = thread_rng().gen_range(1..=tempo);
			sleep(Duration::from_millis(sleep_ms)).await;
			// Double the backoff time for next iteration
			tempo *= 2;
		}
		// If we've reached maximum backoff without success, time out the operation
		bail!(Error::QueryTimedout)
	}

	/// Checks if a lease exists and attempts to acquire it if it doesn't or has expired.
	///
	/// This method performs the actual lease checking and acquisition logic:
	/// 1. First checks if there's an existing valid lease in the datastore
	/// 2. If a valid lease exists, returns whether the current node is the owner
	/// 3. If no valid lease exists or it has expired, attempts to create a new lease
	///
	/// # Parameters
	/// * `node` - The UUID of the node trying to acquire/check the lease
	/// * `tf` - Transaction factory for database operations
	/// * `key` - The key used to store the lease in the datastore
	/// * `lt` - Lease duration (how long the lease should be valid)
	///
	/// # Returns
	/// * `Ok(true)` - If the node successfully acquired or already owns the lease
	/// * `Ok(false)` - If another node owns the lease
	/// * `Err` - If database operations fail
	async fn check_lease(
		&self,
		node: &Uuid,
		tf: &TransactionFactory,
		key: &Key,
		lt: &Duration,
	) -> Result<bool> {
		// First check if there's already a valid lease
		if let Some(is_owner) = self.check_existing_lease(node, tf, key).await? {
			return Ok(is_owner);
		}

		// If no valid lease exists or it has expired, acquire a new one
		self.acquire_new_lease(node, tf, key, lt).await
	}

	/// Checks if there's an existing valid lease and determines if the current node is the owner.
	///
	/// # Parameters
	/// * `node` - The UUID of the node checking the lease
	/// * `tf` - Transaction factory for database operations
	/// * `key` - The key used to store the lease in the datastore
	///
	/// # Returns
	/// * `Ok(Some(true))` - If a valid lease exists and this node is the owner
	/// * `Ok(Some(false))` - If a valid lease exists but another node is the owner
	/// * `Ok(None)` - If no valid lease exists (either no lease or it has expired)
	/// * `Err` - If database operations fail
	async fn check_existing_lease(
		&self,
		node: &Uuid,
		tf: &TransactionFactory,
		key: &Key,
	) -> Result<Option<bool>> {
		let tx = tf.transaction(TransactionType::Read, LockType::Optimistic).await?;
		if let Some(l) = tx.get(key, None).await? {
			let l: TaskLease = revision::from_slice(&l)?;
			// If the lease hasn't expired yet, check if this node is the owner
			if l.expiration > Utc::now() {
				// Return whether this node is the owner
				return Ok(Some(l.owner.eq(node)));
			}
			// If we reach here, the lease exists but has expired
		}
		// If we reach here, either no lease exists or it has expired
		Ok(None)
	}

	/// Attempts to acquire a new lease for the current node.
	///
	/// # Parameters
	/// * `node` - The UUID of the node acquiring the lease
	/// * `tf` - Transaction factory for database operations
	/// * `key` - The key used to store the lease in the datastore
	/// * `lt` - Lease duration (how long the lease should be valid)
	///
	/// # Returns
	/// * `Ok(true)` - If the lease was successfully acquired
	/// * `Err` - If database operations fail
	async fn acquire_new_lease(
		&self,
		node: &Uuid,
		tf: &TransactionFactory,
		key: &Key,
		lt: &Duration,
	) -> Result<bool> {
		// Attempt to acquire a new lease by writing to the datastore
		let tx = tf.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let lease = TaskLease {
			owner: *node,
			expiration: Utc::now() + *lt, // Set expiration to current time plus lease duration
		};
		tx.set(key, revision::to_vec(&lease)?, None).await?;
		tx.commit().await?;
		// Successfully acquired the lease
		Ok(true)
	}
}

#[cfg(test)]
#[cfg(any(feature = "kv-rocksdb", feature = "kv-mem"))]
mod tests {
	use crate::dbs::node::Timestamp;
	use crate::kvs::clock::{FakeClock, SizedClock};
	use crate::kvs::ds::{DatastoreFlavor, TransactionFactory};
	use crate::kvs::tasklease::TaskLeaseType;
	use std::sync::Arc;
	use std::time::{Duration, Instant};
	use temp_dir::TempDir;
	use uuid::Uuid;

	/// Tracks the results of lease acquisition attempts by a node.
	///
	/// This struct collects statistics about the outcomes of multiple lease acquisition attempts:
	/// * `ok_true` - Count of successful lease acquisitions (node owns the lease)
	/// * `ok_false` - Count of failed lease acquisitions (another node owns the lease)
	/// * `err` - Count of errors encountered during lease acquisition attempts
	#[derive(Default)]
	struct NodeResult {
		ok_true: usize,
		ok_false: usize,
		err: usize,
	}

	/// Simulates a node repeatedly attempting to acquire a lease for a specified duration.
	///
	/// This function represents a single node in a distributed system that continuously
	/// tries to acquire a task lease for the ChangeFeedCleanup task. It runs for a fixed
	/// duration and collects statistics on the outcomes of each attempt.
	///
	/// # Parameters
	/// * `id` - UUID identifying the node
	/// * `tf` - Transaction factory for database operations
	/// * `test_duration` - How long the node should run and attempt to acquire leases
	/// * `lease_duration` - How long each acquired lease should be valid
	///
	/// # Returns
	/// A `NodeResult` containing statistics about the lease acquisition attempts:
	/// * How many times the node successfully acquired the lease
	/// * How many times the node failed to acquire the lease (owned by another node)
	/// * How many errors occurred during lease acquisition attempts
	#[cfg(feature = "kv-mem")]
	async fn node_task_lease(
		id: Uuid,
		tf: Arc<TransactionFactory>,
		test_duration: Duration,
		lease_duration: Duration,
	) -> NodeResult {
		let mut result = NodeResult::default();
		let start_time = Instant::now();
		while start_time.elapsed() < test_duration {
			match TaskLeaseType::ChangeFeedCleanup.has_lease(&id, &tf, &lease_duration).await {
				Ok(true) => {
					result.ok_true += 1;
				}
				Ok(false) => {
					result.ok_false += 1;
				}
				Err(_e) => {
					result.err += 1;
				}
			}
		}
		result
	}

	/// Tests the task lease mechanism with multiple concurrent nodes.
	///
	/// This function simulates a distributed environment where multiple nodes compete
	/// for the same task lease. It creates three concurrent nodes, each with a unique ID,
	/// and has them all attempt to acquire the same lease repeatedly for a fixed duration.
	///
	/// The test verifies that:
	/// 1. At least one node successfully acquires the lease at some point
	/// 2. At least one node fails to acquire the lease at some point (because another node owns it)
	/// 3. No errors occur during the lease acquisition process
	///
	/// # Parameters
	/// * `flavor` - The type of datastore to use for the test (memory or RocksDB)
	async fn task_lease_concurrency(flavor: DatastoreFlavor) {
		// Create a fake clock for deterministic testing
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		// Create a transaction factory with the specified datastore flavor
		let tf = Arc::new(TransactionFactory::new(clock, flavor));
		// Set test to run for 3 seconds
		let test_duration = Duration::from_secs(3);
		// Set each lease to be valid for 1 second
		let lease_duration = Duration::from_secs(1);
		// Create a closure that generates a node task with a specific UUID
		let new_node =
			|n| node_task_lease(Uuid::from_u128(n), tf.clone(), test_duration, lease_duration);

		// Spawn three concurrent nodes with different UUIDs
		let node1 = tokio::spawn(new_node(0));
		let node2 = tokio::spawn(new_node(1));
		let node3 = tokio::spawn(new_node(2));

		// Wait for all nodes to complete and collect their results
		let (res1, res2, res3) = tokio::try_join!(node1, node2, node3).expect("Tasks failed");

		// Verify that at least one node successfully acquired the lease
		assert!(res1.ok_true + res2.ok_true + res3.ok_true > 0);
		// Verify that at least one node failed to acquire the lease (another node owned it)
		assert!(res1.ok_false + res2.ok_false + res3.ok_false > 0);
		// Verify that no errors occurred during lease acquisition
		assert_eq!(res1.err + res2.err + res3.err, 0);
	}

	/// Tests the task lease concurrency mechanism using an in-memory datastore.
	///
	/// This test creates an in-memory datastore and runs the task lease concurrency test
	/// to verify that the lease mechanism works correctly in a multi-threaded environment
	/// with an in-memory storage backend.
	///
	/// The test is only compiled and run when the "kv-mem" feature is enabled.
	#[cfg(feature = "kv-mem")]
	#[tokio::test(flavor = "multi_thread")]
	async fn task_lease_concurrency_memory() {
		// Create a new in-memory datastore
		let flavor = crate::kvs::mem::Datastore::new().await.map(DatastoreFlavor::Mem).unwrap();
		// Run the concurrency test with the in-memory datastore
		task_lease_concurrency(flavor).await;
	}

	/// Tests the task lease concurrency mechanism using a RocksDB datastore.
	///
	/// This test creates a temporary RocksDB datastore and runs the task lease concurrency test
	/// to verify that the lease mechanism works correctly in a multi-threaded environment
	/// with a persistent storage backend.
	///
	/// The test is only compiled and run when the "kv-rocksdb" feature is enabled.
	#[cfg(feature = "kv-rocksdb")]
	#[tokio::test(flavor = "multi_thread")]
	async fn task_lease_concurrency_rocksdb() {
		// Create a temporary directory for the RocksDB datastore
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		// Create a new RocksDB datastore in the temporary directory
		let flavor =
			crate::kvs::rocksdb::Datastore::new(&path).await.map(DatastoreFlavor::RocksDB).unwrap();
		// Run the concurrency test with the RocksDB datastore
		task_lease_concurrency(flavor).await;
	}
}
