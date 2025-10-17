use crate::err::Error;
use crate::key::root::tl::Tl;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::{impl_key, KeyDecode, KeyEncode, LockType, Transaction, TransactionType};
use chrono::{DateTime, Duration, Utc};
use rand::{thread_rng, Rng};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::trace;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) enum TaskLeaseType {
	/// Index compaction
	IndexCompaction,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct TaskLease {
	owner: Uuid,
	expiration: DateTime<Utc>,
}

impl_key!(TaskLease);

#[derive(Debug)]
enum LeaseStatus {
	Valid,
	Renewable,
	Expired,
}

/// Manages distributed task leases in a multi-node environment.
///
/// The LeaseHandler provides a mechanism for coordinating tasks across multiple
/// nodes by implementing a distributed lease system. This ensures that only one
/// node at a time performs a specific task, preventing duplicate work and race
/// conditions in a distributed setup.
///
/// # Race Condition Prevention
///
/// The handler uses optimistic locking with conditional writes (putc) to prevent
/// race conditions when multiple nodes attempt to acquire the same lease simultaneously.
/// Only one node can successfully acquire a lease, even under high contention.
///
/// # Features
///
/// The handler uses a datastore to persist lease information, with built-in
/// support for:
/// - Lease acquisition with exponential backoff and jitter to handle contention
/// - Automatic lease expiration based on configurable duration
/// - Automatic lease renewal when more than half the duration has elapsed
/// - Lease ownership verification
pub struct LeaseHandler {
	/// UUID of the current node trying to acquire or check leases
	node: Uuid,
	/// Transaction factory for database operations
	tf: TransactionFactory,
	/// Type of task this lease handler is managing
	task_type: TaskLeaseType,
	/// How long each acquired lease should remain valid
	lease_duration: Duration,
}

impl LeaseHandler {
	pub(super) fn new(
		node: Uuid,
		tf: TransactionFactory,
		task_type: TaskLeaseType,
		lease_duration: std::time::Duration,
	) -> Result<Self, Error> {
		Ok(Self {
			node,
			tf,
			task_type,
			lease_duration: Duration::from_std(lease_duration)
				.map_err(|e| Error::Internal(e.to_string()))?,
		})
	}

	/// Attempts to acquire or check a lease for a specific task type.
	///
	/// This method tries to determine if the current node has a lease for the
	/// specified task. It uses exponential backoff with jitter to handle
	/// contention when multiple nodes attempt to acquire the same lease
	/// simultaneously.
	///
	/// # Returns
	/// * `Ok(true)` - If the node successfully acquired or already owns the lease
	/// * `Ok(false)` - If another node owns the lease
	/// * `Err` - If the operation timed out or encountered other errors
	pub(super) async fn has_lease(&self) -> Result<bool, Error> {
		// Initial backoff time in milliseconds
		let mut tempo = 4;
		// Maximum backoff time in milliseconds before giving up
		const MAX_BACKOFF: u64 = 32_768;

		// Loop until we have a successful allocation or reach max backoff
		// We use exponential backoff with a maximum limit to prevent infinite retries
		while tempo < MAX_BACKOFF {
			match self.check_lease().await {
				Ok(r) => return Ok(r),
				Err(e) => {
					trace!("Tolerated error while getting a lease for {:?}: {e}", self.task_type);
				}
			}
			// Apply exponential backoff with full jitter to reduce contention
			// This randomizes sleep time between 1 and current tempo value
			let sleep_ms = thread_rng().gen_range(1..=tempo);
			sleep(std::time::Duration::from_millis(sleep_ms)).await;
			// Double the backoff time for next iteration
			tempo *= 2;
		}
		// If we've reached maximum backoff without success, time out the operation
		Err(Error::QueryTimedout)
	}

	/// Attempts to maintain the current lease by checking and potentially
	/// renewing it.
	///
	/// This method is a simplified wrapper around `check_lease()` that:
	/// 1. Checks if the current node owns a valid lease
	/// 2. Renews the lease if needed
	/// 3. Ignores the boolean return value from `check_lease()`
	///
	/// # Returns
	/// * `Ok(())` - If the lease check completed successfully (regardless of ownership)
	/// * `Err` - If database operations fail
	pub(crate) async fn try_maintain_lease(&self) -> Result<(), Error> {
		self.check_lease().await?;
		Ok(())
	}

	/// Checks if a lease exists and attempts to acquire or renew it.
	///
	/// This method performs the actual lease checking, acquisition, and renewal logic:
	/// 1. Checks if there's an existing lease in the datastore
	/// 2. If another node owns a non-expired lease, returns false
	/// 3. If this node owns the lease and it's still valid (more than half duration remaining),
	///    returns true without renewal
	/// 4. If this node owns the lease and it's renewable (less than half duration remaining),
	///    attempts to renew it
	/// 5. If no lease exists or it has expired, attempts to acquire a new lease
	///
	/// # Returns
	/// * `Ok(true)` - If the node successfully acquired, renewed, or already owns a valid lease
	/// * `Ok(false)` - If another node owns the lease
	/// * `Err` - If database operations fail
	async fn check_lease(&self) -> Result<bool, Error> {
		let now = Utc::now();
		// First check if there's already a valid lease
		if let Some((current_lease, lease_status)) = self.read_lease(now).await? {
			// If another node owns a non-expired lease, we cannot acquire it
			if current_lease.owner != self.node && !matches!(lease_status, LeaseStatus::Expired) {
				return Ok(false);
			}
			// If we own the lease and it's still valid (more than half the lease duration
			// remaining), we can continue using it without renewal
			if matches!(lease_status, LeaseStatus::Valid) {
				return Ok(true);
			}
			// If we reach here, we own the lease but it's in Renewable state (close to expiring),
			// so we'll try to acquire a new lease below to extend it
		}

		// Acquire a new lease - this handles three scenarios:
		// 1. No lease exists yet
		// 2. An expired lease exists (any owner)
		// 3. We own a renewable lease that needs to be extended
		self.acquire_new_lease(now).await
	}

	async fn read_lease(
		&self,
		current: DateTime<Utc>,
	) -> Result<Option<(TaskLease, LeaseStatus)>, Error> {
		let tx = self.tf.transaction(TransactionType::Read, LockType::Optimistic).await?;
		self.get_lease(&tx, current).await
	}

	/// Checks if there's an existing valid lease and determines the expiration status
	async fn get_lease(
		&self,
		tx: &Transaction,
		current: DateTime<Utc>,
	) -> Result<Option<(TaskLease, LeaseStatus)>, Error> {
		if let Some(l) = tx.get(&Tl::new(&self.task_type), None).await? {
			let lease = TaskLease::decode(&l)?;
			let status = if current > lease.expiration {
				LeaseStatus::Expired
			} else if current > lease.expiration - self.lease_duration / 2 {
				LeaseStatus::Renewable
			} else {
				LeaseStatus::Valid
			};
			Ok(Some((lease, status)))
		} else {
			Ok(None)
		}
	}

	/// Attempts to acquire a new lease for the current node using atomic conditional writes.
	///
	/// This method implements the core lease acquisition logic with race condition protection.
	/// It uses optimistic locking with conditional writes to ensure that only one node can
	/// successfully acquire a lease at a time, even when multiple nodes attempt acquisition
	/// simultaneously.
	///
	/// # Returns
	/// * `Ok(true)` - If the lease was successfully acquired by this node
	/// * `Ok(false)` - If another node owns a valid (non-expired) lease
	/// * `Err` - If database operations fail or a race condition is detected
	async fn acquire_new_lease(&self, current: DateTime<Utc>) -> Result<bool, Error> {
		let tx = self.tf.transaction(TransactionType::Write, LockType::Optimistic).await?;

		// Optimization: If a valid (non-expired) lease already exists and we don't own it,
		// return early without attempting to write. This avoids unnecessary transaction overhead.
		let previous_lease = if let Some((current_lease, current_status)) =
			self.get_lease(&tx, current).await?
		{
			if current_lease.owner != self.node && !matches!(current_status, LeaseStatus::Expired) {
				return Ok(false);
			}
			Some(current_lease.encode()?)
		} else {
			None
		};

		let new_lease = TaskLease {
			owner: self.node,
			expiration: current + self.lease_duration,
		}
		.encode()?;

		// Use putc() (conditional put) to atomically write the lease ONLY if the current value
		// matches what we read earlier. This prevents race conditions:
		// - If previous_lease is None: writes succeed only if the key still doesn't exist
		// - If previous_lease is Some(expired): writes succeed only if the value hasn't changed
		// - If another node wrote between our get() and putc(): condition fails with
		//   TxConditionNotMet
		//
		// This ensures mutual exclusion: only one node can successfully acquire the lease when
		// multiple nodes attempt acquisition simultaneously (e.g., when replacing an expired
		// lease).
		tx.putc(&Tl::new(&self.task_type), new_lease, previous_lease).await?;
		tx.commit().await?;
		Ok(true)
	}
}

#[cfg(test)]
#[cfg(any(feature = "kv-rocksdb", feature = "kv-mem"))]
mod tests {
	use std::sync::Arc;
	use std::time::{Duration, Instant};

	#[cfg(feature = "kv-mem")]
	use chrono::Utc;
	#[cfg(feature = "kv-rocksdb")]
	use temp_dir::TempDir;
	#[cfg(feature = "kv-mem")]
	use tokio::time::sleep;
	use uuid::Uuid;

	use crate::dbs::node::Timestamp;
	use crate::kvs::clock::{FakeClock, SizedClock};
	use crate::kvs::ds::{DatastoreFlavor, TransactionFactory};
	#[cfg(feature = "kv-mem")]
	use crate::kvs::tasklease::LeaseStatus;
	use crate::kvs::tasklease::{LeaseHandler, TaskLeaseType};

	/// Tracks the results of lease acquisition attempts by a node.
	///
	/// This struct collects statistics about the outcomes of multiple lease
	/// acquisition attempts:
	/// * `ok_true` - Count of successful lease acquisitions (node owns the lease)
	/// * `ok_false` - Count of failed lease acquisitions (another node owns the lease)
	/// * `err` - Count of errors encountered during lease acquisition attempts
	#[derive(Default)]
	struct NodeResult {
		ok_true: usize,
		ok_false: usize,
		err: usize,
	}

	/// Simulates a node repeatedly attempting to acquire a lease for a
	/// specified duration.
	///
	/// This function represents a single node in a distributed system that
	/// continuously tries to acquire a task lease for the IndexCompaction
	/// task. It runs for a fixed duration and collects statistics on the
	/// outcomes of each attempt.
	///
	/// # Parameters
	/// * `id` - UUID identifying the node
	/// * `tf` - Transaction factory for database operations
	/// * `test_duration` - How long the node should run and attempt to acquire leases
	/// * `lease_duration` - How long each acquired lease should be valid
	///
	/// # Returns
	/// A `NodeResult` containing statistics about the lease acquisition
	/// attempts:
	/// * How many times the node successfully acquired the lease
	/// * How many times the node failed to acquire the lease (owned by another node)
	/// * How many errors occurred during lease acquisition attempts
	async fn node_task_lease(
		id: Uuid,
		tf: TransactionFactory,
		test_duration: Duration,
		lease_duration: Duration,
	) -> NodeResult {
		let lh = LeaseHandler::new(id, tf, TaskLeaseType::IndexCompaction, lease_duration).unwrap();
		let mut result = NodeResult::default();
		let start_time = Instant::now();
		while start_time.elapsed() < test_duration {
			match lh.has_lease().await {
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
	/// This function simulates a distributed environment where multiple nodes
	/// compete for the same task lease. It creates three concurrent nodes,
	/// each with a unique ID, and has them all attempt to acquire the same
	/// lease repeatedly for a fixed duration.
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
		let tf = TransactionFactory::new(clock, flavor);
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
		// Verify that at least one node failed to acquire the lease (another node owned
		// it)
		assert!(res1.ok_false + res2.ok_false + res3.ok_false > 0);
		// Verify that no errors occurred during lease acquisition
		assert_eq!(res1.err + res2.err + res3.err, 0);
	}

	/// Tests the task lease concurrency mechanism using an in-memory datastore.
	///
	/// This test creates an in-memory datastore and runs the task lease
	/// concurrency test to verify that the lease mechanism works correctly in
	/// a multi-threaded environment with an in-memory storage backend.
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
	/// This test creates a temporary RocksDB datastore and runs the task lease
	/// concurrency test to verify that the lease mechanism works correctly in
	/// a multi-threaded environment with a persistent storage backend.
	///
	/// The test is only compiled and run when the "kv-rocksdb" feature is
	/// enabled.
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

	/// Tests the lease renewal behavior when a node already owns a lease.
	///
	/// This test verifies that:
	/// 1. A node that owns a lease doesn't try to re-acquire it if more than half the lease
	///    duration remains
	/// 2. A node that owns a lease does try to re-acquire it if less than half the lease duration
	///    remains
	///
	/// Note: This test has limitations because we can't directly control the
	/// `Utc::now()` used in `check_lease()`. Instead, we verify the behavior
	/// by:
	/// - Checking that multiple calls to `check_lease()` in quick succession don't change the lease
	///   expiration
	/// - Manually verifying the condition that would trigger renewal (less than half duration
	///   remaining)
	/// - Forcing a renewal by calling `check_lease()` and verifying the expiration changes
	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn test_lease_renewal_behavior() {
		// Create a fake clock for deterministic testing
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		// Create an in-memory datastore
		let flavor = crate::kvs::mem::Datastore::new().await.map(DatastoreFlavor::Mem).unwrap();
		let tf = TransactionFactory::new(clock, flavor);
		let node_id = Uuid::new_v4();

		// Set lease duration to 5 seconds
		let lease_duration = Duration::from_secs(5);

		// Create a lease handler
		let lh =
			LeaseHandler::new(node_id, tf, TaskLeaseType::IndexCompaction, lease_duration).unwrap();

		// PART 1: Initial lease acquisition
		// Initially acquire the lease
		let has_lease = lh.check_lease().await.unwrap();
		assert!(has_lease, "Should successfully acquire the lease initially");

		let (initial_lease, status) = lh.read_lease(Utc::now()).await.unwrap().unwrap();
		assert_eq!(initial_lease.owner, node_id);
		assert!(
			matches!(status, LeaseStatus::Valid),
			"Lease should be initially valid (not expired and not renewable) - {status:?}"
		);

		// PART 2: Verify no renewal when more than half duration remains
		// Check again immediately - should return true without re-acquiring
		let has_lease = lh.check_lease().await.unwrap();
		assert!(has_lease, "Should still have the lease without re-acquiring");

		let (lease, status) = lh.read_lease(Utc::now()).await.unwrap().unwrap();
		// Verify the status
		assert!(
			matches!(status, LeaseStatus::Valid),
			"Lease should still be valid (not expired and not renewable) - {status:?}"
		);
		// Verify the expiration hasn't changed (no renewal)
		assert_eq!(lease.expiration, initial_lease.expiration);

		// PART 3: Verify the condition for renewal
		// Wait half the lease duration
		sleep(Duration::from_secs(3)).await;

		// Get the current lease status
		let (_, status) = lh.read_lease(Utc::now()).await.unwrap().unwrap();
		assert!(matches!(status, LeaseStatus::Renewable), "Lease should be renewable - {status:?}");

		// Now trigger the renewal
		let has_lease = lh.check_lease().await.unwrap();
		assert!(has_lease, "Should still have the lease after renewal");

		// Get the renewed lease
		let (new_lease, status) = lh.read_lease(Utc::now()).await.unwrap().unwrap();
		assert!(matches!(status, LeaseStatus::Valid), "Lease should be valid - {status:?}");
		assert!(new_lease.expiration > initial_lease.expiration, "Lease should have been renewed");
	}
}
