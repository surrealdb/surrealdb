use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Result, bail};
use chrono::{DateTime, Duration, Utc};
use rand::{Rng, thread_rng};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::trace;
use uuid::Uuid;
use web_time::Instant;

use crate::err::Error;
use crate::key::root::tl::Tl;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::sequences::Sequences;
use crate::kvs::{
	Error as KvsError, LockType, Transaction, TransactionType, impl_kv_value_revisioned,
};

#[derive(Debug, Clone)]
pub(crate) enum TaskLeaseType {
	/// Task for cleaning up old changefeed data
	ChangeFeedCleanup,
	/// Index compaction
	IndexCompaction,
	/// Event processing
	EventProcessing,
}

/// Represents a distributed task lease stored in the datastore.
///
/// A TaskLease records which node currently owns the exclusive right to perform
/// a specific task, and when that right expires. The lease is stored in the
/// datastore and checked/updated atomically to ensure only one node can hold
/// the lease at any given time.
///
/// # Fields
/// * `owner` - UUID of the node that currently owns this lease
/// * `expiration` - UTC timestamp when this lease will expire
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct TaskLease {
	owner: Uuid,
	expiration: DateTime<Utc>,
}

impl_kv_value_revisioned!(TaskLease);

#[cfg(test)]
impl TaskLease {
	pub(crate) fn new(owner: Uuid, expiration: DateTime<Utc>) -> Self {
		Self {
			owner,
			expiration,
		}
	}
}

/// Represents the current status of a lease based on its expiration time.
///
/// The status determines whether a lease can be used as-is, needs renewal,
/// or has expired and can be replaced. Status is calculated by comparing
/// the current time against the lease's expiration time and duration.
///
/// # Variants
///
/// * `Valid` - The lease has more than half its duration remaining before expiration. The owning
///   node can continue using it without renewal.
///
/// * `Renewable` - The lease has less than half its duration remaining before expiration, but
///   hasn't expired yet. The owning node should renew it to maintain ownership.
///
/// * `Expired` - The lease's expiration time has passed. Any node can attempt to acquire a new
///   lease to replace it.
///
/// # Status Transitions
///
/// ```text
/// Valid (>50% remaining) → Renewable (0-50% remaining) → Expired (past expiration)
/// ```
#[derive(Debug)]
enum LeaseStatus {
	Valid,
	Renewable,
	Expired,
}

/// Manages distributed task leases in a multi-node environment.
///
/// The LeaseHandler provides a mechanism for coordinating tasks across multiple
/// nodes by implementing a distributed lease system. This helps keep a single
/// node responsible for a task at a time, while allowing an in-flight batch to
/// complete even if the lease expires.
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
#[derive(Clone)]
pub struct LeaseHandler {
	sequences: Sequences,
	/// UUID of the current node trying to acquire or check leases
	node: Uuid,
	/// Transaction factory for database operations
	tf: TransactionFactory,
	/// Type of task this lease handler is managing
	task_type: TaskLeaseType,
	/// How long each acquired lease should remain valid
	lease_duration: Duration,
	/// Unix timestamp (seconds) of the last lease maintenance check
	last_maintain_check: Arc<AtomicI64>,
	/// Maintenance period in seconds used to throttle lease checks
	maintain_period: i64,
}

impl LeaseHandler {
	/// Creates a new LeaseHandler for managing distributed task leases.
	///
	/// This constructor initializes a lease handler that will manage leases for a specific
	/// task type using the provided transaction factory and lease duration.
	///
	/// The effective lease duration is clamped to a minimum of 8 seconds to prevent
	/// excessive contention from very short lease durations.
	///
	/// # Arguments
	/// * `sequences` - Sequence generator for transaction operations
	/// * `node` - UUID of the current node that will attempt to acquire leases
	/// * `tf` - Transaction factory for performing database operations
	/// * `task_type` - The type of task this handler will manage leases for
	/// * `lease_duration` - How long each acquired lease should remain valid before expiring
	///   (minimum effective duration: 8 seconds)
	///
	/// # Returns
	/// * `Ok(Self)` - A new LeaseHandler instance ready to manage leases
	/// * `Err` - If the lease_duration cannot be converted to chrono::Duration (e.g., if it's too
	///   large)
	pub(super) fn new(
		sequences: Sequences,
		node: Uuid,
		tf: TransactionFactory,
		task_type: TaskLeaseType,
		lease_duration: std::time::Duration,
	) -> Result<Self> {
		Ok(Self {
			sequences,
			node,
			tf,
			task_type,
			lease_duration: Duration::from_std(lease_duration)?.max(Duration::seconds(8)),
			last_maintain_check: Arc::new(AtomicI64::new(0)),
			maintain_period: ((lease_duration.as_secs() / 8) as i64).max(1),
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
	pub(super) async fn has_lease(&self) -> Result<bool> {
		// Initial backoff time in milliseconds
		let mut tempo = 4;
		// Maximum backoff time in milliseconds before giving up
		const MAX_BACKOFF: u64 = 32_768;

		// Loop until we have a successful allocation or reach max backoff
		// We use exponential backoff with a maximum limit to prevent infinite retries
		let start = Instant::now();
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
		bail!(Error::QueryTimedout(start.elapsed().into()))
	}

	/// Attempts to maintain the current lease by checking and potentially renewing it.
	///
	/// This method is throttled: it performs an actual lease check at most once per maintenance
	/// period (`lease_duration / 8`). When throttled (i.e., called again before the maintenance
	/// period has elapsed), it returns `Ok(true)` immediately without contacting the datastore,
	/// assuming the lease is still held.
	///
	/// When a check is performed, it provides lease ownership status to allow callers to decide
	/// whether to continue processing or stop:
	/// - **Continue processing**: Callers may choose to complete work already started even if the
	///   lease is lost, preventing inconsistent state
	/// - **Stop processing**: Callers may choose to abort immediately when losing the lease to
	///   avoid duplicate work with another node
	///
	/// The initial lease check (via `has_lease()`) at the start of a task ensures only one node
	/// begins processing. This method is for lease maintenance and ownership verification during
	/// execution.
	///
	/// # Returns
	/// * `Ok(true)` - The lease check was throttled (assumed still held), or the node successfully
	///   maintained/renewed the lease
	/// * `Ok(false)` - The node lost the lease to another node (or another node acquired it during
	///   a race condition)
	/// * `Err` - If database operations fail
	///
	/// # Example Usage
	/// ```ignore
	/// // Check lease before starting work
	/// if !lease_handler.has_lease().await? {
	///     return Ok(()); // Another node owns the lease
	/// }
	///
	/// // Example 1: Continue processing regardless of lease status (complete work once started)
	/// for item in items {
	///     let _ = lease_handler.try_maintain_lease().await?; // Ignore lease ownership
	///     process_item(item).await?;
	/// }
	///
	/// // Example 2: Stop processing immediately if lease is lost
	/// for item in items {
	///     if !lease_handler.try_maintain_lease().await? {
	///         return Ok(()); // Another node owns the lease now
	///     }
	///     process_item(item).await?;
	/// }
	/// ```
	pub(crate) async fn try_maintain_lease(&self) -> Result<bool> {
		let now = Utc::now();
		let now_ts = now.timestamp();
		let last = self.last_maintain_check.load(Ordering::Relaxed);
		if (now_ts < last || now_ts - last > self.maintain_period)
			&& self
				.last_maintain_check
				.compare_exchange(last, now_ts, Ordering::Relaxed, Ordering::Relaxed)
				.is_ok()
		{
			// Check and potentially renew the lease, returning ownership status.
			// Callers can use this information to decide whether to continue or stop processing.
			return self.check_lease().await;
		}
		Ok(true)
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
	async fn check_lease(&self) -> Result<bool> {
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

	/// Convenience wrapper that creates a read-only transaction and retrieves the lease.
	///
	/// This method is used when checking lease status without intending to modify it.
	/// It creates a read transaction and delegates to `get_lease()` for the actual retrieval.
	///
	/// # Arguments
	/// * `current` - The current timestamp to use for determining lease status
	///
	/// # Returns
	/// * `Ok(Some((lease, status)))` - If a lease exists, returns it with its current status
	/// * `Ok(None)` - If no lease exists for this task type
	/// * `Err` - If database operations fail
	async fn read_lease(&self, current: DateTime<Utc>) -> Result<Option<(TaskLease, LeaseStatus)>> {
		let tx = self
			.tf
			.transaction(TransactionType::Read, LockType::Optimistic, self.sequences.clone())
			.await?;
		self.get_lease(&tx, current).await
	}

	/// Retrieves the current lease from the datastore and determines its status.
	///
	/// This method fetches the lease for the handler's task type and calculates its status
	/// by comparing the current time against the lease's expiration time and duration:
	///
	/// - **Valid**: More than 50% of the lease duration remains (no action needed)
	/// - **Renewable**: Between 0% and 50% of the lease duration remains (should renew soon)
	/// - **Expired**: Past the expiration time (can be replaced by any node)
	///
	/// # Arguments
	/// * `tx` - The transaction to use for database operations
	/// * `current` - The current timestamp to use for determining lease status
	///
	/// # Returns
	/// * `Ok(Some((lease, status)))` - If a lease exists, returns it with its calculated status
	/// * `Ok(None)` - If no lease exists for this task type
	/// * `Err` - If database operations fail
	async fn get_lease(
		&self,
		tx: &Transaction,
		current: DateTime<Utc>,
	) -> Result<Option<(TaskLease, LeaseStatus)>> {
		if let Some(lease) = tx.get(&Tl::new(&self.task_type), None).await? {
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
	/// # Race Condition Handling
	///
	/// When multiple nodes attempt to acquire or renew a lease simultaneously, the conditional
	/// write (`putc`) may fail with `TxConditionNotMet` if another node wrote to the lease
	/// between our read and write operations. This is gracefully handled by returning `Ok(false)`
	/// rather than propagating an error, allowing ongoing tasks to continue processing while
	/// acknowledging that another node now owns the lease.
	///
	/// # Returns
	/// * `Ok(true)` - If the lease was successfully acquired by this node
	/// * `Ok(false)` - If another node owns a valid lease or acquired it during our attempt
	/// * `Err` - Only if database operations fail (network errors, transaction failures, etc.)
	async fn acquire_new_lease(&self, current: DateTime<Utc>) -> Result<bool> {
		let tx = self
			.tf
			.transaction(TransactionType::Write, LockType::Optimistic, self.sequences.clone())
			.await?;

		// Re-check within the write transaction: if another node owns a non-expired lease,
		// return early without attempting to write. This avoids unnecessary write attempts
		// when the lease state changed between the initial read and this write transaction.
		let previous_lease = if let Some((current_lease, current_status)) =
			self.get_lease(&tx, current).await?
		{
			if current_lease.owner != self.node && !matches!(current_status, LeaseStatus::Expired) {
				return Ok(false);
			}
			Some(current_lease)
		} else {
			None
		};

		let new_lease = TaskLease {
			owner: self.node,
			expiration: current + self.lease_duration,
		};

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
		let res = tx.putc(&Tl::new(&self.task_type), &new_lease, previous_lease.as_ref()).await;
		match res {
			Ok(()) => {
				tx.commit().await?;
				Ok(true)
			}
			// CRITICAL: Convert TxConditionNotMet to Ok(false) rather than propagating as an error.
			// This race condition occurs when another node acquires/renews the lease between our
			// read and write operations. By returning Ok(false) instead of Err, we enable the
			// best-effort lease maintenance behavior: try_maintain_lease() can ignore the result
			// and allow ongoing tasks to complete even when another node takes over the lease.
			// This prevents tasks from aborting mid-process when lease ownership changes.
			Err(e) => {
				tx.cancel().await?;
				if matches!(
					e.downcast_ref::<Error>(),
					Some(Error::Kvs(KvsError::TransactionConditionNotMet))
				) {
					Ok(false)
				} else {
					Err(e)
				}
			}
		}
	}
}

#[cfg(test)]
#[cfg(any(feature = "kv-rocksdb", feature = "kv-mem"))]
mod tests {
	use std::sync::Arc;
	#[cfg(feature = "kv-mem")]
	use std::sync::atomic::Ordering;
	use std::time::{Duration, Instant};

	#[cfg(feature = "kv-mem")]
	use chrono::Utc;
	#[cfg(feature = "kv-rocksdb")]
	use temp_dir::TempDir;
	use tokio::sync::Notify;
	#[cfg(feature = "kv-mem")]
	use tokio::time::sleep;
	use uuid::Uuid;

	use crate::kvs::ds::{DatastoreFlavor, TransactionFactory};
	use crate::kvs::sequences::Sequences;
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
		sequences: Sequences,
		id: Uuid,
		tf: TransactionFactory,
		test_duration: Duration,
		lease_duration: Duration,
	) -> NodeResult {
		let lh =
			LeaseHandler::new(sequences, id, tf, TaskLeaseType::IndexCompaction, lease_duration)
				.unwrap();
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
		// Async event trigger
		let async_event_trigger = Arc::new(Notify::new());
		// Create a transaction factory with the specified datastore flavor
		let tf = TransactionFactory::new(async_event_trigger, Box::new(flavor));
		// Create a sequence generator for the transaction factory
		let sequences = Sequences::new(tf.clone(), Uuid::new_v4());
		// Set test to run for 3 seconds
		let test_duration = Duration::from_secs(3);
		// Set each lease to be valid for 1 second
		let lease_duration = Duration::from_secs(1);
		// Create a closure that generates a node task with a specific UUID
		let new_node = |n| {
			node_task_lease(
				sequences.clone(),
				Uuid::from_u128(n),
				tf.clone(),
				test_duration,
				lease_duration,
			)
		};

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
		// Create a new memory configuration
		let config = crate::kvs::config::MemoryConfig::default();
		// Create a new in-memory datastore
		let flavor =
			crate::kvs::mem::Datastore::new(config).await.map(DatastoreFlavor::Mem).unwrap();
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
		// Create a new RocksDB configuration
		let config = crate::kvs::config::RocksDbConfig::default();
		// Create a new RocksDB datastore in the temporary directory
		let flavor = crate::kvs::rocksdb::Datastore::new(&path, config)
			.await
			.map(DatastoreFlavor::RocksDB)
			.unwrap();
		// Run the concurrency test with the RocksDB datastore
		task_lease_concurrency(flavor).await;
	}

	/// Tests the task lease concurrency mechanism using a SurrealKV datastore.
	///
	/// This test creates a temporary SurrealKV datastore and runs the task lease
	/// concurrency test to verify that the lease mechanism works correctly in
	/// a multi-threaded environment with a persistent storage backend.
	///
	/// The test is only compiled and run when the "kv-surrealkv" feature is
	/// enabled.
	#[cfg(feature = "kv-surrealkv")]
	#[tokio::test(flavor = "multi_thread")]
	async fn task_lease_concurrency_surrealkv() {
		// Create a temporary directory for the SurrealKV datastore
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		// Create a new SurrealKV configuration
		let config = crate::kvs::config::SurrealKvConfig::default();
		// Create a new SurrealKV datastore
		let flavor = crate::kvs::surrealkv::Datastore::new(&path, config)
			.await
			.map(DatastoreFlavor::SurrealKV)
			.unwrap();
		// Run the concurrency test with the SurrealKV datastore
		task_lease_concurrency(flavor).await;
	}

	/// Tests the lease renewal behavior when a node already owns a lease.
	///
	/// This test verifies that:
	/// 1. A node that owns a lease doesn't try to re-acquire it if more than half the lease
	///    duration remains (status: Valid)
	/// 2. After waiting for more than half the lease duration, the lease enters the Renewable state
	///    and `check_lease()` triggers a renewal
	/// 3. After renewal, the lease is Valid again with a later expiration time
	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn test_lease_renewal_behavior() {
		// Create a new memory configuration
		let config = crate::kvs::config::MemoryConfig::default();
		// Create an in-memory datastore
		let flavor =
			crate::kvs::mem::Datastore::new(config).await.map(DatastoreFlavor::Mem).unwrap();
		// Create an async event trigger
		let async_event_trigger = Arc::new(Notify::new());
		// Create the transaction factory
		let tf = TransactionFactory::new(async_event_trigger, Box::new(flavor));
		let sequences = Sequences::new(tf.clone(), Uuid::new_v4());

		// Set lease duration to 10 seconds
		let lease_duration = Duration::from_secs(10);
		let node_id = Uuid::new_v4();

		// Create a lease handler
		let lh = LeaseHandler::new(
			sequences,
			node_id,
			tf,
			TaskLeaseType::IndexCompaction,
			lease_duration,
		)
		.unwrap();

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
		sleep(Duration::from_secs(7)).await;

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

	/// Tests that another node cannot acquire a lease while a valid lease is held.
	///
	/// This test verifies the mutual exclusion property of the lease system:
	/// 1. Node A acquires a lease successfully
	/// 2. Node B attempts to acquire the same lease type and is rejected
	/// 3. Node A can still confirm it holds the lease
	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn test_another_node_rejected_while_lease_valid() {
		let config = crate::kvs::config::MemoryConfig::default();
		let flavor =
			crate::kvs::mem::Datastore::new(config).await.map(DatastoreFlavor::Mem).unwrap();
		let async_event_trigger = Arc::new(Notify::new());
		let tf = TransactionFactory::new(async_event_trigger, Box::new(flavor));
		let sequences = Sequences::new(tf.clone(), Uuid::new_v4());

		let lease_duration = Duration::from_secs(60);
		let node_a = Uuid::from_u128(1);
		let node_b = Uuid::from_u128(2);

		// Node A acquires the lease
		let lh_a = LeaseHandler::new(
			sequences.clone(),
			node_a,
			tf.clone(),
			TaskLeaseType::IndexCompaction,
			lease_duration,
		)
		.unwrap();
		let acquired = lh_a.check_lease().await.unwrap();
		assert!(acquired, "Node A should acquire the lease");

		// Node B tries to acquire the same lease type
		let lh_b = LeaseHandler::new(
			sequences.clone(),
			node_b,
			tf.clone(),
			TaskLeaseType::IndexCompaction,
			lease_duration,
		)
		.unwrap();
		let acquired = lh_b.check_lease().await.unwrap();
		assert!(!acquired, "Node B should be rejected while Node A holds a valid lease");

		// Node A still holds the lease
		let acquired = lh_a.check_lease().await.unwrap();
		assert!(acquired, "Node A should still hold the lease");

		// Verify the lease is owned by Node A
		let (lease, status) = lh_a.read_lease(Utc::now()).await.unwrap().unwrap();
		assert_eq!(lease.owner, node_a);
		assert!(matches!(status, LeaseStatus::Valid), "Lease should be valid - {status:?}");
	}

	/// Tests that the minimum lease duration floor of 8 seconds is enforced.
	///
	/// When a very short lease duration (e.g., 1 second) is provided, the effective
	/// duration should be clamped to 8 seconds. This prevents excessive contention
	/// from misconfigured short leases.
	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn test_min_lease_duration_floor() {
		let config = crate::kvs::config::MemoryConfig::default();
		let flavor =
			crate::kvs::mem::Datastore::new(config).await.map(DatastoreFlavor::Mem).unwrap();
		let async_event_trigger = Arc::new(Notify::new());
		let tf = TransactionFactory::new(async_event_trigger, Box::new(flavor));
		let sequences = Sequences::new(tf.clone(), Uuid::new_v4());

		// Create a handler with a 1-second lease duration (below the 8-second floor)
		let lh = LeaseHandler::new(
			sequences,
			Uuid::new_v4(),
			tf,
			TaskLeaseType::IndexCompaction,
			Duration::from_secs(1),
		)
		.unwrap();

		// The effective lease duration should be clamped to 8 seconds
		assert_eq!(
			lh.lease_duration,
			chrono::Duration::seconds(8),
			"Lease duration should be clamped to the 8-second minimum"
		);

		// Acquire a lease and verify the expiration reflects the 8-second floor
		lh.check_lease().await.unwrap();
		let now = Utc::now();
		let (lease, _) = lh.read_lease(now).await.unwrap().unwrap();
		// The expiration should be approximately 8 seconds from when it was acquired
		let remaining = lease.expiration - now;
		assert!(
			remaining > chrono::Duration::seconds(6),
			"Lease expiration should reflect the 8-second floor, but remaining is {remaining}"
		);
	}

	/// Tests that different task types maintain independent leases.
	///
	/// Two handlers managing different task types should not interfere with each other.
	/// Both should be able to acquire leases simultaneously, even for the same node.
	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn test_different_task_types_are_independent() {
		let config = crate::kvs::config::MemoryConfig::default();
		let flavor =
			crate::kvs::mem::Datastore::new(config).await.map(DatastoreFlavor::Mem).unwrap();
		let async_event_trigger = Arc::new(Notify::new());
		let tf = TransactionFactory::new(async_event_trigger, Box::new(flavor));
		let sequences = Sequences::new(tf.clone(), Uuid::new_v4());

		let lease_duration = Duration::from_secs(60);
		let node_a = Uuid::from_u128(1);
		let node_b = Uuid::from_u128(2);

		// Node A acquires a lease for IndexCompaction
		let lh_a = LeaseHandler::new(
			sequences.clone(),
			node_a,
			tf.clone(),
			TaskLeaseType::IndexCompaction,
			lease_duration,
		)
		.unwrap();
		let acquired = lh_a.check_lease().await.unwrap();
		assert!(acquired, "Node A should acquire IndexCompaction lease");

		// Node B acquires a lease for ChangeFeedCleanup (different task type)
		let lh_b = LeaseHandler::new(
			sequences.clone(),
			node_b,
			tf.clone(),
			TaskLeaseType::ChangeFeedCleanup,
			lease_duration,
		)
		.unwrap();
		let acquired = lh_b.check_lease().await.unwrap();
		assert!(acquired, "Node B should acquire ChangeFeedCleanup lease independently");

		// Both leases should still be valid
		let (lease_a, _) = lh_a.read_lease(Utc::now()).await.unwrap().unwrap();
		let (lease_b, _) = lh_b.read_lease(Utc::now()).await.unwrap().unwrap();
		assert_eq!(lease_a.owner, node_a);
		assert_eq!(lease_b.owner, node_b);
	}

	/// Tests the throttling behavior of `try_maintain_lease`.
	///
	/// This test verifies that:
	/// 1. The first call to `try_maintain_lease` performs an actual lease check
	/// 2. Subsequent calls within the maintenance period are throttled and return `Ok(true)`
	///    without contacting the datastore
	/// 3. The `last_maintain_check` timestamp is updated on the first call but not on throttled
	///    calls
	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn test_try_maintain_lease_throttling() {
		let config = crate::kvs::config::MemoryConfig::default();
		let flavor =
			crate::kvs::mem::Datastore::new(config).await.map(DatastoreFlavor::Mem).unwrap();
		let async_event_trigger = Arc::new(Notify::new());
		let tf = TransactionFactory::new(async_event_trigger, Box::new(flavor));
		let sequences = Sequences::new(tf.clone(), Uuid::new_v4());

		// Use a 60-second lease so maintain_period = 60/8 = 7 seconds
		let lease_duration = Duration::from_secs(60);
		let node_id = Uuid::new_v4();

		let lh = LeaseHandler::new(
			sequences,
			node_id,
			tf,
			TaskLeaseType::IndexCompaction,
			lease_duration,
		)
		.unwrap();

		// First, acquire the lease
		let acquired = lh.check_lease().await.unwrap();
		assert!(acquired, "Should acquire the lease");

		// The last_maintain_check starts at 0 (never checked)
		// First call to try_maintain_lease should perform an actual check
		let result = lh.try_maintain_lease().await.unwrap();
		assert!(result, "First try_maintain_lease should succeed");

		// Record the timestamp after the first maintain call
		let first_check_ts = lh.last_maintain_check.load(Ordering::Relaxed);
		assert!(first_check_ts > 0, "last_maintain_check should be updated after first call");

		// Subsequent calls within the maintenance period should be throttled
		// (maintain_period = 7 seconds, and we're calling immediately)
		let result = lh.try_maintain_lease().await.unwrap();
		assert!(result, "Throttled try_maintain_lease should return Ok(true)");

		// The timestamp should not change on throttled calls
		let second_check_ts = lh.last_maintain_check.load(Ordering::Relaxed);
		assert_eq!(
			first_check_ts, second_check_ts,
			"last_maintain_check should not change on throttled calls"
		);

		// Call many times rapidly - all should be throttled and return Ok(true)
		for _ in 0..100 {
			let result = lh.try_maintain_lease().await.unwrap();
			assert!(result, "All throttled calls should return Ok(true)");
		}
	}

	/// Tests the full lease lifecycle including expiration and takeover by another node.
	///
	/// This test verifies that:
	/// 1. Node A acquires a lease
	/// 2. Node B is rejected while the lease is valid
	/// 3. After the lease expires, the status transitions to Expired
	/// 4. Node B can then successfully acquire the expired lease
	/// 5. Node A is now rejected when trying to re-acquire
	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn test_lease_expiration_and_takeover() {
		let config = crate::kvs::config::MemoryConfig::default();
		let flavor =
			crate::kvs::mem::Datastore::new(config).await.map(DatastoreFlavor::Mem).unwrap();
		let async_event_trigger = Arc::new(Notify::new());
		let tf = TransactionFactory::new(async_event_trigger, Box::new(flavor));
		let sequences = Sequences::new(tf.clone(), Uuid::new_v4());

		// Use the minimum lease duration (8 seconds due to the floor)
		let lease_duration = Duration::from_secs(1); // Will be clamped to 8 seconds
		let node_a = Uuid::from_u128(1);
		let node_b = Uuid::from_u128(2);

		let lh_a = LeaseHandler::new(
			sequences.clone(),
			node_a,
			tf.clone(),
			TaskLeaseType::IndexCompaction,
			lease_duration,
		)
		.unwrap();
		let lh_b = LeaseHandler::new(
			sequences.clone(),
			node_b,
			tf.clone(),
			TaskLeaseType::IndexCompaction,
			lease_duration,
		)
		.unwrap();

		// Node A acquires the lease
		let acquired = lh_a.check_lease().await.unwrap();
		assert!(acquired, "Node A should acquire the lease");

		// Node B is rejected
		let acquired = lh_b.check_lease().await.unwrap();
		assert!(!acquired, "Node B should be rejected while Node A holds the lease");

		// Wait for the lease to expire (8 seconds + small margin)
		sleep(Duration::from_secs(9)).await;

		// Verify the lease is now expired
		let (_, status) = lh_a.read_lease(Utc::now()).await.unwrap().unwrap();
		assert!(
			matches!(status, LeaseStatus::Expired),
			"Lease should be expired after waiting - {status:?}"
		);

		// Node B can now acquire the expired lease
		let acquired = lh_b.check_lease().await.unwrap();
		assert!(acquired, "Node B should acquire the expired lease");

		// Verify Node B now owns the lease
		let (lease, status) = lh_b.read_lease(Utc::now()).await.unwrap().unwrap();
		assert_eq!(lease.owner, node_b, "Node B should now own the lease");
		assert!(matches!(status, LeaseStatus::Valid), "New lease should be valid - {status:?}");

		// Node A should now be rejected
		let acquired = lh_a.check_lease().await.unwrap();
		assert!(!acquired, "Node A should be rejected now that Node B owns the lease");
	}

	/// Tests that `try_maintain_lease` returns `Ok(false)` when another node has taken the lease.
	///
	/// This verifies that the lease ownership status is correctly propagated through
	/// `try_maintain_lease` when an actual check is performed and the lease is no longer
	/// owned by the current node.
	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn test_try_maintain_lease_reports_lost_lease() {
		let config = crate::kvs::config::MemoryConfig::default();
		let flavor =
			crate::kvs::mem::Datastore::new(config).await.map(DatastoreFlavor::Mem).unwrap();
		let async_event_trigger = Arc::new(Notify::new());
		let tf = TransactionFactory::new(async_event_trigger, Box::new(flavor));
		let sequences = Sequences::new(tf.clone(), Uuid::new_v4());

		// Use minimum lease duration (clamped to 8 seconds)
		let lease_duration = Duration::from_secs(1);
		let node_a = Uuid::from_u128(1);
		let node_b = Uuid::from_u128(2);

		let lh_a = LeaseHandler::new(
			sequences.clone(),
			node_a,
			tf.clone(),
			TaskLeaseType::IndexCompaction,
			lease_duration,
		)
		.unwrap();
		let lh_b = LeaseHandler::new(
			sequences.clone(),
			node_b,
			tf.clone(),
			TaskLeaseType::IndexCompaction,
			lease_duration,
		)
		.unwrap();

		// Node A acquires the lease
		let acquired = lh_a.check_lease().await.unwrap();
		assert!(acquired, "Node A should acquire the lease");

		// Node A's try_maintain_lease should succeed
		let result = lh_a.try_maintain_lease().await.unwrap();
		assert!(result, "Node A should maintain the lease");

		// Wait for the lease to expire
		sleep(Duration::from_secs(9)).await;

		// Node B acquires the expired lease
		let acquired = lh_b.check_lease().await.unwrap();
		assert!(acquired, "Node B should acquire the expired lease");

		// Force Node A's throttle to allow a real check by advancing last_maintain_check
		// far into the past
		lh_a.last_maintain_check.store(0, Ordering::Relaxed);

		// Node A's try_maintain_lease should now report the lease is lost
		let result = lh_a.try_maintain_lease().await.unwrap();
		assert!(!result, "Node A should detect that it lost the lease");
	}

	/// Tests that a lease created with no prior state works correctly.
	///
	/// This verifies the `acquire_new_lease` path when no lease exists in the datastore
	/// (previous_lease is None), ensuring the `putc` conditional write handles the
	/// "key doesn't exist" case correctly.
	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn test_initial_lease_acquisition_from_empty_state() {
		let config = crate::kvs::config::MemoryConfig::default();
		let flavor =
			crate::kvs::mem::Datastore::new(config).await.map(DatastoreFlavor::Mem).unwrap();
		let async_event_trigger = Arc::new(Notify::new());
		let tf = TransactionFactory::new(async_event_trigger, Box::new(flavor));
		let sequences = Sequences::new(tf.clone(), Uuid::new_v4());

		let node_id = Uuid::new_v4();
		let lh = LeaseHandler::new(
			sequences.clone(),
			node_id,
			tf.clone(),
			TaskLeaseType::IndexCompaction,
			Duration::from_secs(60),
		)
		.unwrap();

		// Verify no lease exists initially
		let result = lh.read_lease(Utc::now()).await.unwrap();
		assert!(result.is_none(), "No lease should exist initially");

		// Acquire the lease from empty state
		let acquired = lh.check_lease().await.unwrap();
		assert!(acquired, "Should acquire lease from empty state");

		// Verify the lease now exists with correct owner
		let (lease, status) = lh.read_lease(Utc::now()).await.unwrap().unwrap();
		assert_eq!(lease.owner, node_id, "Lease owner should match the node");
		assert!(matches!(status, LeaseStatus::Valid), "New lease should be valid - {status:?}");

		// Verify has_lease also works from empty state (using a different task type)
		let lh2 = LeaseHandler::new(
			sequences,
			node_id,
			tf,
			TaskLeaseType::EventProcessing,
			Duration::from_secs(60),
		)
		.unwrap();
		let acquired = lh2.has_lease().await.unwrap();
		assert!(acquired, "has_lease should acquire from empty state");
	}
}
