use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use rocksdb::{Env, Options, SstFileManager};

use super::TARGET;
use crate::kvs::Result;

const MAX_PERCENTAGE_USAGE: u8 = 80;

/// The state of the disk space manager.
#[derive(Default, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub(super) enum DiskSpaceState {
	/// The datastore is in normal operation.
	#[default]
	Normal,
	/// The datastore is in read-and-deletion-only mode.
	ReadAndDeletionOnly,
}

/// Tracks the types of write operations performed in a transaction.
#[derive(Default, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub(super) enum TransactionState {
	/// The transaction contains only reads.
	#[default]
	ReadsOnly,
	/// The transaction contains only reads, or deletes.
	HasDeletes,
	/// The transaction contains reads, deletes, or writes.
	HasWrites,
}

/// Manages disk space monitoring and enforces space limits for the RocksDB datastore.
///
/// This manager tracks SST file space usage and implements a state machine to transition
/// the datastore between normal operation and read-and-deletion-only mode based on
/// configured space limits. It provides gradual degradation of service rather than
/// abrupt failures when disk space is constrained.
#[derive(Clone)]
pub(super) struct DiskSpaceManager {
	/// SST file manager for monitoring space usage
	sst_file_manager: Arc<SstFileManager>,
	/// The latest state of the disk space manager
	latest_state: Arc<AtomicU8>,
	/// The maximum space usage allowed for the database in bytes
	max_allowed_space_usage_limit: u64,
}

impl DiskSpaceManager {
	/// Creates a new disk space manager with the specified space limit.
	///
	/// # Parameters
	/// - `limit`: The maximum allowed SST file space usage in bytes
	/// - `opts`: RocksDB options to configure with the SST file manager
	///
	/// # Implementation Details
	/// This method disables RocksDB's built-in hard limit enforcement and instead
	/// implements application-level space management at the transaction level.
	/// This approach provides more graceful degradation and allows deletions to
	/// free space even when the limit is reached.
	pub(super) fn new(opts: &mut Options, limit: u64) -> Result<Self> {
		let env = Env::new()?;
		let sst_file_manager = SstFileManager::new(&env)?;
		// Disable RocksDB's built-in hard limit (set to 0 = unlimited).
		// This prevents RocksDB from blocking writes due to temporary size spikes from
		// write buffering and pending compactions. Instead, the application manages space
		// restrictions at the transaction level through state transitions, providing more
		// graceful handling and allowing deletions to free space.
		sst_file_manager.set_max_allowed_space_usage(0);
		opts.set_sst_file_manager(&sst_file_manager);
		Ok(Self {
			sst_file_manager: Arc::new(sst_file_manager),
			latest_state: Arc::new(AtomicU8::new(DiskSpaceState::Normal as u8)),
			max_allowed_space_usage_limit: limit,
		})
	}

	/// Returns the current data usage as a percentage of the allowed space usage.
	pub(super) fn usage(&self) -> u8 {
		// Get the current total size of the SST files
		let current_size = self.sst_file_manager.get_total_size();
		// Get the maximum allowed space usage in bytes
		let allowed_size = self.max_allowed_space_usage_limit;
		// Calculate the usage as a percentage
		((current_size as f64 / allowed_size as f64) * 100.0).round() as u8
	}

	/// Returns the cached state of the disk space manager.
	pub(super) fn cached_state(&self) -> DiskSpaceState {
		match self.latest_state.load(Ordering::Acquire) {
			0 => DiskSpaceState::Normal,
			1 => DiskSpaceState::ReadAndDeletionOnly,
			_ => unreachable!(),
		}
	}

	/// Returns the current state of the disk space manager.
	pub(super) fn latest_state(&self) -> DiskSpaceState {
		// Get the maximum allowed space usage limit
		let limit = self.max_allowed_space_usage_limit;
		// Get the current usage as a percentage
		match self.usage() < MAX_PERCENTAGE_USAGE {
			true => {
				// The new state
				let state = DiskSpaceState::Normal;
				// Get the latest state
				let latest_state = self.cached_state();
				// If the latest state is not normal, log a warning
				if latest_state != state {
					self.latest_state.store(state as u8, Ordering::Release);
					warn!(target: TARGET, "SST file space is below the {MAX_PERCENTAGE_USAGE}% usage threshold of the {limit} byte limit");
					warn!(target: TARGET, "Transitioning to normal disk mode due to disk space limit being within the threshold");
				}
				// Disk space state is normal
				DiskSpaceState::Normal
			}
			false => {
				// The new state
				let state = DiskSpaceState::ReadAndDeletionOnly;
				// Get the latest state
				let latest_state = self.cached_state();
				// If the latest state is not restricted, log a warning
				if latest_state != state {
					self.latest_state.store(state as u8, Ordering::Release);
					warn!(target: TARGET, "SST file space is above the {MAX_PERCENTAGE_USAGE}% usage threshold of the {limit} byte limit");
					warn!(target: TARGET, "Transitioning to read-and-deletion-only mode due to disk space limit being reached");
				}
				// Disk space state is restricted
				DiskSpaceState::ReadAndDeletionOnly
			}
		}
	}
}
