use std::sync::atomic::{AtomicU8, Ordering};

use rocksdb::{Env, Options, SstFileManager};

use super::{TARGET, cnf};
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
/// This manager tracks SST (Sorted String Table) file space usage and implements an
/// application-level state machine to gracefully handle space constraints. Rather than
/// allowing RocksDB to abruptly fail writes when disk space is exhausted, this manager
/// provides gradual service degradation with clear state transitions.
///
/// ## State Machine
///
/// The manager transitions between two operational states based on SST file usage:
///
/// - **Normal**: All operations (reads, writes, deletions) are allowed
///   - Active when SST file usage is below 80% of the configured limit
///
/// - **ReadAndDeletionOnly**: Only reads and deletions are permitted
///   - Active when SST file usage reaches or exceeds 80% of the configured limit
///   - Write operations are blocked at the transaction level
///   - Deletions remain allowed to free up space and return to normal operation
///
/// ## Implementation Approach
///
/// Unlike RocksDB's built-in `set_max_allowed_space_usage` hard limit, which can block
/// writes during temporary size spikes from write buffering and pending compactions,
/// this manager:
///
/// - Monitors SST file sizes using RocksDB's `SstFileManager`
/// - Evaluates state transitions at transaction boundaries
/// - Allows deletions even when limits are exceeded
/// - Provides clear warnings when transitioning between states
///
/// ## Configuration
///
/// Space management is controlled via the `SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE`
/// environment variable (in bytes). When set to 0 (default), space management is disabled
/// and users must monitor disk usage manually.
///
/// When enabled, the manager also configures conservative limits for RocksDB metadata:
/// - Maximum manifest file size: 64 MiB
/// - Maximum log file size: 16 MiB
/// - Number of old log files retained: 3
pub(super) struct DiskSpaceManager {
	/// SST file manager for monitoring space usage
	sst_file_manager: SstFileManager,
	/// The latest state of the disk space manager
	latest_state: AtomicU8,
	/// The maximum space usage allowed for the database in bytes
	max_allowed_space_usage_limit: u64,
}

impl DiskSpaceManager {
	/// Pre-configure the disk space manager
	pub(super) fn configure(opts: &mut Options) -> Result<bool> {
		// Get the maximum allowed space usage in bytes
		let limit = *cnf::ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE;
		// Check if the maximum allowed space usage is configured
		if limit > 0 {
			// Disk space manager is enabled so we configure it
			// to monitor and manage disk space.
			info!(target: TARGET, "Disk space manager: enabled (limit={limit}B)");
			// Set the maximum size of the manifest file to 64 MiB
			opts.set_max_manifest_file_size(64 * 1024 * 1024);
			// Set the maximum size of each log file to 16 MiB
			opts.set_max_log_file_size(16 * 1024 * 1024);
			// Set the number of old log files to keep to 3
			opts.set_keep_log_file_num(3);
			// Continue
			Ok(true)
		} else {
			// Disk space manager is disabled so users must monitor
			// and manage disk space manually.
			info!(target: TARGET, "Disk space manager: disabled");
			// Continue
			Ok(false)
		}
	}
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
	pub(super) fn new(opts: &mut Options) -> Result<Self> {
		// Get the maximum allowed space usage in bytes
		let limit = *cnf::ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE;
		// Create a new environment
		let env = Env::new()?;
		// Create a new SST file manager
		let sst_file_manager = SstFileManager::new(&env)?;
		// Disable RocksDB's built-in hard limit (set to 0 = unlimited).
		// This prevents RocksDB from blocking writes due to temporary size spikes from
		// write buffering and pending compactions. Instead, the application manages space
		// restrictions at the transaction level through state transitions, providing more
		// graceful handling and allowing deletions to free space.
		sst_file_manager.set_max_allowed_space_usage(0);
		// Set the SST file manager in the options
		opts.set_sst_file_manager(&sst_file_manager);
		// Create a new disk space manager
		Ok(Self {
			sst_file_manager,
			latest_state: AtomicU8::new(DiskSpaceState::Normal as u8),
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
