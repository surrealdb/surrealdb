use std::time::Duration;

use crate::cnf::Config;
use crate::kvs::config::{SyncMode, parse_duration};
use crate::sys::TOTAL_SYSTEM_MEMORY;

/// Configuration for the SurrealKV storage engine, parsed from query parameters.
#[derive(Debug, Clone)]
pub struct SurrealKvConfig {
	/// Whether MVCC versioning is enabled.
	pub versioned: bool,
	/// Version retention period in nanoseconds (0 = unlimited).
	pub retention: Duration,
	/// Disk sync mode.
	pub sync_mode: SyncMode,
	/// Whether to enable value log separation (default: true)
	pub enable_vlog: bool,
	/// Whether to enable versioned index (default: false, only applies when versioning is enabled)
	pub versioned_index: bool,
	/// The block size in bytes (default: 64 KiB)
	pub block_size: usize,
	/// The maximum value log file size in bytes (default: dynamic from 64 MiB to 512 MiB)
	pub vlog_max_file_size: u64,
	/// The value log threshold in bytes - values larger than this are stored in the value log
	/// (default: 4 KiB)
	pub vlog_threshold: usize,
	/// The block cache capacity in bytes (default: dynamic based on memory)
	pub block_cache_capacity: u64,
	/// The maximum wait time in nanoseconds before forcing a grouped commit (default: 5ms).
	/// This timeout ensures that transactions don't wait indefinitely under low concurrency and
	/// balances commit latency against write throughput.
	pub grouped_commit_timeout: u64,
	/// Threshold for deciding whether to wait for more transactions (default: 12)
	/// If the current batch size is greater or equal to this threshold (and below
	/// SURREALKV_GROUPED_COMMIT_MAX_BATCH_SIZE), then the coordinator will wait up to
	/// SURREALKV_GROUPED_COMMIT_TIMEOUT to collect more transactions. Smaller batches are flushed
	/// immediately to preserve low latency.
	pub grouped_commit_wait_threshold: usize,
	/// The maximum number of transactions in a single grouped commit batch (default: 4096)
	/// This prevents unbounded memory growth while still allowing large batches for efficiency.
	/// Larger batches improve throughput but increase memory usage and commit latency.
	pub grouped_commit_max_batch_size: usize,
	/// The maximum memtable size in bytes before flushing to disk
	/// This is the arena of memory that is used to store the memtable data.
	/// If a single transaction is larger than this size, it will throw an error.
	/// If a single transaction needs to store larger than this size, this value should be
	/// increased.
	pub max_memtable_size: usize,
}

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;

fn default_log_file_size() -> u64 {
	let mem = *TOTAL_SYSTEM_MEMORY;
	if mem < 4 * GIB {
		64 * MIB
	} else if mem < 16 * GIB {
		128 * MIB
	} else if mem < 64 * GIB {
		256 * MIB
	} else {
		512 * MIB
	}
}

fn default_block_cache_capacity() -> u64 {
	let mem = *TOTAL_SYSTEM_MEMORY;
	(mem / 2).saturating_sub(GIB).max(16 * MIB)
}

fn default_max_memtable_size() -> usize {
	let mem = *TOTAL_SYSTEM_MEMORY;
	if mem < GIB {
		64 * MIB as usize
	} else if mem < 4 * GIB {
		128 * MIB as usize
	} else if mem < 16 * GIB {
		256 * MIB as usize
	} else if mem < 64 * GIB {
		GIB as usize
	} else {
		4 * GIB as usize
	}
}

impl Default for SurrealKvConfig {
	fn default() -> Self {
		Self {
			versioned: false,
			retention: Duration::ZERO,
			sync_mode: SyncMode::Every,
			enable_vlog: true,
			versioned_index: false,
			block_size: (64 * KIB) as usize,
			vlog_max_file_size: default_log_file_size(),
			vlog_threshold: (4 * KIB) as usize,
			block_cache_capacity: default_block_cache_capacity(),
			grouped_commit_timeout: Duration::from_millis(5).as_nanos() as u64,
			grouped_commit_wait_threshold: 12,
			grouped_commit_max_batch_size: 4096,
			max_memtable_size: default_max_memtable_size(),
		}
	}
}

impl Config for SurrealKvConfig {
	fn parse(&mut self, map: &crate::cnf::ConfigMap) {
		map.parse_key("datastore_versioned", &mut self.versioned)
			.parse_key_with("datastore_retention", &mut self.retention, |x| parse_duration(x).ok())
			.parse_key("surrealkv_enable_vlog", &mut self.enable_vlog)
			.parse_key("surrealkv_versioned_index", &mut self.versioned_index)
			.parse_key("surrealkv_block_size", &mut self.block_size)
			.parse_key("surrealkv_vlog_max_file_size", &mut self.vlog_max_file_size)
			.parse_key("surrealkv_vlog_threshold", &mut self.vlog_threshold)
			.parse_key("surrealkv_block_cache_capacity", &mut self.block_cache_capacity)
			.parse_key("surrealkv_grouped_commit_timeout", &mut self.grouped_commit_timeout)
			.parse_key(
				"surrealkv_grouped_commit_wait_threshold",
				&mut self.grouped_commit_wait_threshold,
			)
			.parse_key(
				"surrealkv_grouped_commit_max_batch_size",
				&mut self.grouped_commit_max_batch_size,
			)
			.parse_key("surrealkv_max_memtable_size", &mut self.max_memtable_size);

		if map.has_key("datastore_sync") {
			map.parse_key("datastore_sync", &mut self.sync_mode);
		} else {
			map.parse_key("datastore_sync_data", &mut self.sync_mode);
		}
	}
}

#[cfg(test)]
mod test {
	use std::time::Duration;

	use crate::cnf::ConfigMap;
	use crate::kvs::config::SyncMode;
	use crate::kvs::surrealkv::cnf::SurrealKvConfig;

	#[test]
	fn test_surrealkv_config_defaults() {
		let map = ConfigMap::empty();

		let config = map.load::<SurrealKvConfig>();
		assert!(!config.versioned);
		assert_eq!(config.retention, Duration::ZERO);
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	#[test]
	fn test_surrealkv_config_from_params() {
		let map = ConfigMap::from_config_string("versioned=true&retention=30d&sync=every");
		let config = map.load::<SurrealKvConfig>();
		assert!(config.versioned);
		assert_eq!(config.retention, Duration::from_secs(30 * 24 * 60 * 60));
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	#[test]
	fn test_surrealkv_config_interval_sync() {
		let map = ConfigMap::from_config_string("sync=5s");
		let config = map.load::<SurrealKvConfig>();
		assert_eq!(config.sync_mode, SyncMode::Interval(Duration::from_secs(5)));
	}
}
