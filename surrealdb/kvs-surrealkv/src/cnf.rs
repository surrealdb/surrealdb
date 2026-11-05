use std::time::Duration;

use common::sys::TOTAL_SYSTEM_MEMORY;
use surrealdb_cnf::{Config, parse_duration};
use surrealdb_kvs::config::SyncMode;
use tracing::warn;

/// Configuration for the SurrealKV storage engine, parsed from query parameters.
#[derive(Debug, Clone)]
pub struct SurrealKvConfig {
	/// Whether MVCC versioning is enabled.
	pub versioned: bool,
	/// Version retention period in nanoseconds (0 = unlimited).
	pub retention: Duration,
	/// Disk sync mode. The default is `every` on native targets, and a one-second
	/// interval in the browser (`wasm32`), where `every` would wait on each commit's flush.
	pub sync_mode: SyncMode,
	/// Whether to enable value log separation (default: true)
	pub enable_vlog: bool,
	/// The block size in bytes (default: 64 KiB)
	pub block_size: usize,
	/// The maximum value log file size in bytes (default: dynamic from 64 MiB to 512 MiB)
	pub vlog_max_file_size: u64,
	/// The value log threshold in bytes - values larger than this are stored in the value log
	/// (default: 4 KiB)
	pub vlog_threshold: usize,
	/// The block cache capacity in bytes. The default is half of the system memory less 1 GiB,
	/// and at least 16 MiB: about 30 GiB on a 62 GiB host, as for MongoDB's WiredTiger cache.
	/// The cache holds blocks of live tables only: a compaction neither fills it nor leaves the
	/// blocks of the tables it merged in it.
	pub block_cache_capacity: u64,
	/// The maximum memtable size in bytes before flushing to disk.
	/// This is the arena of memory that holds recent writes. It bounds memtable residency, not
	/// transaction size: a commit too large for the memtable is written directly to a level-0
	/// table instead.
	pub max_memtable_size: usize,
	/// Whether a directory a 3.x release wrote, as RocksDB or as SurrealKV v0.x, is converted in
	/// place when it is opened (default: false, which refuses to open it). The datastore reads the
	/// same key to decide whether to upgrade the data such a directory holds.
	pub experimental_upgrade_from_3x: bool,
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
		// Falls back to 1 GiB on a target whose address space is smaller than 4 GiB
		usize::try_from(4 * GIB).unwrap_or(GIB as usize)
	}
}

/// The sync mode of a target. On native a commit is durable before it returns. In the browser
/// that flush would wait on the origin private file system for every commit, so the browser
/// flushes the log once a second instead.
fn default_sync_mode_for(wasm: bool) -> SyncMode {
	if wasm {
		SyncMode::Interval(Duration::from_secs(1))
	} else {
		SyncMode::Every
	}
}

/// The sync mode when none is set, for the target this is compiled for.
fn default_sync_mode() -> SyncMode {
	default_sync_mode_for(cfg!(target_family = "wasm"))
}

impl Default for SurrealKvConfig {
	fn default() -> Self {
		Self {
			versioned: false,
			retention: Duration::ZERO,
			sync_mode: default_sync_mode(),
			enable_vlog: true,
			block_size: (64 * KIB) as usize,
			vlog_max_file_size: default_log_file_size(),
			vlog_threshold: (4 * KIB) as usize,
			block_cache_capacity: default_block_cache_capacity(),
			max_memtable_size: default_max_memtable_size(),
			experimental_upgrade_from_3x: false,
		}
	}
}

/// The settings of the removed RocksDB engine. Nothing reads them, so an
/// operator who still sets one is told.
const REMOVED_ROCKSDB_KEYS: &[&str] = &[
	"rocksdb_blob_compaction_readahead_size",
	"rocksdb_blob_compression_type",
	"rocksdb_blob_file_size",
	"rocksdb_blob_gc_age_cutoff",
	"rocksdb_blob_gc_force_threshold",
	"rocksdb_block_cache_size",
	"rocksdb_block_size",
	"rocksdb_compact_on_shutdown",
	"rocksdb_compaction_readahead_size",
	"rocksdb_compaction_style",
	"rocksdb_deletion_factory_delete_count",
	"rocksdb_deletion_factory_ratio",
	"rocksdb_deletion_factory_window_size",
	"rocksdb_enable_blob_files",
	"rocksdb_enable_blob_gc",
	"rocksdb_enable_pipelined_writes",
	"rocksdb_file_compaction_trigger",
	"rocksdb_file_reads_for_auto_readahead",
	"rocksdb_grouped_commit_max_batch_size",
	"rocksdb_grouped_commit_timeout",
	"rocksdb_grouped_commit_wait_threshold",
	"rocksdb_initial_auto_readahead_size",
	"rocksdb_jobs_count",
	"rocksdb_keep_log_file_num",
	"rocksdb_level0_slowdown_writes_trigger",
	"rocksdb_level0_stop_writes_trigger",
	"rocksdb_max_auto_readahead_size",
	"rocksdb_max_concurrent_subcompactions",
	"rocksdb_max_open_files",
	"rocksdb_max_write_buffer_number",
	"rocksdb_memtable_prefix_bloom_ratio",
	"rocksdb_min_blob_size",
	"rocksdb_min_write_buffer_number_to_merge",
	"rocksdb_periodic_compaction_seconds",
	"rocksdb_prefix_extractor_enabled",
	"rocksdb_runtime_reserve",
	"rocksdb_scan_verify_checksums",
	"rocksdb_shutdown_wait_for_compact_seconds",
	"rocksdb_sst_max_allowed_space_usage",
	"rocksdb_storage_log_level",
	"rocksdb_target_file_size_base",
	"rocksdb_target_file_size_multiplier",
	"rocksdb_thread_count",
	"rocksdb_universal_compression_size_percent",
	"rocksdb_universal_max_merge_width",
	"rocksdb_universal_max_size_amplification_percent",
	"rocksdb_universal_min_merge_width",
	"rocksdb_universal_size_ratio",
	"rocksdb_universal_stop_style",
	"rocksdb_wal_size_limit",
	"rocksdb_whole_key_filtering",
	"rocksdb_write_buffer_size",
];

impl Config for SurrealKvConfig {
	fn parse(&mut self, map: &surrealdb_cnf::ConfigMap) {
		map.parse_key("datastore_versioned", &mut self.versioned)
			.parse_key_with("datastore_retention", &mut self.retention, |x| parse_duration(x).ok())
			.parse_key("surrealkv_enable_vlog", &mut self.enable_vlog)
			.parse_key("surrealkv_block_size", &mut self.block_size)
			.parse_key("surrealkv_vlog_max_file_size", &mut self.vlog_max_file_size)
			.parse_key("surrealkv_vlog_threshold", &mut self.vlog_threshold)
			.parse_key("surrealkv_block_cache_capacity", &mut self.block_cache_capacity)
			.parse_key("surrealkv_max_memtable_size", &mut self.max_memtable_size)
			.parse_key_bool("experimental_upgrade_from_3x", &mut self.experimental_upgrade_from_3x);

		if map.has_key("datastore_sync") {
			map.parse_key("datastore_sync", &mut self.sync_mode);
		} else {
			map.parse_key("datastore_sync_data", &mut self.sync_mode);
		}

		let removed: Vec<&str> =
			REMOVED_ROCKSDB_KEYS.iter().copied().filter(|key| map.has_key(key)).collect();
		if !removed.is_empty() {
			warn!(
				target: "surrealdb::core::kvs::surrealkv",
				"The RocksDB engine has been removed and its settings are ignored: {}. \
				 The SurrealKV equivalents are surrealkv_block_cache_capacity, \
				 surrealkv_max_memtable_size, surrealkv_block_size and datastore_sync",
				removed.join(", ")
			);
		}

		// The engine has no separate index of versions, so the setting has no effect. It is
		// ignored, as any other key which nothing reads is, and an operator who still sets it
		// is told.
		if map.has_key("surrealkv_versioned_index") {
			warn!(
				target: "surrealdb::core::kvs::surrealkv",
				"The surrealkv_versioned_index setting has no effect and is ignored"
			);
		}
		// SurrealKV groups the syncs of concurrent commits itself, so the grouped commit
		// settings have no effect and are ignored the same way.
		for key in [
			"surrealkv_grouped_commit_timeout",
			"surrealkv_grouped_commit_wait_threshold",
			"surrealkv_grouped_commit_max_batch_size",
		] {
			if map.has_key(key) {
				warn!(
					target: "surrealdb::core::kvs::surrealkv",
					"The {key} setting has no effect and is ignored"
				);
			}
		}
	}
}

#[cfg(test)]
mod test {
	use std::time::Duration;

	use surrealdb_cnf::{Config, ConfigMap};
	use surrealdb_kvs::config::SyncMode;

	use crate::cnf::{SurrealKvConfig, default_sync_mode, default_sync_mode_for};

	#[test]
	#[cfg(not(target_family = "wasm"))]
	fn test_surrealkv_config_defaults() {
		let map = ConfigMap::empty();

		let config = map.load::<SurrealKvConfig>();
		assert!(!config.versioned);
		assert_eq!(config.retention, Duration::ZERO);
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	/// Compiled only for the browser: the default there is a one-second interval, so that no
	/// commit waits for a durable flush on the origin private file system.
	#[test]
	#[cfg(target_family = "wasm")]
	fn test_surrealkv_config_defaults_wasm() {
		let map = ConfigMap::empty();

		let config = map.load::<SurrealKvConfig>();
		assert!(!config.versioned);
		assert_eq!(config.retention, Duration::ZERO);
		assert_eq!(config.sync_mode, SyncMode::Interval(Duration::from_secs(1)));
	}

	/// Runs on every target: the default for each target is chosen by a helper that takes the
	/// target as an argument, so the browser's default is checked on the native test run too.
	#[test]
	fn test_default_sync_mode_by_target() {
		assert_eq!(default_sync_mode_for(false), SyncMode::Every);
		assert_eq!(default_sync_mode_for(true), SyncMode::Interval(Duration::from_secs(1)));
		assert_eq!(
			default_sync_mode(),
			default_sync_mode_for(cfg!(target_family = "wasm")),
			"the default of this target must be the one for its target family"
		);
		assert_eq!(SurrealKvConfig::default().sync_mode, default_sync_mode());
	}

	/// An explicit `sync=every` wins over a default that is not `every`, as the browser's is.
	#[test]
	fn test_explicit_sync_overrides_default() {
		let mut config = SurrealKvConfig {
			sync_mode: SyncMode::Interval(Duration::from_secs(1)),
			..SurrealKvConfig::default()
		};
		let map =
			ConfigMap::from_config_string("sync=every").map_keys(|x| format!("datastore_{x}"));
		config.parse(&map);
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	#[test]
	fn test_surrealkv_config_from_params() {
		let map = ConfigMap::from_config_string("versioned=true&retention=30d&sync=every")
			.map_keys(|x| format!("datastore_{x}"));
		let config = map.load::<SurrealKvConfig>();
		assert!(config.versioned);
		assert_eq!(config.retention, Duration::from_secs(30 * 24 * 60 * 60));
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	#[test]
	fn test_surrealkv_config_ignores_versioned_index() {
		let map = ConfigMap::empty()
			.with_key_value("surrealkv_versioned_index", "true")
			.with_key_value("datastore_versioned", "true");
		let config = map.load::<SurrealKvConfig>();
		assert!(config.versioned);
		assert_eq!(config.sync_mode, default_sync_mode());
	}

	#[test]
	fn test_surrealkv_config_interval_sync() {
		let map = ConfigMap::from_config_string("sync=5s").map_keys(|x| format!("datastore_{x}"));
		let config = map.load::<SurrealKvConfig>();
		assert_eq!(config.sync_mode, SyncMode::Interval(Duration::from_secs(5)));
	}
}
