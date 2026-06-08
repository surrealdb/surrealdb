use crate::cnf::Config;
use crate::kvs::config::{AolMode, SnapshotMode, SyncMode, parse_duration};

/// Configuration for the in-memory storage engine, parsed from query parameters.
#[derive(Debug, Clone)]
pub struct MemoryConfig {
	/// Whether MVCC versioning is enabled.
	pub versioned: bool,
	/// Version retention period in nanoseconds (0 = unlimited).
	pub retention_ns: u64,
	/// Path for persistence files (from URL path, e.g. `mem:///tmp/data`). If set, enables disk
	/// persistence.
	pub persist_path: Option<String>,
	/// Sync mode. Requires `persist_path`.
	pub sync_mode: SyncMode,
	/// AOL (Append-Only Log) mode. Requires `persist_path`.
	pub aol_mode: AolMode,
	/// Snapshot interval. Requires `persist_path`.
	pub snapshot_mode: SnapshotMode,
}

impl Default for MemoryConfig {
	fn default() -> Self {
		Self {
			versioned: false,
			retention_ns: 0,
			persist_path: None,
			sync_mode: SyncMode::Never,
			aol_mode: AolMode::Never,
			snapshot_mode: SnapshotMode::Never,
		}
	}
}

impl Config for MemoryConfig {
	fn parse(&mut self, map: &crate::cnf::ConfigMap) {
		map.parse_key_with("datastore_persist", &mut self.persist_path, |x| {
			let x = x.trim();
			if x.is_empty() {
				None
			} else {
				Some(Some(x.to_owned()))
			}
		})
		.parse_key_bool("datastore_versioned", &mut self.versioned)
		.parse_key_with("datastore_retention", &mut self.retention_ns, |x| {
			parse_duration(x).map(|x| x.as_nanos() as u64).ok()
		})
		.parse_key("datastore_aol", &mut self.aol_mode)
		.parse_key("datastore_snapshot", &mut self.snapshot_mode);

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
	use crate::kvs::config::{AolMode, SnapshotMode, SyncMode};
	use crate::kvs::mem::MemoryConfig;

	#[test]
	fn test_memory_config_defaults() {
		let map = ConfigMap::empty();
		let config = map.load::<MemoryConfig>();
		assert!(!config.versioned);
		assert_eq!(config.retention_ns, 0);
		assert!(config.persist_path.is_none());
		assert_eq!(config.aol_mode, AolMode::Never);
		assert_eq!(config.snapshot_mode, SnapshotMode::Never);
		assert_eq!(config.sync_mode, SyncMode::Never);
	}

	#[test]
	fn test_memory_config_with_persistence() {
		// Persist path comes from URL path (e.g. mem:///tmp/data), not query params
		let map = ConfigMap::from_config_string("versioned=true&aol=sync&snapshot=60s&sync=5s")
			.with_key_value("persist", "/tmp/data")
			.map_keys(|x| format!("datastore_{x}"));
		let config = map.load::<MemoryConfig>();
		assert!(config.versioned);
		assert_eq!(config.persist_path.as_deref(), Some("/tmp/data"));
		assert_eq!(config.aol_mode, AolMode::Sync);
		assert_eq!(config.snapshot_mode, SnapshotMode::Interval(Duration::from_secs(60)));
		assert_eq!(config.sync_mode, SyncMode::Interval(Duration::from_secs(5)));
	}
}
