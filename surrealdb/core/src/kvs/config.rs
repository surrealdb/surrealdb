use std::collections::HashMap;
use std::fmt;
use std::sync::LazyLock;
use std::time::Duration;

use super::err::{Error, Result};

// --------------------------------------------------
// Environment variable fallbacks
// --------------------------------------------------

/// The sync mode for the datastore (default: 'every').
/// This is an alternative to the `sync` query parameter.
/// Only used by the 'memory', 'rocksdb', and 'surrealkv' engines.
/// Accepts: "never", "every", or a duration string (e.g. "5s", "1m").
static SURREAL_DATASTORE_SYNC_DATA: LazyLock<Option<String>> =
	lazy_env_parse!("SURREAL_DATASTORE_SYNC_DATA", Option<String>);

/// Whether MVCC versioning is enabled (default: false).
/// This is an alternative to the `versioned` query parameter.
/// Only used by the 'memory' and 'surrealkv' engines.
/// Accepts: "true", "false", "1", "0".
static SURREAL_DATASTORE_VERSIONED: LazyLock<Option<String>> =
	lazy_env_parse!("SURREAL_DATASTORE_VERSIONED", Option<String>);

/// Version retention period as a duration string (default: 0 / unlimited).
/// This is an alternative to the `retention` query parameter.
/// Only used by the 'memory' and 'surrealkv' engines.
/// Accepts: a duration string (e.g. "30d", "24h").
static SURREAL_DATASTORE_RETENTION: LazyLock<Option<String>> =
	lazy_env_parse!("SURREAL_DATASTORE_RETENTION", Option<String>);

/// Filesystem path for persistence ('memory' engine only).
/// This is an alternative to the `persist` query parameter.
/// Only used by the 'memory' engine.
/// Accepts: a filesystem path.
static SURREAL_DATASTORE_PERSIST: LazyLock<Option<String>> =
	lazy_env_parse!("SURREAL_DATASTORE_PERSIST", Option<String>);

/// Append-only log mode ('memory' engine only).
/// This is an alternative to the `aol` query parameter.
/// Only used by the 'memory' engine.
/// Accepts: "never", "sync", "async".
static SURREAL_DATASTORE_AOL: LazyLock<Option<String>> =
	lazy_env_parse!("SURREAL_DATASTORE_AOL", Option<String>);

/// Snapshot interval ('memory' engine only).
/// This is an alternative to the `snapshot` query parameter.
/// Only used by the 'memory' engine.
/// Accepts: "never" or a duration string (e.g. "60s", "5m").
static SURREAL_DATASTORE_SNAPSHOT: LazyLock<Option<String>> =
	lazy_env_parse!("SURREAL_DATASTORE_SNAPSHOT", Option<String>);

// --------------------------------------------------
// Query parameter parsing helpers
// --------------------------------------------------

/// Parse a query string (e.g. "versioned=true&sync=every") into key-value pairs.
pub fn parse_query_params(query: &str) -> HashMap<String, String> {
	query
		.split('&')
		.filter(|s| !s.is_empty())
		.filter_map(|pair| {
			let (k, v) = pair.split_once('=')?;
			Some((k.to_lowercase(), v.to_string()))
		})
		.collect()
}

// --------------------------------------------------
// SurrealMX configuration
// --------------------------------------------------

/// Configuration for the in-memory storage engine, parsed from query parameters.
#[derive(Debug, Clone)]
pub struct MemoryConfig {
	/// Whether MVCC versioning is enabled.
	pub versioned: bool,
	/// Version retention period in nanoseconds (0 = unlimited).
	pub retention_ns: u64,
	/// Path for persistence files. If set, enables disk persistence.
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

impl MemoryConfig {
	/// Build configuration from parsed query parameters, with environment
	/// variable fallbacks. Query parameters take precedence over env vars,
	/// which take precedence over engine defaults.
	pub fn from_params(params: &HashMap<String, String>) -> Result<Self> {
		let mut config = Self::default();
		// Check whether versioning is enabled (query param > env var > default)
		if let Some(v) = params.get("versioned") {
			config.versioned = v.eq_ignore_ascii_case("true") || v == "1";
		} else if let Some(v) = SURREAL_DATASTORE_VERSIONED.as_deref() {
			config.versioned = v.eq_ignore_ascii_case("true") || v == "1";
		}
		// Determine the version retention period (query param > env var > default)
		if let Some(v) = params.get("retention") {
			let dur = parse_duration(v)?;
			config.retention_ns = dur.as_nanos() as u64;
		} else if let Some(v) = SURREAL_DATASTORE_RETENTION.as_deref() {
			let dur = parse_duration(v)?;
			config.retention_ns = dur.as_nanos() as u64;
		}
		// Determine whether persistence is enabled (query param > env var > default)
		if let Some(v) = params.get("persist") {
			config.persist_path = Some(v.clone());
		} else if let Some(v) = SURREAL_DATASTORE_PERSIST.as_deref() {
			config.persist_path = Some(v.to_string());
		}
		// Determine the append-only-log mode (query param > env var > default)
		if let Some(v) = params.get("aol") {
			config.aol_mode = parse_aol_mode(v)?;
		} else if let Some(v) = SURREAL_DATASTORE_AOL.as_deref() {
			config.aol_mode = parse_aol_mode(v)?;
		}
		// Determine the snapshot mode (query param > env var > default)
		if let Some(v) = params.get("snapshot") {
			config.snapshot_mode = parse_snapshot_mode(v)?;
		} else if let Some(v) = SURREAL_DATASTORE_SNAPSHOT.as_deref() {
			config.snapshot_mode = parse_snapshot_mode(v)?;
		}
		// Determine the sync mode (query param > env var > default)
		if let Some(v) = params.get("sync") {
			config.sync_mode = parse_sync_mode(v)?;
		} else if let Some(v) = SURREAL_DATASTORE_SYNC_DATA.as_deref() {
			config.sync_mode = parse_sync_mode(v)?;
		}
		// Validate: aol, snapshot, and sync require persist
		if config.persist_path.is_none() {
			if config.sync_mode != SyncMode::Never {
				return Err(Error::Datastore(
					"The 'sync' option requires 'persist' to be set".to_string(),
				));
			}
			if config.aol_mode != AolMode::Never {
				return Err(Error::Datastore(
					"The 'aol' option requires 'persist' to be set".to_string(),
				));
			}
			if config.snapshot_mode != SnapshotMode::Never {
				return Err(Error::Datastore(
					"The 'snapshot' option requires 'persist' to be set".to_string(),
				));
			}
		}
		// Return the configuration
		Ok(config)
	}
}

// --------------------------------------------------
// SurrealKV configuration
// --------------------------------------------------

/// Configuration for the SurrealKV storage engine, parsed from query parameters.
#[derive(Debug, Clone)]
pub struct SurrealKvConfig {
	/// Whether MVCC versioning is enabled.
	pub versioned: bool,
	/// Version retention period in nanoseconds (0 = unlimited).
	pub retention_ns: u64,
	/// Disk sync mode.
	pub sync_mode: SyncMode,
}

impl Default for SurrealKvConfig {
	fn default() -> Self {
		Self {
			versioned: false,
			retention_ns: 0,
			sync_mode: SyncMode::Every,
		}
	}
}

impl SurrealKvConfig {
	/// Build configuration from parsed query parameters, with environment
	/// variable fallbacks. Query parameters take precedence over env vars,
	/// which take precedence over engine defaults.
	pub fn from_params(params: &HashMap<String, String>) -> Result<Self> {
		let mut config = Self::default();
		// Check whether versioning is enabled (query param > env var > default)
		if let Some(v) = params.get("versioned") {
			config.versioned = v.eq_ignore_ascii_case("true") || v == "1";
		} else if let Some(v) = SURREAL_DATASTORE_VERSIONED.as_deref() {
			config.versioned = v.eq_ignore_ascii_case("true") || v == "1";
		}
		// Determine the version retention period (query param > env var > default)
		if let Some(v) = params.get("retention") {
			let dur = parse_duration(v)?;
			config.retention_ns = dur.as_nanos() as u64;
		} else if let Some(v) = SURREAL_DATASTORE_RETENTION.as_deref() {
			let dur = parse_duration(v)?;
			config.retention_ns = dur.as_nanos() as u64;
		}
		// Determine the sync mode (query param > env var > default)
		if let Some(v) = params.get("sync") {
			config.sync_mode = parse_sync_mode(v)?;
		} else if let Some(v) = SURREAL_DATASTORE_SYNC_DATA.as_deref() {
			config.sync_mode = parse_sync_mode(v)?;
		}
		// Return the configuration
		Ok(config)
	}
}

// --------------------------------------------------
// RocksDB configuration
// --------------------------------------------------

/// Configuration for the RocksDB storage engine, parsed from query parameters.
#[derive(Debug, Clone)]
pub struct RocksDbConfig {
	/// Whether MVCC versioning is enabled.
	pub versioned: bool,
	/// Version retention period in nanoseconds (0 = unlimited).
	pub retention_ns: u64,
	/// Disk sync mode.
	pub sync_mode: SyncMode,
}

impl Default for RocksDbConfig {
	fn default() -> Self {
		Self {
			versioned: false,
			retention_ns: 0,
			sync_mode: SyncMode::Every,
		}
	}
}

impl RocksDbConfig {
	/// Build configuration from parsed query parameters, with environment
	/// variable fallbacks. Query parameters take precedence over env vars,
	/// which take precedence over engine defaults.
	pub fn from_params(params: &HashMap<String, String>) -> Result<Self> {
		let mut config = Self::default();
		// Check whether versioning is enabled (query param > env var > default)
		if let Some(v) = params.get("versioned") {
			config.versioned = v.eq_ignore_ascii_case("true") || v == "1";
		} else if let Some(v) = SURREAL_DATASTORE_VERSIONED.as_deref() {
			config.versioned = v.eq_ignore_ascii_case("true") || v == "1";
		}
		// Determine the version retention period (query param > env var > default)
		if let Some(v) = params.get("retention") {
			let dur = parse_duration(v)?;
			config.retention_ns = dur.as_nanos() as u64;
		} else if let Some(v) = SURREAL_DATASTORE_RETENTION.as_deref() {
			let dur = parse_duration(v)?;
			config.retention_ns = dur.as_nanos() as u64;
		}
		// Determine the sync mode (query param > env var > default)
		if let Some(v) = params.get("sync") {
			config.sync_mode = parse_sync_mode(v)?;
		} else if let Some(v) = SURREAL_DATASTORE_SYNC_DATA.as_deref() {
			config.sync_mode = parse_sync_mode(v)?;
		}
		// Return the configuration
		Ok(config)
	}
}

// --------------------------------------------------
// Duration
// --------------------------------------------------

/// Parse a duration string in the form `<number><unit>`.
///
/// Supported units:
///   - `µs` / `us` (microseconds)
///   - `ms` (milliseconds)
///   - `s` (seconds)
///   - `m` (minutes)
///   - `h` (hours)
///   - `d` (days)
pub fn parse_duration(s: &str) -> Result<Duration> {
	let s = s.trim();
	if s.is_empty() {
		return Err(Error::Datastore("Empty duration string".into()));
	}
	// Plain numeric value (seconds)
	if let Ok(secs) = s.parse::<u64>() {
		return Ok(Duration::from_secs(secs));
	}
	// Split into numeric prefix and unit suffix
	let num_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
	let (num_str, unit) = s.split_at(num_end);
	if num_str.is_empty() {
		return Err(Error::Datastore(format!("Invalid duration string: '{s}'")));
	}
	let n: u64 = num_str
		.parse()
		.map_err(|_| Error::Datastore(format!("Invalid duration number in: '{s}'")))?;
	match unit {
		"µs" | "us" => Ok(Duration::from_micros(n)),
		"ms" => Ok(Duration::from_millis(n)),
		"s" => Ok(Duration::from_secs(n)),
		"m" => Ok(Duration::from_secs(n * 60)),
		"h" => Ok(Duration::from_secs(n * 3600)),
		"d" => Ok(Duration::from_secs(n * 86400)),
		_ => Err(Error::Datastore(format!(
			"Unknown duration unit '{unit}' in: '{s}'. Expected µs, us, ms, s, m, h, or d"
		))),
	}
}

/// Format a duration as a compact string for query parameters.
///
/// Picks the largest unit that divides evenly, falling back to seconds.
pub fn format_duration(d: Duration) -> String {
	let micros = d.as_micros() as u64;
	if micros == 0 {
		return "0".to_string();
	}
	let secs = d.as_secs();
	// Try largest unit first
	if secs > 0 && secs.is_multiple_of(86400) && d.subsec_nanos() == 0 {
		return format!("{}d", secs / 86400);
	}
	if secs > 0 && secs.is_multiple_of(3600) && d.subsec_nanos() == 0 {
		return format!("{}h", secs / 3600);
	}
	if secs > 0 && secs.is_multiple_of(60) && d.subsec_nanos() == 0 {
		return format!("{}m", secs / 60);
	}
	if d.subsec_nanos() == 0 {
		return format!("{secs}s");
	}
	if micros.is_multiple_of(1000) {
		return format!("{}ms", micros / 1000);
	}
	format!("{micros}us")
}

// --------------------------------------------------
// Sync mode
// --------------------------------------------------

/// Sync mode shared across all storage engines.
///
/// - `Never` - leave flushing to the OS (least durable).
/// - `Every` - sync on every commit (most durable).
/// - `Interval(Duration)` - periodic background flushing at the given interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SyncMode {
	/// Leave flushing to the OS (fastest, least durable).
	Never,
	/// Sync on every commit (slowest, most durable).
	#[default]
	Every,
	/// Periodic background flushing at the given interval.
	Interval(Duration),
}

impl fmt::Display for SyncMode {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Never => f.write_str("never"),
			Self::Every => f.write_str("every"),
			Self::Interval(d) => f.write_str(&format_duration(*d)),
		}
	}
}

/// Parse a `sync` query parameter value into a `SyncMode`.
///
/// Accepts `"never"`, `"every"`, or a duration string (e.g. `"5s"`, `"1m"`).
fn parse_sync_mode(v: &str) -> Result<SyncMode> {
	match v.to_lowercase().as_str() {
		"never" => Ok(SyncMode::Never),
		"every" => Ok(SyncMode::Every),
		v => match parse_duration(v) {
			Ok(dur) if dur.as_millis() > 100 => Ok(SyncMode::Interval(dur)),
			_ => Err(Error::Datastore(format!(
				"Invalid sync mode: '{v}'. Expected 'never', 'every', or a duration larger than 100ms (e.g. '1s')"
			))),
		},
	}
}

// --------------------------------------------------
// Aol mode
// --------------------------------------------------

/// AOL (Append-Only Log) mode for the memory storage engine.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AolMode {
	/// Never use AOL (default).
	#[default]
	Never,
	/// Write synchronously to AOL on every commit.
	Sync,
	/// Write asynchronously to AOL after commit.
	Async,
}

impl fmt::Display for AolMode {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Never => f.write_str("never"),
			Self::Sync => f.write_str("sync"),
			Self::Async => f.write_str("async"),
		}
	}
}

/// Parse a `aol` query parameter value into a `AolMode`.
///
/// Accepts `"never"`, `"sync"`, or `"async"`.
fn parse_aol_mode(v: &str) -> Result<AolMode> {
	match v.to_lowercase().as_str() {
		"never" => Ok(AolMode::Never),
		"sync" => Ok(AolMode::Sync),
		"async" => Ok(AolMode::Async),
		v => Err(Error::Datastore(format!(
			"Invalid aol mode: '{v}'. Expected 'never', 'sync', or 'async'"
		))),
	}
}

// --------------------------------------------------
// Snapshot mode
// --------------------------------------------------

/// Snapshot mode for the memory storage engine.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SnapshotMode {
	/// Never use snapshots (default).
	#[default]
	Never,
	/// Periodically snapshot at the given interval.
	Interval(Duration),
}

impl fmt::Display for SnapshotMode {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Never => f.write_str("never"),
			Self::Interval(d) => f.write_str(&format_duration(*d)),
		}
	}
}

/// Parse a `aol` query parameter value into a `AolMode`.
///
/// Accepts `"never"`, `"sync"`, or `"async"`.
fn parse_snapshot_mode(v: &str) -> Result<SnapshotMode> {
	match v.to_lowercase().as_str() {
		"never" => Ok(SnapshotMode::Never),
		v => match parse_duration(v) {
			Ok(dur) if dur.as_secs() > 30 => Ok(SnapshotMode::Interval(dur)),
			_ => Err(Error::Datastore(format!(
				"Invalid snapshot mode: '{v}'. Expected 'never', or a duration larger than 30s (e.g. '5m')"
			))),
		},
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_parse_duration_plain_seconds() {
		assert_eq!(parse_duration("0").unwrap(), Duration::from_secs(0));
		assert_eq!(parse_duration("60").unwrap(), Duration::from_secs(60));
		assert_eq!(parse_duration("2592000").unwrap(), Duration::from_secs(2592000));
	}

	#[test]
	fn test_parse_duration_with_units() {
		assert_eq!(parse_duration("100µs").unwrap(), Duration::from_micros(100));
		assert_eq!(parse_duration("100us").unwrap(), Duration::from_micros(100));
		assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
		assert_eq!(parse_duration("60s").unwrap(), Duration::from_secs(60));
		assert_eq!(parse_duration("30m").unwrap(), Duration::from_secs(30 * 60));
		assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(24 * 3600));
		assert_eq!(parse_duration("30d").unwrap(), Duration::from_secs(30 * 86400));
	}

	#[test]
	fn test_parse_duration_invalid() {
		assert!(parse_duration("").is_err());
		assert!(parse_duration("abc").is_err());
		assert!(parse_duration("30x").is_err());
		assert!(parse_duration("30d8h").is_err());
	}

	#[test]
	fn test_format_duration() {
		assert_eq!(format_duration(Duration::from_secs(0)), "0");
		assert_eq!(format_duration(Duration::from_micros(500)), "500us");
		assert_eq!(format_duration(Duration::from_millis(100)), "100ms");
		assert_eq!(format_duration(Duration::from_secs(30)), "30s");
		assert_eq!(format_duration(Duration::from_secs(60)), "1m");
		assert_eq!(format_duration(Duration::from_secs(3600)), "1h");
		assert_eq!(format_duration(Duration::from_secs(86400)), "1d");
		// Non-round durations fall back to seconds
		assert_eq!(format_duration(Duration::from_secs(90)), "90s");
		assert_eq!(format_duration(Duration::from_secs(90061)), "90061s");
	}

	#[test]
	fn test_parse_query_params() {
		let params = parse_query_params("versioned=true&sync=every");
		assert_eq!(&params["versioned"], "true");
		assert_eq!(&params["sync"], "every");
	}

	#[test]
	fn test_sync_mode_parsing() {
		assert_eq!(parse_sync_mode("never").unwrap(), SyncMode::Never);
		assert_eq!(parse_sync_mode("every").unwrap(), SyncMode::Every);
		assert_eq!(parse_sync_mode("5s").unwrap(), SyncMode::Interval(Duration::from_secs(5)));
		assert_eq!(parse_sync_mode("1m").unwrap(), SyncMode::Interval(Duration::from_secs(60)));
		assert!(parse_sync_mode("invalid").is_err());
	}

	#[test]
	fn test_surrealkv_config_defaults() {
		let config = SurrealKvConfig::from_params(&HashMap::new()).unwrap();
		assert!(!config.versioned);
		assert_eq!(config.retention_ns, 0);
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	#[test]
	fn test_surrealkv_config_from_params() {
		let params = parse_query_params("versioned=true&retention=30d&sync=every");
		let config = SurrealKvConfig::from_params(&params).unwrap();
		assert!(config.versioned);
		assert_eq!(config.retention_ns, 30 * 86400 * 1_000_000_000);
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	#[test]
	fn test_surrealkv_config_interval_sync() {
		let params = parse_query_params("sync=5s");
		let config = SurrealKvConfig::from_params(&params).unwrap();
		assert_eq!(config.sync_mode, SyncMode::Interval(Duration::from_secs(5)));
	}

	#[test]
	fn test_memory_config_defaults() {
		let config = MemoryConfig::from_params(&HashMap::new()).unwrap();
		assert!(!config.versioned);
		assert_eq!(config.retention_ns, 0);
		assert!(config.persist_path.is_none());
		assert_eq!(config.aol_mode, AolMode::Never);
		assert_eq!(config.snapshot_mode, SnapshotMode::Never);
		assert_eq!(config.sync_mode, SyncMode::Never);
	}

	#[test]
	fn test_memory_config_with_persistence() {
		let params =
			parse_query_params("versioned=true&persist=/tmp/data&aol=sync&snapshot=60s&sync=5s");
		let config = MemoryConfig::from_params(&params).unwrap();
		assert!(config.versioned);
		assert_eq!(config.persist_path.as_deref(), Some("/tmp/data"));
		assert_eq!(config.aol_mode, AolMode::Sync);
		assert_eq!(config.snapshot_mode, SnapshotMode::Interval(Duration::from_secs(60)));
		assert_eq!(config.sync_mode, SyncMode::Interval(Duration::from_secs(5)));
	}

	#[test]
	fn test_memory_config_aol_requires_persist() {
		let params = parse_query_params("aol=sync");
		assert!(MemoryConfig::from_params(&params).is_err());
	}

	#[test]
	fn test_memory_config_sync_requires_persist() {
		let params = parse_query_params("sync=every");
		assert!(MemoryConfig::from_params(&params).is_err());
	}

	#[test]
	fn test_rocksdb_config_defaults() {
		let config = RocksDbConfig::from_params(&HashMap::new()).unwrap();
		assert!(!config.versioned);
		assert_eq!(config.retention_ns, 0);
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	#[test]
	fn test_rocksdb_config_sync_every() {
		let params = parse_query_params("sync=every");
		let config = RocksDbConfig::from_params(&params).unwrap();
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	#[test]
	fn test_rocksdb_config_sync_never() {
		let params = parse_query_params("sync=never");
		let config = RocksDbConfig::from_params(&params).unwrap();
		assert_eq!(config.sync_mode, SyncMode::Never);
	}

	#[test]
	fn test_rocksdb_config_sync_periodic() {
		let params = parse_query_params("sync=200ms");
		let config = RocksDbConfig::from_params(&params).unwrap();
		assert_eq!(config.sync_mode, SyncMode::Interval(Duration::from_millis(200)));
	}

	#[test]
	fn test_rocksdb_config_sync_periodic_seconds() {
		let params = parse_query_params("sync=5s");
		let config = RocksDbConfig::from_params(&params).unwrap();
		assert_eq!(config.sync_mode, SyncMode::Interval(Duration::from_secs(5)));
	}

	#[test]
	fn test_rocksdb_config_sync_invalid() {
		let params = parse_query_params("sync=invalid");
		assert!(RocksDbConfig::from_params(&params).is_err());
	}

	#[test]
	fn test_rocksdb_config_full_params() {
		let params = parse_query_params("versioned=true&retention=30d&sync=every");
		let config = RocksDbConfig::from_params(&params).unwrap();
		assert!(config.versioned);
		assert_eq!(config.retention_ns, 30 * 86400 * 1_000_000_000);
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	// --------------------------------------------------
	// Query param override tests
	// --------------------------------------------------
	// These tests verify that explicit query parameters always take
	// precedence, regardless of any env var fallback values. The env
	// var fallback code paths use the same `parse_*` functions tested
	// above, so parsing correctness is already covered.

	#[test]
	fn test_query_param_overrides_for_surrealkv() {
		// When query params are explicitly set, they must be used
		// regardless of what env vars might provide.
		let params = parse_query_params("versioned=true&retention=7d&sync=never");
		let config = SurrealKvConfig::from_params(&params).unwrap();
		assert!(config.versioned);
		assert_eq!(config.retention_ns, 7 * 86400 * 1_000_000_000);
		assert_eq!(config.sync_mode, SyncMode::Never);
	}

	#[test]
	fn test_query_param_overrides_for_rocksdb() {
		let params = parse_query_params("versioned=true&retention=1h&sync=5s");
		let config = RocksDbConfig::from_params(&params).unwrap();
		assert!(config.versioned);
		assert_eq!(config.retention_ns, 3600 * 1_000_000_000);
		assert_eq!(config.sync_mode, SyncMode::Interval(Duration::from_secs(5)));
	}

	#[test]
	fn test_query_param_overrides_for_memory() {
		let params = parse_query_params(
			"versioned=true&retention=24h&persist=/tmp/test&aol=async&snapshot=5m&sync=every",
		);
		let config = MemoryConfig::from_params(&params).unwrap();
		assert!(config.versioned);
		assert_eq!(config.retention_ns, 24 * 3600 * 1_000_000_000);
		assert_eq!(config.persist_path.as_deref(), Some("/tmp/test"));
		assert_eq!(config.aol_mode, AolMode::Async);
		assert_eq!(config.snapshot_mode, SnapshotMode::Interval(Duration::from_secs(300)));
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	#[test]
	fn test_aol_mode_parsing() {
		assert_eq!(parse_aol_mode("never").unwrap(), AolMode::Never);
		assert_eq!(parse_aol_mode("sync").unwrap(), AolMode::Sync);
		assert_eq!(parse_aol_mode("async").unwrap(), AolMode::Async);
		assert!(parse_aol_mode("invalid").is_err());
	}

	#[test]
	fn test_snapshot_mode_parsing() {
		assert_eq!(parse_snapshot_mode("never").unwrap(), SnapshotMode::Never);
		assert_eq!(
			parse_snapshot_mode("60s").unwrap(),
			SnapshotMode::Interval(Duration::from_secs(60))
		);
		assert_eq!(
			parse_snapshot_mode("5m").unwrap(),
			SnapshotMode::Interval(Duration::from_secs(300))
		);
		assert!(parse_snapshot_mode("invalid").is_err());
		// Duration must be > 30s
		assert!(parse_snapshot_mode("10s").is_err());
	}

	// --------------------------------------------------
	// Env var static definition tests
	// --------------------------------------------------
	// Verify that the environment variable statics are defined and
	// accessible. When env vars are not set (typical in CI), they
	// resolve to None, which means defaults are used.

	#[test]
	fn test_env_var_statics_are_accessible() {
		// These should not panic; they return None when unset.
		let _ = SURREAL_DATASTORE_SYNC_DATA.as_deref();
		let _ = SURREAL_DATASTORE_VERSIONED.as_deref();
		let _ = SURREAL_DATASTORE_RETENTION.as_deref();
		let _ = SURREAL_DATASTORE_PERSIST.as_deref();
		let _ = SURREAL_DATASTORE_AOL.as_deref();
		let _ = SURREAL_DATASTORE_SNAPSHOT.as_deref();
	}
}
