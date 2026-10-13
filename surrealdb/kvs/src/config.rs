use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

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
// RocksDB configuration
// --------------------------------------------------

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
			Self::Interval(d) => f.write_str(&surrealdb_cnf::format_duration(*d)),
		}
	}
}

impl FromStr for SyncMode {
	type Err = String;

	/// Parse a `sync` query parameter value into a `SyncMode`.
	///
	/// Accepts `"never"`, `"every"`, or a duration string (e.g. `"5s"`, `"1m"`).
	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		match s.to_lowercase().as_str() {
			"never" => Ok(SyncMode::Never),
			"every" => Ok(SyncMode::Every),
			v => match surrealdb_cnf::parse_duration(v) {
				Ok(dur) if dur.as_millis() > 100 => Ok(SyncMode::Interval(dur)),
				_ => Err(format!(
					"Invalid sync mode: '{v}'. Expected 'never', 'every', or a duration larger than 100ms (e.g. '1s')"
				)),
			},
		}
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

impl FromStr for AolMode {
	type Err = String;

	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		match s.to_lowercase().as_str() {
			"never" => Ok(AolMode::Never),
			"sync" => Ok(AolMode::Sync),
			"async" => Ok(AolMode::Async),
			v => Err(format!("Invalid aol mode: '{v}'. Expected 'never', 'sync', or 'async'")),
		}
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
			Self::Interval(d) => f.write_str(&surrealdb_cnf::format_duration(*d)),
		}
	}
}

impl FromStr for SnapshotMode {
	type Err = String;

	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		match s.to_lowercase().as_str() {
			"never" => Ok(SnapshotMode::Never),
			v => match surrealdb_cnf::parse_duration(v) {
				Ok(dur) if dur.as_secs() > 30 => Ok(SnapshotMode::Interval(dur)),
				_ => Err(format!(
					"Invalid snapshot mode: '{v}'. Expected 'never', or a duration larger than 30s (e.g. '5m')"
				)),
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_parse_query_params() {
		let params = parse_query_params("versioned=true&sync=every");
		assert_eq!(&params["versioned"], "true");
		assert_eq!(&params["sync"], "every");
	}

	#[test]
	fn test_sync_mode_parsing() {
		assert_eq!("never".parse::<SyncMode>().unwrap(), SyncMode::Never);
		assert_eq!("every".parse::<SyncMode>().unwrap(), SyncMode::Every);
		assert_eq!("5s".parse::<SyncMode>().unwrap(), SyncMode::Interval(Duration::from_secs(5)));
		assert_eq!("1m".parse::<SyncMode>().unwrap(), SyncMode::Interval(Duration::from_secs(60)));
		assert!("invalid".parse::<SyncMode>().is_err());
	}

	#[test]
	fn test_aol_mode_parsing() {
		assert_eq!("never".parse::<AolMode>().unwrap(), AolMode::Never);
		assert_eq!("sync".parse::<AolMode>().unwrap(), AolMode::Sync);
		assert_eq!("async".parse::<AolMode>().unwrap(), AolMode::Async);
		assert!("invalid".parse::<AolMode>().is_err());
	}

	#[test]
	fn test_snapshot_mode_parsing() {
		assert_eq!("never".parse::<SnapshotMode>().unwrap(), SnapshotMode::Never);
		assert_eq!(
			"60s".parse::<SnapshotMode>().unwrap(),
			SnapshotMode::Interval(Duration::from_secs(60))
		);
		assert_eq!(
			"5m".parse::<SnapshotMode>().unwrap(),
			SnapshotMode::Interval(Duration::from_secs(300))
		);
		assert!("invalid".parse::<SnapshotMode>().is_err());
		// Duration must be > 30s
		assert!("10s".parse::<SnapshotMode>().is_err());
	}
}
