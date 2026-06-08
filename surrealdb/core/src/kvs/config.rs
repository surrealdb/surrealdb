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
pub fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
	let s = s.trim();
	if s.is_empty() {
		return Err("Empty duration string".into());
	}
	// Plain numeric value (seconds)
	if let Ok(secs) = s.parse::<u64>() {
		return Ok(Duration::from_secs(secs));
	}
	// Split into numeric prefix and unit suffix
	let num_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
	let (num_str, unit) = s.split_at(num_end);
	if num_str.is_empty() {
		return Err(format!("Invalid duration string: '{s}'"));
	}
	let n: u64 = num_str.parse().map_err(|_| format!("Invalid duration number in: '{s}'"))?;
	match unit {
		"µs" | "us" => Ok(Duration::from_micros(n)),
		"ms" => Ok(Duration::from_millis(n)),
		"s" => Ok(Duration::from_secs(n)),
		"m" => Ok(Duration::from_secs(n * 60)),
		"h" => Ok(Duration::from_secs(n * 3600)),
		"d" => Ok(Duration::from_secs(n * 86400)),
		_ => Err(format!(
			"Unknown duration unit '{unit}' in: '{s}'. Expected µs, us, ms, s, m, h, or d"
		)),
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

impl FromStr for SyncMode {
	type Err = String;

	/// Parse a `sync` query parameter value into a `SyncMode`.
	///
	/// Accepts `"never"`, `"every"`, or a duration string (e.g. `"5s"`, `"1m"`).
	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		match s.to_lowercase().as_str() {
			"never" => Ok(SyncMode::Never),
			"every" => Ok(SyncMode::Every),
			v => match parse_duration(v) {
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
			Self::Interval(d) => f.write_str(&format_duration(*d)),
		}
	}
}

impl FromStr for SnapshotMode {
	type Err = String;

	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		match s.to_lowercase().as_str() {
			"never" => Ok(SnapshotMode::Never),
			v => match parse_duration(v) {
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
