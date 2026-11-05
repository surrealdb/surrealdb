//! Durability options for the embedded storage engines, passed to
//! [`Connect::sync`](crate::Connect::sync), [`Connect::aol`](crate::Connect::aol)
//! and [`Connect::snapshot`](crate::Connect::snapshot).
//!
//! Each mode reaches the engine as the text of a connection query parameter,
//! which the engine parses, so these types belong to the SDK and its public API
//! names no type from the storage crates.

use std::fmt;
use std::time::Duration;

/// How and when the storage engine flushes committed data to disk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SyncMode {
	/// Leave flushing to the operating system (fastest, least durable).
	Never,
	/// Flush on every transaction commit (slowest, most durable).
	#[default]
	Every,
	/// Flush in the background at the given interval, which must be longer
	/// than 100 milliseconds.
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

/// Whether, and how, the memory engine writes an append-only log.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum AolMode {
	/// Never write an append-only log.
	#[default]
	Never,
	/// Write to the log synchronously on every commit.
	Sync,
	/// Write to the log asynchronously after each commit.
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

/// Whether the memory engine takes periodic snapshots.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SnapshotMode {
	/// Never take snapshots.
	#[default]
	Never,
	/// Take a snapshot at the given interval.
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

/// Format a duration in the compact suffix form the engine's config parser
/// accepts (`30d`, `12h`, `5m`, `90s`, `250ms`, `10us`), choosing the largest
/// unit that divides the duration exactly.
pub(crate) fn format_duration(d: Duration) -> String {
	let micros = d.as_micros();
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

#[cfg(test)]
mod tests {
	use std::str::FromStr;
	use std::time::Duration;

	use super::*;

	/// Every mode reaches the engine as text the engine's own parser reads
	/// back as the same mode.
	#[test]
	fn modes_round_trip_through_the_engine_parser() {
		use surrealdb_kvs::config as kvs;

		for (mode, expected) in [
			(SyncMode::Never, kvs::SyncMode::Never),
			(SyncMode::Every, kvs::SyncMode::Every),
			(
				SyncMode::Interval(Duration::from_millis(250)),
				kvs::SyncMode::Interval(Duration::from_millis(250)),
			),
			(
				SyncMode::Interval(Duration::from_secs(90)),
				kvs::SyncMode::Interval(Duration::from_secs(90)),
			),
		] {
			assert_eq!(kvs::SyncMode::from_str(&mode.to_string()).unwrap(), expected, "{mode}");
		}
		for (mode, expected) in [
			(AolMode::Never, kvs::AolMode::Never),
			(AolMode::Sync, kvs::AolMode::Sync),
			(AolMode::Async, kvs::AolMode::Async),
		] {
			assert_eq!(kvs::AolMode::from_str(&mode.to_string()).unwrap(), expected, "{mode}");
		}
		for (mode, expected) in [
			(SnapshotMode::Never, kvs::SnapshotMode::Never),
			(
				SnapshotMode::Interval(Duration::from_secs(3600)),
				kvs::SnapshotMode::Interval(Duration::from_secs(3600)),
			),
		] {
			assert_eq!(kvs::SnapshotMode::from_str(&mode.to_string()).unwrap(), expected, "{mode}");
		}
	}
}
