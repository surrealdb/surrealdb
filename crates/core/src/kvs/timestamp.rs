use chrono::{DateTime, Utc};

use super::{Error, Result};

/// A monotonic timestamp which is represented differently depending on the storage backend. The
/// timestamp should be unique and monotonic, and should serialize lexicographically to a vector of
/// bytes.
pub trait Timestamp: Send + Sync {
	/// Convert the timestamp to a byte array
	fn to_ts_bytes(&self) -> Vec<u8>;
	/// Create a timestamp from a byte array
	fn from_ts_bytes(bytes: &[u8]) -> Result<Self>
	where
		Self: Sized;
	/// Convert the timestamp to a version
	fn to_versionstamp(&self) -> u128;
	/// Create a timestamp from a version
	fn from_versionstamp(version: u128) -> Result<Self>
	where
		Self: Sized;
	/// Convert the timestamp to a datetime
	fn to_datetime(&self) -> DateTime<Utc>;
	/// Create a timestamp from a datetime
	fn from_datetime(datetime: DateTime<Utc>) -> Result<Self>
	where
		Self: Sized;
}

impl Timestamp for u64 {
	/// Convert the timestamp to a version
	fn to_versionstamp(&self) -> u128 {
		*self as u128
	}
	/// Create a timestamp from a version
	fn from_versionstamp(version: u128) -> Result<Self> {
		Ok(u64::try_from(version)?)
	}
	/// Convert the timestamp to a datetime
	fn to_datetime(&self) -> DateTime<Utc> {
		DateTime::from_timestamp_nanos(*self as i64)
	}
	/// Create a timestamp from a datetime
	fn from_datetime(datetime: DateTime<Utc>) -> Result<Self> {
		Ok(datetime.timestamp_nanos() as u64)
	}
	/// Convert the timestamp to a byte array
	fn to_ts_bytes(&self) -> Vec<u8> {
		self.to_be_bytes().to_vec()
	}
	/// Create a timestamp from a byte array
	fn from_ts_bytes(bytes: &[u8]) -> Result<Self> {
		match bytes.try_into() {
			Ok(v) => Ok(u64::from_be_bytes(v)),
			Err(_) => Err(Error::TimestampInvalid("timestamp should be 8 bytes".to_string())),
		}
	}
}

/// Hybrid Logical Clock timestamp that combines wall-clock time with a logical counter.
/// Format: Upper 48 bits = milliseconds since epoch, Lower 16 bits = counter (0-65535)
///
/// This provides up to 65,535 unique timestamps per millisecond while maintaining monotonicity
/// even when the system clock goes backwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HlcTimestamp(u64);

impl HlcTimestamp {
	/// Generate the next monotonic HLC timestamp.
	/// Uses a global atomic to ensure monotonicity across all calls.
	///
	/// This method will never return a timestamp that is less than or equal to any previously
	/// returned timestamp. If the system clock goes backwards, it continues from the last known
	/// good timestamp. If more than 65,535 timestamps are requested within the same millisecond,
	/// it will spin-wait until the next millisecond.
	pub fn next() -> Self {
		use std::sync::atomic::{AtomicU64, Ordering};
		#[cfg(not(target_family = "wasm"))]
		use std::time::{SystemTime, UNIX_EPOCH};

		#[cfg(target_family = "wasm")]
		use wasmtimer::std::{SystemTime, UNIX_EPOCH};

		// Set the tiimestamps and masks
		static LAST_TIMESTAMP: AtomicU64 = AtomicU64::new(0);
		const COUNTER_MASK: u64 = 0xFFFF;
		const COUNTER_MAX: u64 = COUNTER_MASK;
		// Set the memory ordering for atomic operations
		let ordering = Ordering::SeqCst;
		// Loop until the timestamp is set
		loop {
			// Get the current system time
			let now_millis = SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.expect("system time cannot be before epoch")
				.as_millis() as u64;
			// Get the last timestamp
			let last = LAST_TIMESTAMP.load(ordering);
			let last_millis = last >> 16;
			let last_counter = last & COUNTER_MASK;
			// Determine the next timestamp
			let (next_millis, next_counter) = if now_millis > last_millis {
				// Time advanced, reset counter
				(now_millis, 0)
			} else if now_millis == last_millis && last_counter < COUNTER_MAX {
				// Same millisecond, increment counter
				(now_millis, last_counter + 1)
			} else {
				// Counter exhausted, spin until next millisecond
				continue;
			};
			// Create the next timestamp
			let next = (next_millis << 16) | next_counter;
			// Compare and exchange the last timestamp with the next timestamp
			if LAST_TIMESTAMP.compare_exchange_weak(last, next, ordering, ordering).is_ok() {
				// The timestamp was successfully set, return the new timestamp
				return HlcTimestamp(next);
			}
		}
	}
}

impl Timestamp for HlcTimestamp {
	/// Convert the timestamp to a version
	fn to_versionstamp(&self) -> u128 {
		self.0 as u128
	}

	/// Create a timestamp from a version
	fn from_versionstamp(version: u128) -> Result<Self> {
		Ok(HlcTimestamp(u64::try_from(version)?))
	}

	/// Convert the timestamp to a datetime
	/// Extracts the milliseconds component and converts to DateTime
	fn to_datetime(&self) -> DateTime<Utc> {
		let millis = self.0 >> 16;
		DateTime::from_timestamp_millis(millis as i64)
			.expect("timestamp milliseconds should be valid")
	}

	/// Create a timestamp from a datetime
	/// Creates an HLC timestamp with the datetime's milliseconds and counter=0
	fn from_datetime(datetime: DateTime<Utc>) -> Result<Self> {
		let millis = datetime.timestamp_millis() as u64;
		Ok(HlcTimestamp(millis << 16))
	}

	/// Convert the timestamp to a byte array
	fn to_ts_bytes(&self) -> Vec<u8> {
		self.0.to_be_bytes().to_vec()
	}

	/// Create a timestamp from a byte array
	fn from_ts_bytes(bytes: &[u8]) -> Result<Self> {
		match bytes.try_into() {
			Ok(v) => Ok(HlcTimestamp(u64::from_be_bytes(v))),
			Err(_) => Err(Error::TimestampInvalid("timestamp should be 8 bytes".to_string())),
		}
	}
}

#[cfg(test)]
mod tests {
	use chrono::TimeZone;

	use super::*;

	#[test]
	fn test_u64_bytes_roundtrip() {
		let values = [0u64, 1, 42, u64::MAX / 2, u64::MAX];

		for &value in &values {
			let bytes = value.to_ts_bytes();
			let recovered = u64::from_ts_bytes(&bytes).unwrap();
			assert_eq!(value, recovered, "Failed roundtrip for u64 value {}", value);
		}
	}

	#[test]
	fn test_u64_bytes_length() {
		let value = 12345u64;
		let bytes = value.to_ts_bytes();
		assert_eq!(bytes.len(), 8, "u64 timestamp should be 8 bytes");
	}

	#[test]
	fn test_u64_bytes_big_endian() {
		// Verify big-endian encoding for lexicographic ordering
		let small = 100u64;
		let large = 1000u64;
		let small_bytes = small.to_ts_bytes();
		let large_bytes = large.to_ts_bytes();
		assert!(small_bytes < large_bytes, "Bytes should be lexicographically ordered");
	}

	#[test]
	fn test_u64_bytes_invalid_length() {
		let too_short = vec![0u8; 4];
		let result = u64::from_ts_bytes(&too_short);
		assert!(result.is_err(), "Should fail with invalid byte length");

		let too_long = vec![0u8; 16];
		let result = u64::from_ts_bytes(&too_long);
		assert!(result.is_err(), "Should fail with invalid byte length");
	}

	#[test]
	fn test_u64_versionstamp_roundtrip() {
		let values = [0u64, 1, 42, u64::MAX / 2, u64::MAX];

		for &value in &values {
			let version = value.to_versionstamp();
			let recovered = u64::from_versionstamp(version).unwrap();
			assert_eq!(value, recovered, "Failed versionstamp roundtrip for u64 value {}", value);
		}
	}

	#[test]
	fn test_u64_datetime_roundtrip() {
		// Test with various timestamps
		let now = Utc::now();
		let ts = u64::from_datetime(now).unwrap();
		let recovered = ts.to_datetime();

		// DateTime roundtrip should be within reasonable precision
		// Note: nanosecond precision might be lost in conversion
		assert_eq!(now.timestamp_nanos(), recovered.timestamp_nanos(), "Failed datetime roundtrip");
	}

	#[test]
	fn test_u64_datetime_specific_values() {
		// Test epoch
		let epoch = Utc.timestamp_opt(0, 0).unwrap();
		let ts = u64::from_datetime(epoch).unwrap();
		let recovered = ts.to_datetime();
		assert_eq!(epoch.timestamp_nanos(), recovered.timestamp_nanos());

		// Test a known timestamp
		let known_time = Utc.timestamp_opt(1700000000, 123456789).unwrap();
		let ts = u64::from_datetime(known_time).unwrap();
		let recovered = ts.to_datetime();
		assert_eq!(known_time.timestamp_nanos(), recovered.timestamp_nanos());
	}

	#[test]
	fn test_cross_type_conversions() {
		// Test that conversions work correctly across different methods
		let original = 1234567890u64;

		// Bytes -> Version -> DateTime and back
		let bytes = original.to_ts_bytes();
		let from_bytes = u64::from_ts_bytes(&bytes).unwrap();
		let version = from_bytes.to_versionstamp();
		let from_version = u64::from_versionstamp(version).unwrap();
		let datetime = from_version.to_datetime();
		let from_datetime = u64::from_datetime(datetime).unwrap();

		assert_eq!(original, from_datetime, "Cross-type conversion failed");
	}

	#[test]
	fn test_monotonic_property() {
		// Ensure that larger timestamps convert to larger byte arrays
		let timestamps = [1u64, 100, 1000, 10000, 100000];
		let byte_arrays: Vec<Vec<u8>> = timestamps.iter().map(|t| t.to_ts_bytes()).collect();

		// Verify that byte arrays are in ascending order
		for i in 1..byte_arrays.len() {
			assert!(
				byte_arrays[i - 1] < byte_arrays[i],
				"Monotonic property violated: byte arrays should be ordered"
			);
		}
	}

	// HlcTimestamp tests

	#[test]
	fn test_hlc_bytes_roundtrip() {
		// Create HLC timestamps with various millisecond and counter values
		let test_cases = vec![
			(0u64 << 16) | 0,        // epoch with counter 0
			(1000u64 << 16) | 0,     // 1 second with counter 0
			(1000u64 << 16) | 1,     // 1 second with counter 1
			(1000u64 << 16) | 65535, // 1 second with max counter
			(u64::MAX >> 16) << 16,  // max milliseconds with counter 0
		];

		for value in test_cases {
			let ts = super::HlcTimestamp(value);
			let bytes = ts.to_ts_bytes();
			let recovered = super::HlcTimestamp::from_ts_bytes(&bytes).unwrap();
			assert_eq!(ts, recovered, "Failed roundtrip for HLC value {}", value);
		}
	}

	#[test]
	fn test_hlc_bytes_length() {
		let ts = super::HlcTimestamp::next();
		let bytes = ts.to_ts_bytes();
		assert_eq!(bytes.len(), 8, "HLC timestamp should be 8 bytes");
	}

	#[test]
	fn test_hlc_bytes_lexicographic_ordering() {
		// Create timestamps with increasing values
		let ts1 = super::HlcTimestamp((1000u64 << 16) | 0);
		let ts2 = super::HlcTimestamp((1000u64 << 16) | 1);
		let ts3 = super::HlcTimestamp((1001u64 << 16) | 0);

		let bytes1 = ts1.to_ts_bytes();
		let bytes2 = ts2.to_ts_bytes();
		let bytes3 = ts3.to_ts_bytes();

		assert!(bytes1 < bytes2, "Same millisecond, counter should order lexicographically");
		assert!(bytes2 < bytes3, "Different milliseconds should order lexicographically");
	}

	#[test]
	fn test_hlc_versionstamp_roundtrip() {
		let test_values = vec![(1000u64 << 16) | 0, (1000u64 << 16) | 100, (u64::MAX >> 16) << 16];

		for value in test_values {
			let ts = super::HlcTimestamp(value);
			let version = ts.to_versionstamp();
			let recovered = super::HlcTimestamp::from_versionstamp(version).unwrap();
			assert_eq!(ts, recovered, "Failed versionstamp roundtrip for HLC value {}", value);
		}
	}

	#[test]
	fn test_hlc_datetime_roundtrip() {
		// Test with various timestamps
		let now = Utc::now();
		let ts = super::HlcTimestamp::from_datetime(now).unwrap();
		let recovered = ts.to_datetime();

		// Should match at millisecond precision (counter is lost)
		assert_eq!(
			now.timestamp_millis(),
			recovered.timestamp_millis(),
			"Failed datetime roundtrip"
		);
	}

	#[test]
	fn test_hlc_datetime_specific_values() {
		// Test epoch
		let epoch = Utc.timestamp_opt(0, 0).unwrap();
		let ts = super::HlcTimestamp::from_datetime(epoch).unwrap();
		let recovered = ts.to_datetime();
		assert_eq!(epoch.timestamp_millis(), recovered.timestamp_millis());

		// Test a known timestamp
		let known_time = Utc.timestamp_opt(1700000000, 123456789).unwrap();
		let ts = super::HlcTimestamp::from_datetime(known_time).unwrap();
		let recovered = ts.to_datetime();
		assert_eq!(known_time.timestamp_millis(), recovered.timestamp_millis());
	}

	#[test]
	fn test_hlc_monotonicity() {
		// Generate multiple timestamps in quick succession
		let mut timestamps = Vec::new();
		for _ in 0..100 {
			timestamps.push(super::HlcTimestamp::next());
		}

		// Verify strict monotonicity
		for i in 1..timestamps.len() {
			assert!(
				timestamps[i - 1] < timestamps[i],
				"HLC timestamps should be strictly monotonic"
			);
		}
	}

	#[test]
	fn test_hlc_monotonicity_bytes() {
		// Generate multiple timestamps and verify byte ordering
		let mut byte_arrays = Vec::new();
		for _ in 0..100 {
			let ts = super::HlcTimestamp::next();
			byte_arrays.push(ts.to_ts_bytes());
		}

		// Verify that byte arrays are in ascending order
		for i in 1..byte_arrays.len() {
			assert!(
				byte_arrays[i - 1] < byte_arrays[i],
				"HLC timestamp bytes should be lexicographically ordered"
			);
		}
	}

	#[test]
	fn test_hlc_concurrent_generation() {
		// Test that concurrent timestamp generation maintains monotonicity
		use std::sync::{Arc, Barrier};
		use std::thread;

		let num_threads = 10;
		let timestamps_per_thread = 100;
		let barrier = Arc::new(Barrier::new(num_threads));
		let mut handles = vec![];

		for _ in 0..num_threads {
			let barrier_clone = Arc::clone(&barrier);
			let handle = thread::spawn(move || {
				barrier_clone.wait();
				let mut local_timestamps = Vec::new();
				for _ in 0..timestamps_per_thread {
					local_timestamps.push(super::HlcTimestamp::next());
				}
				local_timestamps
			});
			handles.push(handle);
		}

		// Collect all timestamps from all threads
		let mut all_timestamps = Vec::new();
		for handle in handles {
			let thread_timestamps = handle.join().unwrap();
			all_timestamps.extend(thread_timestamps);
		}

		// Sort and verify no duplicates
		all_timestamps.sort();
		for i in 1..all_timestamps.len() {
			assert!(
				all_timestamps[i - 1] < all_timestamps[i],
				"Concurrent HLC timestamps should have no duplicates"
			);
		}
	}

	#[test]
	fn test_hlc_ordering_property() {
		// Verify that HlcTimestamp implements proper ordering
		let ts1 = super::HlcTimestamp((1000u64 << 16) | 0);
		let ts2 = super::HlcTimestamp((1000u64 << 16) | 1);
		let ts3 = super::HlcTimestamp((1001u64 << 16) | 0);

		assert!(ts1 < ts2);
		assert!(ts2 < ts3);
		assert!(ts1 < ts3);
		assert_eq!(ts1, ts1);
	}
}
