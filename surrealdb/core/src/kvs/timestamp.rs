use std::any::Any;
use std::hint::unreachable_unchecked;
use std::time::Duration;

use chrono::{DateTime, Utc};

use super::{Error, Result};

pub const MAX_TIMESTAMP_BYTES: usize = 32;

/// The kind of implementation of a version stamp.
/// Should not be created manually but retrieved from the KV store.
pub trait TimeStampImpl: Any + Send + Sync {
	fn earliest(&self) -> BoxTimeStamp;
	/// Create a timestamp from a versionstamp, can return `None` if the versionstamp is out of
	/// range from the timestamp
	fn create_from_versionstamp(&self, version: u128) -> Option<BoxTimeStamp>;

	/// Create a timestamp from a duration, can return `None` if the datetime is out of range from
	/// the timestamp
	fn create_from_datetime(&self, dt: DateTime<Utc>) -> Option<BoxTimeStamp>;

	/// Decode key-encoded bytes into a timestamp
	fn decode(&self, bytes: &[u8]) -> Result<BoxTimeStamp>;
}
pub type BoxTimeStampImpl = Box<dyn TimeStampImpl>;

pub trait TimeStamp: Any + Send + Sync {
	/// Returns the version stamp for the timestamp.
	fn as_versionstamp(&self) -> u128;

	/// Returns the datetime that the timestamp belongs to.
	fn as_datetime(&self) -> Option<DateTime<Utc>>;

	/// Subtract a duration from the timestamp returning a duration that much in the past from this
	/// timestamp. Can return none if the new time is outside of the range of the timestamp.
	fn sub_checked(&self, duration: Duration) -> Option<BoxTimeStamp>;

	/// Encode the timestamp into key-encoded bytes.
	fn encode<'a>(&self, bytes: &'a mut [u8; MAX_TIMESTAMP_BYTES]) -> &'a [u8];
}

pub struct BoxTimeStamp(Box<dyn TimeStamp>);

impl BoxTimeStamp {
	pub fn new<T: TimeStamp>(t: T) -> Self {
		BoxTimeStamp(Box::new(t))
	}

	pub fn downcast<T: TimeStamp>(self) -> std::result::Result<Box<T>, Self> {
		if (&self.0 as &dyn Any).is::<T>() {
			let Ok(x) = (self.0 as Box<dyn Any>).downcast::<T>() else {
				unsafe { unreachable_unchecked() }
			};
			Ok(x)
		} else {
			Err(self)
		}
	}

	pub fn as_versionstamp(&self) -> u128 {
		self.0.as_versionstamp()
	}

	pub fn as_datetime(&self) -> Option<DateTime<Utc>> {
		self.0.as_datetime()
	}

	pub fn sub_checked(&self, duration: Duration) -> Option<BoxTimeStamp> {
		self.0.sub_checked(duration)
	}

	pub fn encode<'a>(&self, bytes: &'a mut [u8; MAX_TIMESTAMP_BYTES]) -> &'a [u8] {
		self.0.encode(bytes)
	}
}

/// Simple monotonically incrementing atomic timestamp.
///
/// This uses a global atomic counter that increments for each call to `next()`.
/// The counter is treated as milliseconds since epoch for datetime conversions.
/// This provides monotonicity without using system time or bit-splitting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct IncTimeStamp(u64);

pub struct IncTimeStampImpl;

impl TimeStampImpl for IncTimeStampImpl {
	fn earliest(&self) -> BoxTimeStamp {
		BoxTimeStamp::new(IncTimeStamp(0))
	}

	fn create_from_versionstamp(&self, version: u128) -> Option<BoxTimeStamp> {
		Some(BoxTimeStamp::new(IncTimeStamp(version.try_into().ok()?)))
	}

	fn create_from_datetime(&self, dt: DateTime<Utc>) -> Option<BoxTimeStamp> {
		let milis = dt.timestamp_millis();
		if milis < 0 {
			return None;
		}

		Some(BoxTimeStamp::new(IncTimeStamp(milis as u64)))
	}

	fn decode(&self, bytes: &[u8]) -> Result<BoxTimeStamp> {
		let bytes = <[u8; 8]>::try_from(bytes).map_err(|_| {
			Error::TimestampInvalid("encoded timestamp not a valid length".to_string())
		})?;
		Ok(BoxTimeStamp::new(HlcTimeStamp(u64::from_be_bytes(bytes))))
	}
}

impl IncTimeStamp {
	/// Generate the next monotonic timestamp.
	/// Uses a global atomic counter to ensure monotonicity across all calls.
	///
	/// This method will never return a timestamp that is less than or equal to any previously
	/// returned timestamp. Each call increments the counter by 1.
	pub fn next() -> Self {
		static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
		IncTimeStamp(COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
	}
}

impl TimeStamp for IncTimeStamp {
	fn as_versionstamp(&self) -> u128 {
		self.0 as u128
	}

	fn as_datetime(&self) -> Option<DateTime<Utc>> {
		DateTime::from_timestamp_millis(self.0 as i64)
	}

	fn sub_checked(&self, duration: Duration) -> Option<BoxTimeStamp> {
		let duration_milis = duration.as_millis().try_into().ok()?;
		Some(BoxTimeStamp::new(IncTimeStamp(self.0.checked_sub(duration_milis)?)))
	}

	fn encode<'a>(&self, bytes: &'a mut [u8; MAX_TIMESTAMP_BYTES]) -> &'a [u8] {
		let ts_bytes = self.0.to_be_bytes();
		bytes[..8].copy_from_slice(&ts_bytes);
		&bytes[..8]
	}
}

/// Hybrid Logical Clock timestamp that combines wall-clock time with a logical counter.
/// Format: Upper 48 bits = milliseconds since epoch, Lower 16 bits = counter (0-65535)
///
/// This provides up to 65,535 unique timestamps per millisecond while maintaining monotonicity
/// even when the system clock goes backwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HlcTimeStamp(pub(crate) u64);

pub struct HlcTimeStampImpl;

impl TimeStampImpl for HlcTimeStampImpl {
	fn earliest(&self) -> BoxTimeStamp {
		BoxTimeStamp::new(IncTimeStamp(0))
	}

	fn create_from_versionstamp(&self, version: u128) -> Option<BoxTimeStamp> {
		Some(BoxTimeStamp::new(HlcTimeStamp(version.try_into().ok()?)))
	}

	fn create_from_datetime(&self, dt: DateTime<Utc>) -> Option<BoxTimeStamp> {
		let milis = dt.timestamp_millis();
		if milis < 0 {
			return None;
		}

		Some(BoxTimeStamp::new(HlcTimeStamp(milis as u64)))
	}

	fn decode(&self, bytes: &[u8]) -> Result<BoxTimeStamp> {
		let bytes = <[u8; 8]>::try_from(bytes).map_err(|_| {
			Error::TimestampInvalid("encoded timestamp not a valid length".to_string())
		})?;
		Ok(BoxTimeStamp::new(HlcTimeStamp(u64::from_be_bytes(bytes))))
	}
}

impl HlcTimeStamp {
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

		// Set the timestamps and masks
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
			if LAST_TIMESTAMP.compare_exchange_weak(last, next, ordering, Ordering::Relaxed).is_ok()
			{
				// The timestamp was successfully set, return the new timestamp
				return HlcTimeStamp(next);
			}
		}
	}
}

impl TimeStamp for HlcTimeStamp {
	fn as_versionstamp(&self) -> u128 {
		self.0 as u128
	}

	fn as_datetime(&self) -> Option<DateTime<Utc>> {
		DateTime::from_timestamp_millis((self.0 >> 16) as i64)
	}

	fn sub_checked(&self, duration: Duration) -> Option<BoxTimeStamp> {
		let duration_milis = duration.as_millis().try_into().ok()?;

		let milis = self.0 >> 16;
		let milis = milis.checked_sub(duration_milis)?;
		if milis >= 1 << 48 {
			return None;
		}

		Some(BoxTimeStamp::new(HlcTimeStamp(milis << 16 | self.0 & 0xFFFF)))
	}

	fn encode<'a>(&self, bytes: &'a mut [u8; MAX_TIMESTAMP_BYTES]) -> &'a [u8] {
		let ts_bytes = self.0.to_be_bytes();
		bytes[..8].copy_from_slice(&ts_bytes);
		&bytes[..8]
	}
}

#[cfg(test)]
mod tests {
	use chrono::TimeZone;

	use super::*;

	#[test]
	fn test_versionstamp_bytes_roundtrip() {
		let ts_impl = HlcTimeStampImpl;
		let values = [0, 1, 42, (u64::MAX / 2) as u128, u64::MAX as u128];

		for &value in &values {
			let buf = &mut [0; _];
			let bytes = ts_impl.create_from_versionstamp(value).unwrap().encode(buf);
			let recovered = ts_impl.decode(bytes).unwrap().as_versionstamp();
			assert_eq!(value, recovered, "Failed roundtrip for u64 value {}", value);
		}
	}

	#[test]
	fn test_u64_bytes_length() {
		let ts_impl = HlcTimeStampImpl;
		let buf = &mut [0; _];
		let bytes = ts_impl.create_from_versionstamp(12345).unwrap().encode(buf);
		assert_eq!(bytes.len(), 8, "u64 timestamp should be 8 bytes");
	}

	#[test]
	fn test_u64_bytes_big_endian() {
		let ts_impl = HlcTimeStampImpl;
		// Verify big-endian encoding for lexicographic ordering
		let small = 100;
		let large = 1000;
		let buf = &mut [0; _];
		let small_bytes = ts_impl.create_from_versionstamp(small).unwrap().encode(buf);
		let buf = &mut [0; _];
		let large_bytes = ts_impl.create_from_versionstamp(large).unwrap().encode(buf);
		assert!(small_bytes < large_bytes, "Bytes should be lexicographically ordered");
	}

	#[test]
	fn test_u64_bytes_invalid_length() {
		let ts_impl = HlcTimeStampImpl;
		let too_short = [0u8; 4];
		let result = ts_impl.decode(&too_short);
		assert!(result.is_err(), "Should fail with invalid byte length");

		let too_long = [0u8; 16];
		let result = ts_impl.decode(&too_long);
		assert!(result.is_err(), "Should fail with invalid byte length");
	}

	#[test]
	fn test_datetime_roundtrip() {
		let ts_impl = HlcTimeStampImpl;
		// Test with various timestamps
		let now = Utc::now();
		let ts = ts_impl.create_from_datetime(now).unwrap();
		let recovered = ts.as_datetime().unwrap();

		// DateTime roundtrip should be within reasonable precision
		// Note: nanosecond precision might be lost in conversion
		assert_eq!(
			now.timestamp_millis(),
			recovered.timestamp_millis(),
			"Failed datetime roundtrip"
		);
	}

	#[test]
	fn test_u64_datetime_specific_values() {
		let ts_impl = HlcTimeStampImpl;
		// Test epoch
		let epoch = Utc.timestamp_opt(0, 0).unwrap();
		let ts = ts_impl.create_from_datetime(epoch).unwrap();
		let recovered = ts.as_datetime().unwrap();
		assert_eq!(epoch.timestamp_nanos(), recovered.timestamp_nanos());

		// Test a known timestamp
		let known_time = Utc.timestamp_opt(1700000000, 123456789).unwrap();
		let ts = ts_impl.create_from_datetime(known_time).unwrap();
		let recovered = ts.as_datetime().unwrap();
		assert_eq!(known_time.timestamp_millis(), recovered.timestamp_millis());
	}

	#[test]
	fn test_cross_type_conversions() {
		let ts_impl = HlcTimeStampImpl;
		// Test that conversions work correctly across different methods
		let original = ts_impl.create_from_versionstamp(1234567890).unwrap();

		// Bytes -> Version -> DateTime and back
		let buf = &mut [0; _];
		let bytes = original.encode(buf);
		let from_bytes = ts_impl.decode(bytes).unwrap();
		let version = from_bytes.as_versionstamp();
		let from_version = ts_impl.create_from_versionstamp(version).unwrap();
		let datetime = from_version.as_datetime().unwrap();
		let from_datetime = ts_impl.create_from_datetime(datetime).unwrap();

		let Ok(original) = original.downcast::<HlcTimeStamp>() else {
			panic!()
		};
		let Ok(from_datetime) = from_datetime.downcast::<HlcTimeStamp>() else {
			panic!()
		};

		assert_eq!(*original, *from_datetime, "Cross-type conversion failed");
	}

	#[test]
	fn test_monotonic_property() {
		let ts_impl = HlcTimeStampImpl;
		// Ensure that larger timestamps convert to larger byte arrays
		let timestamps = [1u128, 100, 1000, 10000, 100000];
		let byte_arrays: Vec<Vec<u8>> = timestamps
			.iter()
			.map(|t| {
				let buf = &mut [0; _];
				ts_impl.create_from_versionstamp(*t).unwrap().encode(buf).to_vec()
			})
			.collect();

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
			0,                       // epoch with counter 0
			1000u64 << 16,           // 1 second with counter 0
			(1000u64 << 16) | 1,     // 1 second with counter 1
			(1000u64 << 16) | 65535, // 1 second with max counter
			(u64::MAX >> 16) << 16,  // max milliseconds with counter 0
		];

		for value in test_cases {
			let ts = super::HlcTimeStamp(value);
			let buf = &mut [0u8; _];
			let bytes = ts.encode(buf);
			let Ok(recovered) =
				super::HlcTimeStampImpl.decode(bytes).unwrap().downcast::<HlcTimeStamp>()
			else {
				panic!()
			};
			assert_eq!(ts, *recovered, "Failed roundtrip for HLC value {}", value);
		}
	}

	#[test]
	fn test_hlc_bytes_length() {
		let ts = super::HlcTimeStamp::next();
		let buf = &mut [0u8; _];
		let bytes = ts.encode(buf);
		assert_eq!(bytes.len(), 8, "HLC timestamp should be 8 bytes");
	}

	#[test]
	fn test_hlc_bytes_lexicographic_ordering() {
		// Create timestamps with increasing values
		let ts1 = super::HlcTimeStamp(1000u64 << 16);
		let ts2 = super::HlcTimeStamp((1000u64 << 16) | 1);
		let ts3 = super::HlcTimeStamp(1001u64 << 16);

		let buf = &mut [0u8; _];
		let bytes1 = ts1.encode(buf);
		let buf = &mut [0u8; _];
		let bytes2 = ts2.encode(buf);
		let buf = &mut [0u8; _];
		let bytes3 = ts3.encode(buf);

		assert!(bytes1 < bytes2, "Same millisecond, counter should order lexicographically");
		assert!(bytes2 < bytes3, "Different milliseconds should order lexicographically");
	}

	#[test]
	fn test_hlc_versionstamp_roundtrip() {
		let test_values = vec![1000u64 << 16, (1000u64 << 16) | 100, (u64::MAX >> 16) << 16];

		for value in test_values {
			let ts = super::HlcTimeStamp(value);
			let version = ts.as_versionstamp();
			let Ok(recovered) =
				super::HlcTimeStampImpl.create_from_versionstamp(version).unwrap().downcast()
			else {
				panic!()
			};
			assert_eq!(ts, *recovered, "Failed versionstamp roundtrip for HLC value {}", value);
		}
	}

	#[test]
	fn test_hlc_datetime_roundtrip() {
		// Test with various timestamps
		let now = Utc::now();
		let ts = super::HlcTimeStampImpl.create_from_datetime(now).unwrap();
		let recovered = ts.as_datetime().unwrap();

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
		let ts = super::HlcTimeStampImpl.create_from_datetime(epoch).unwrap();
		let recovered = ts.as_datetime().unwrap();
		assert_eq!(epoch.timestamp_millis(), recovered.timestamp_millis());

		// Test a known timestamp
		let known_time = Utc.timestamp_opt(1700000000, 123456789).unwrap();
		let ts = super::HlcTimeStampImpl.create_from_datetime(known_time).unwrap();
		let recovered = ts.as_datetime().unwrap();
		assert_eq!(known_time.timestamp_millis(), recovered.timestamp_millis());
	}

	#[test]
	fn test_hlc_monotonicity() {
		// Generate multiple timestamps in quick succession
		let mut timestamps = Vec::new();
		for _ in 0..100 {
			timestamps.push(super::HlcTimeStamp::next());
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
			let ts = super::HlcTimeStamp::next();
			let buf = &mut [0u8; _];
			byte_arrays.push(ts.encode(buf).to_vec());
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
					local_timestamps.push(super::HlcTimeStamp::next());
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
		let ts1 = super::HlcTimeStamp(1000u64 << 16);
		let ts2 = super::HlcTimeStamp((1000u64 << 16) | 1);
		let ts3 = super::HlcTimeStamp(1001u64 << 16);

		assert!(ts1 < ts2);
		assert!(ts2 < ts3);
		assert!(ts1 < ts3);
		assert_eq!(ts1, ts1);
	}
}
