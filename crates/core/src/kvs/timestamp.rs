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
		Ok(version as u64)
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

impl Timestamp for u128 {
	/// Convert the timestamp to a version
	fn to_versionstamp(&self) -> u128 {
		*self
	}
	/// Create a timestamp from a version
	fn from_versionstamp(version: u128) -> Result<Self> {
		Ok(version)
	}
	/// Convert the timestamp to a datetime
	fn to_datetime(&self) -> DateTime<Utc> {
		DateTime::from_timestamp_nanos(*self as i64)
	}
	/// Create a timestamp from a datetime
	fn from_datetime(datetime: DateTime<Utc>) -> Result<Self> {
		Ok(datetime.timestamp_nanos() as u128)
	}
	/// Convert the timestamp to a byte array
	fn to_ts_bytes(&self) -> Vec<u8> {
		self.to_be_bytes().to_vec()
	}
	/// Create a timestamp from a byte array
	fn from_ts_bytes(bytes: &[u8]) -> Result<Self> {
		match bytes.try_into() {
			Ok(v) => Ok(u128::from_be_bytes(v)),
			Err(_) => Err(Error::TimestampInvalid("timestamp should be 16 bytes".to_string())),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use chrono::TimeZone;

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
	fn test_u128_bytes_roundtrip() {
		let values = [0u128, 1, 42, u64::MAX as u128, u128::MAX / 2, u128::MAX];

		for &value in &values {
			let bytes = value.to_ts_bytes();
			let recovered = u128::from_ts_bytes(&bytes).unwrap();
			assert_eq!(value, recovered, "Failed roundtrip for u128 value {}", value);
		}
	}

	#[test]
	fn test_u128_bytes_length() {
		let value = 12345u128;
		let bytes = value.to_ts_bytes();
		assert_eq!(bytes.len(), 16, "u128 timestamp should be 16 bytes");
	}

	#[test]
	fn test_u128_bytes_big_endian() {
		// Verify big-endian encoding for lexicographic ordering
		let small = 100u128;
		let large = 1000u128;
		let small_bytes = small.to_ts_bytes();
		let large_bytes = large.to_ts_bytes();
		assert!(small_bytes < large_bytes, "Bytes should be lexicographically ordered");
	}

	#[test]
	fn test_u128_bytes_invalid_length() {
		let too_short = vec![0u8; 8];
		let result = u128::from_ts_bytes(&too_short);
		assert!(result.is_err(), "Should fail with invalid byte length");

		let too_long = vec![0u8; 32];
		let result = u128::from_ts_bytes(&too_long);
		assert!(result.is_err(), "Should fail with invalid byte length");
	}

	#[test]
	fn test_u128_versionstamp_roundtrip() {
		let values = [0u128, 1, 42, u64::MAX as u128, u128::MAX / 2, u128::MAX];

		for &value in &values {
			let version = value.to_versionstamp();
			let recovered = u128::from_versionstamp(version).unwrap();
			assert_eq!(value, recovered, "Failed versionstamp roundtrip for u128 value {}", value);
		}
	}

	#[test]
	fn test_u128_datetime_roundtrip() {
		// Test with various timestamps
		let now = Utc::now();
		let ts = u128::from_datetime(now).unwrap();
		let recovered = ts.to_datetime();

		// DateTime roundtrip should be within reasonable precision
		assert_eq!(now.timestamp_nanos(), recovered.timestamp_nanos(), "Failed datetime roundtrip");
	}

	#[test]
	fn test_u128_datetime_specific_values() {
		// Test epoch
		let epoch = Utc.timestamp_opt(0, 0).unwrap();
		let ts = u128::from_datetime(epoch).unwrap();
		let recovered = ts.to_datetime();
		assert_eq!(epoch.timestamp_nanos(), recovered.timestamp_nanos());

		// Test a known timestamp
		let known_time = Utc.timestamp_opt(1700000000, 123456789).unwrap();
		let ts = u128::from_datetime(known_time).unwrap();
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
}
