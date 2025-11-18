/// A monotonic timestamp which is represented differently depending on the storage backend. The
/// timestamp should be unique and monotonic, and should serialize lexicographically to a vector of
/// bytes.
pub trait Timestamp: Send + Sync {
	/// Convert the timestamp to a byte array
	fn to_ts_bytes(&self) -> Vec<u8>;
	/// Create a timestamp from a byte array
	fn from_ts_bytes(bytes: &[u8]) -> Self
	where
		Self: Sized;
}

impl Timestamp for Vec<u8> {
	/// Convert the timestamp to a byte array
	fn to_ts_bytes(&self) -> Vec<u8> {
		self.clone()
	}
	/// Create a timestamp from a byte array
	fn from_ts_bytes(bytes: &[u8]) -> Self {
		bytes.to_vec()
	}
}

impl Timestamp for u64 {
	/// Convert the timestamp to a byte array
	fn to_ts_bytes(&self) -> Vec<u8> {
		self.to_be_bytes().to_vec()
	}
	/// Create a timestamp from a byte array
	fn from_ts_bytes(bytes: &[u8]) -> Self {
		u64::from_be_bytes(bytes.try_into().expect("timestamp should be 8 bytes"))
	}
}

impl Timestamp for u128 {
	/// Convert the timestamp to a byte array
	fn to_ts_bytes(&self) -> Vec<u8> {
		self.to_be_bytes().to_vec()
	}
	/// Create a timestamp from a byte array
	fn from_ts_bytes(bytes: &[u8]) -> Self {
		u128::from_be_bytes(bytes.try_into().expect("timestamp should be 16 bytes"))
	}
}
