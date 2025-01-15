//! vs is a module to handle Versionstamps.
//! This module is supplemental to the kvs::tx module and is not intended to be used directly
//! by applications.
//! This module might be migrated into the kvs or kvs::tx module in the future.

pub use std::{error, fmt, mem};

use revision::Revisioned;

/// Versionstamp is a 10-byte array used to identify a specific version of a key.
/// The first 8 bytes are significant (the u64), and the remaining 2 bytes are not significant, but used for extra precision.
/// To convert to and from this module, see the conv module in this same directory.
///
/// You're going to want these
/// 65536
/// 131072
/// 196608
/// 262144
/// 327680
/// 393216
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug, PartialOrd)]
pub struct VersionStamp([u8; 10]);

impl serde::Serialize for VersionStamp {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		self.0.serialize(serializer)
	}
}

impl<'de> serde::Deserialize<'de> for VersionStamp {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let res = <[u8; 10]>::deserialize(deserializer)?;
		Ok(VersionStamp(res))
	}
}

// Version stamp was previously a normal array so it doesn't have any kind of revision tracking and
// is serialized just like any other array.
impl Revisioned for VersionStamp {
	fn revision() -> u16 {
		0
	}

	fn serialize_revisioned<W: std::io::Write>(&self, w: &mut W) -> Result<(), revision::Error> {
		w.write_all(&self.0).map_err(revision::Error::Io)?;
		Ok(())
	}

	fn deserialize_revisioned<R: std::io::Read>(r: &mut R) -> Result<Self, revision::Error>
	where
		Self: Sized,
	{
		let mut buf = [0u8; 10];
		r.read_exact(&mut buf).map_err(revision::Error::Io)?;
		Ok(VersionStamp(buf))
	}
}

impl Default for VersionStamp {
	fn default() -> Self {
		VersionStamp::ZERO
	}
}

pub struct VersionStampError(());

impl fmt::Display for VersionStampError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Invalid version stamp conversion")
	}
}
impl fmt::Debug for VersionStampError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		fmt::Display::fmt(self, f)
	}
}
impl error::Error for VersionStampError {}

impl VersionStamp {
	pub const ZERO: VersionStamp = VersionStamp([0; 10]);

	pub fn from_u64(v: u64) -> Self {
		let mut buf = [0; 10];
		buf[0..8].copy_from_slice(&v.to_be_bytes());
		VersionStamp(buf)
	}

	pub fn from_u64_u16(v: u64, v2: u16) -> Self {
		let mut buf = [0; 10];
		buf[0..8].copy_from_slice(&v.to_be_bytes());
		buf[8..10].copy_from_slice(&v2.to_be_bytes());
		VersionStamp(buf)
	}

	pub fn try_from_u128(v: u128) -> Result<Self, VersionStampError> {
		if (v >> 80) > 0 {
			return Err(VersionStampError(()));
		}
		let bytes = v.to_be_bytes();
		let mut res = [0u8; 10];
		res.copy_from_slice(&bytes[6..16]);
		Ok(VersionStamp(res))
	}

	/// Convert the VersionStamp into a u64 ignoring the 2 normally zero bytes.
	pub fn into_u64_u16(self) -> (u64, u16) {
		let mut u64_bytes = [0; 8];
		u64_bytes.copy_from_slice(&self.0[0..8]);
		let mut u16_bytes = [0; 2];
		u16_bytes.copy_from_slice(&self.0[8..10]);
		(u64::from_be_bytes(u64_bytes), u16::from_be_bytes(u16_bytes))
	}

	/// Convert the VersionStamp into a u64 ignoring the 2 normally zero bytes.
	pub fn into_u64_lossy(self) -> u64 {
		let mut bytes = [0; 8];
		bytes.copy_from_slice(&self.0[0..8]);
		u64::from_be_bytes(bytes)
	}

	pub fn try_into_u64(self) -> Result<u64, VersionStampError> {
		if self.0[8] > 0 || self.0[9] > 0 {
			return Err(VersionStampError(()));
		}
		Ok(self.into_u64_lossy())
	}

	pub fn into_u128(self) -> u128 {
		let mut bytes = [0; 16];
		bytes[6..16].copy_from_slice(&self.0);
		u128::from_be_bytes(bytes)
	}

	pub fn as_bytes(self) -> [u8; 10] {
		self.0
	}

	pub fn from_bytes(bytes: [u8; 10]) -> Self {
		Self(bytes)
	}

	pub fn from_slice(slice: &[u8]) -> Result<Self, VersionStampError> {
		if slice.len() != 10 {
			return Err(VersionStampError(()));
		}
		let mut bytes = [0u8; 10];
		bytes.copy_from_slice(slice);
		Ok(Self::from_bytes(bytes))
	}

	/// Returns an iterator of version stamps starting with the current version stamp.
	pub fn iter(self) -> VersionStampIter {
		VersionStampIter {
			cur: self,
		}
	}
}

pub struct VersionStampIter {
	cur: VersionStamp,
}

impl Iterator for VersionStampIter {
	type Item = VersionStamp;

	fn next(&mut self) -> Option<Self::Item> {
		let (v, suffix) = self.cur.into_u64_u16();
		let v = v.checked_add(1)?;
		let next = VersionStamp::from_u64_u16(v, suffix);
		Some(mem::replace(&mut self.cur, next))
	}
}

#[cfg(test)]
mod test {
	use super::VersionStamp;

	#[test]
	pub fn generate_one_vs() {
		let vs = VersionStamp::ZERO.iter().take(1).collect::<Vec<_>>();
		assert_eq!(vs.len(), 1, "Should be 1, but was {:?}", vs);
		assert_eq!(vs[0], VersionStamp::ZERO);
	}

	#[test]
	pub fn generate_two_vs_in_sequence() {
		let vs = VersionStamp::from_bytes([0, 0, 0, 0, 0, 0, 0, 1, 0, 0]).iter().flat_map(|vs| {
			let skip_because_first_is_equal = 1;
			vs.iter().skip(skip_because_first_is_equal).map(move |vs2| (vs, vs2))
		});
		let versionstamps = vs.take(4).collect::<Vec<(VersionStamp, VersionStamp)>>();

		assert_eq!(
			versionstamps.len(),
			4,
			"We expect the combinations to be 2x2 matrix, but was {:?}",
			versionstamps
		);

		let acceptable_values = [65536u128, 131072, 196608, 262144, 327680, 393216];
		for (first, second) in versionstamps {
			assert!(first < second, "First: {:?}, Second: {:?}", first, second);
			let first = first.into_u128();
			let second = second.into_u128();
			assert!(acceptable_values.contains(&first));
			assert!(acceptable_values.contains(&second));
		}
	}

	#[test]
	pub fn iteration_stops_past_end() {
		let mut iter = VersionStamp::from_bytes([255; 10]).iter();
		assert!(iter.next().is_some());
		assert!(iter.next().is_none());
	}

	#[test]
	fn try_to_u64_be() {
		use super::*;
		// Overflow
		let v = VersionStamp::from_bytes([255, 255, 255, 255, 255, 255, 255, 255, 0, 1]);
		let res = v.try_into_u64();
		assert!(res.is_err());
		// No overflow
		let v = VersionStamp::from_bytes([255, 255, 255, 255, 255, 255, 255, 255, 0, 0]);
		let res = v.try_into_u64().unwrap();
		assert_eq!(res, u64::MAX);
	}

	#[test]
	fn try_u128_to_versionstamp() {
		use super::*;
		// Overflow
		let v = u128::MAX;
		let res = VersionStamp::try_from_u128(v);
		assert!(res.is_err());
		// No overflow
		let v = u128::MAX >> 48;
		let res = VersionStamp::try_from_u128(v).unwrap();
		assert_eq!(res, [255, 255, 255, 255, 255, 255, 255, 255, 255, 255]);
	}

	#[test]
	fn can_add_u64_conversion() {
		let start = 5u64;
		let vs = VersionStamp::from_u64(start);
		// The last 2 bytes are empty
		assert_eq!("00000000000000050000", hex::encode(vs.as_bytes()));
		let mid = vs.try_into_u64().unwrap();
		assert_eq!(start, mid);
		let mid = mid + 1;
		let vs = VersionStamp::from_u64(mid);
		// The last 2 bytes are empty
		assert_eq!("00000000000000060000", hex::encode(vs));
		let end = vs.try_into_u64().unwrap();
		assert_eq!(end, 6);
	}
}
