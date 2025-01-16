use crate::vs::Versionstamp;
use num_traits::ToBytes;
use std::fmt;
use thiserror::Error;

// u64_to_versionstamp converts a u64 to a 10-byte versionstamp
// assuming big-endian and the the last two bytes are zero.
pub fn u64_to_versionstamp(v: u64) -> [u8; 10] {
	let mut buf = [0; 10];
	buf[0] = (v >> 56) as u8;
	buf[1] = (v >> 48) as u8;
	buf[2] = (v >> 40) as u8;
	buf[3] = (v >> 32) as u8;
	buf[4] = (v >> 24) as u8;
	buf[5] = (v >> 16) as u8;
	buf[6] = (v >> 8) as u8;
	buf[7] = v as u8;
	buf
}

#[allow(unused)]
// u64_u16_to_versionstamp converts a u64 and a u16 to a 10-byte versionstamp
// assuming big-endian.
pub fn u64_u16_to_versionstamp(v: u64, v2: u16) -> [u8; 10] {
	let mut buf = [0; 10];
	buf[0] = (v >> 56) as u8;
	buf[1] = (v >> 48) as u8;
	buf[2] = (v >> 40) as u8;
	buf[3] = (v >> 32) as u8;
	buf[4] = (v >> 24) as u8;
	buf[5] = (v >> 16) as u8;
	buf[6] = (v >> 8) as u8;
	buf[7] = v as u8;
	buf[8] = (v2 >> 8) as u8;
	buf[9] = v2 as u8;
	buf
}

#[allow(unused)]
// u64_u16_to_versionstamp converts a u64 and a u16 to a 10-byte versionstamp
// assuming big-endian.
pub fn u16_u64_to_versionstamp(v: u16, v2: u64) -> [u8; 10] {
	let mut buf = [0; 10];
	buf[0] = (v >> 8) as u8;
	buf[1] = v as u8;
	buf[2] = (v2 >> 56) as u8;
	buf[3] = (v2 >> 48) as u8;
	buf[4] = (v2 >> 40) as u8;
	buf[5] = (v2 >> 32) as u8;
	buf[6] = (v2 >> 24) as u8;
	buf[7] = (v2 >> 16) as u8;
	buf[8] = (v2 >> 8) as u8;
	buf[9] = v2 as u8;
	buf
}

// u128_to_versionstamp converts a u128 to a 10-byte versionstamp
// assuming big-endian.
#[allow(unused)]
pub fn try_u128_to_versionstamp(v: u128) -> Result<[u8; 10], Error> {
	if v >> 80 > 0 {
		return Err(Error::InvalidVersionstamp);
	}

	let mut buf = [0; 10];
	buf[0] = (v >> 72) as u8;
	buf[1] = (v >> 64) as u8;
	buf[2] = (v >> 56) as u8;
	buf[3] = (v >> 48) as u8;
	buf[4] = (v >> 40) as u8;
	buf[5] = (v >> 32) as u8;
	buf[6] = (v >> 24) as u8;
	buf[7] = (v >> 16) as u8;
	buf[8] = (v >> 8) as u8;
	buf[9] = v as u8;
	Ok(buf)
}

/// Take the most significant, time-based bytes and ignores the last 2 bytes
///
/// You probably want `to_u128_be` instead
pub fn versionstamp_to_u64(vs: &Versionstamp) -> u64 {
	u64::from_be_bytes(vs[..8].try_into().unwrap())
}
// to_u128_be converts a 10-byte versionstamp to a u128 assuming big-endian.
// This is handy for human comparing versionstamps.
// This is not the same as timestamp u64 representation as the tailing bytes are included
#[allow(unused)]
pub fn to_u128_be(vs: [u8; 10]) -> u128 {
	let mut buf = [0; 16];
	let mut i = 0;
	while i < 10 {
		buf[i + 6] = vs[i];
		i += 1;
	}
	u128::from_be_bytes(buf)
}

#[derive(Error)]
#[non_exhaustive]
pub enum Error {
	#[error("invalid versionstamp")]
	// InvalidVersionstamp is returned when a versionstamp has an unexpected length.
	InvalidVersionstamp,
}

impl fmt::Debug for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Error::InvalidVersionstamp => write!(f, "invalid versionstamp"),
		}
	}
}

// to_u64_be converts a 10-byte versionstamp to a u64 assuming big-endian.
// Only the first 8 bytes are used.
#[allow(unused)]
pub fn try_to_u64_be(vs: [u8; 10]) -> Result<u64, Error> {
	let mut buf = [0; 8];
	let mut i = 0;
	while i < 8 {
		buf[i] = vs[i];
		i += 1;
	}
	if vs[8] != 0 || vs[9] != 0 {
		return Err(Error::InvalidVersionstamp);
	}
	Ok(u64::from_be_bytes(buf))
}

// to_u128_le converts a 10-byte versionstamp to a u128 assuming little-endian.
// This is handy for producing human-readable versions of versionstamps.
#[allow(unused)]
pub fn to_u128_le(vs: [u8; 10]) -> u128 {
	let mut buf = [0; 16];
	let mut i = 0;
	while i < 10 {
		buf[i] = vs[i];
		i += 1;
	}
	u128::from_be_bytes(buf)
}

#[cfg(test)]
mod tests {
	use crate::vs::{u64_to_versionstamp, versionstamp_to_u64};

	#[test]
	fn try_to_u64_be() {
		use super::*;
		// Overflow
		let v = [255, 255, 255, 255, 255, 255, 255, 255, 0, 1];
		let res = try_to_u64_be(v);
		assert!(res.is_err());
		// No overflow
		let v = [255, 255, 255, 255, 255, 255, 255, 255, 0, 0];
		let res = try_to_u64_be(v).unwrap();
		assert_eq!(res, u64::MAX);
	}

	#[test]
	fn try_u128_to_versionstamp() {
		use super::*;
		// Overflow
		let v = u128::MAX;
		let res = try_u128_to_versionstamp(v);
		assert!(res.is_err());
		// No overflow
		let v = u128::MAX >> 48;
		let res = try_u128_to_versionstamp(v).unwrap();
		assert_eq!(res, [255, 255, 255, 255, 255, 255, 255, 255, 255, 255]);
	}

	#[test]
	fn can_add_u64_conversion() {
		let start = 5u64;
		let vs = u64_to_versionstamp(start);
		// The last 2 bytes are empty
		assert_eq!("00000000000000050000", hex::encode(vs));
		let mid = versionstamp_to_u64(&vs);
		assert_eq!(start, mid);
		let mid = mid + 1;
		let vs = u64_to_versionstamp(mid);
		// The last 2 bytes are empty
		assert_eq!("00000000000000060000", hex::encode(vs));
		let end = versionstamp_to_u64(&vs);
		assert_eq!(end, 6);
	}
}
