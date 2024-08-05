//! Stores a Session as a reference that is accessed by live queries
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The Se key is used to quickly update a shared session used by all live queries that share it
///
/// The value is the serialised Session that contains authentication and permissions information
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Se {
	__: u8,
	_a: u8,
	#[serde(with = "uuid::serde::compact")]
	pub nd: Uuid,
	_b: u8,
	_c: u8,
	_d: u8,
	#[serde(with = "uuid::serde::compact")]
	pub se: Uuid,
}

pub fn new(nd: Uuid, se: Uuid) -> Se {
	Se::new(nd, se)
}

pub fn prefix_se(nd: &Uuid) -> Vec<u8> {
	let mut k = [b'/', b'$'].to_vec();
	k.extend_from_slice(nd.as_bytes());
	k.extend_from_slice(&[b'!', b's', b'e', 0x00]);
	k
}

pub fn suffix_se(nd: &Uuid) -> Vec<u8> {
	let mut k = [b'/', b'$'].to_vec();
	k.extend_from_slice(nd.as_bytes());
	k.extend_from_slice(&[b'!', b's', b'e']);
	k.extend_from_slice(&[0xff; 16]);
	k
}

impl KeyRequirements for Se {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::NodeLiveQuery
	}
}

impl Se {
	pub fn new(nd: Uuid, se: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'$',
			nd,
			_b: b'!',
			_c: b's',
			_d: b'e',
			se,
		}
	}
}

#[cfg(test)]
mod tests {

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
        let nd = Uuid::from_bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10]);
		#[rustfmt::skip]
        let se = Uuid::from_bytes([0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20]);
		let val = Se::new(nd, se);
		let enc = Se::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/$\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\
			!se\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x20\
			"
		);

		let dec = Se::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix_se() {
		use super::*;
		let nd = Uuid::from_bytes([
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
			0x0f, 0x10,
		]);
		let val = prefix_se(&nd);
		assert_eq!(
			val,
			b"/$\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x00!se\x00"
		);
	}

	#[test]
	fn suffix_nd() {
		use super::*;
		let nd = Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
		let val = suffix_se(&nd);
		assert_eq!(
			val,
			b"/$\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\xff!se
		\xff\xff\xff\xff
		\xff\xff\xff\xff
		\xff\xff\xff\xff
		\xff\xff\xff\xff
		"
		);
	}
}
