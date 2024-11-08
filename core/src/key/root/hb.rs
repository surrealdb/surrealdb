//! Stores a heartbeat per registered cluster node
use crate::dbs::node::{KeyTimestamp, Timestamp};
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Hb {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub hb: Timestamp,
	_d: u8,
	#[serde(with = "uuid::serde::compact")]
	pub nd: Uuid,
}

impl KeyRequirements for Hb {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::Heartbeat
	}
}

impl Hb {
	pub fn new(hb: Timestamp, nd: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'h',
			_c: b'b',
			hb,
			_d: b'/',
			nd,
		}
	}

	pub fn prefix() -> Vec<u8> {
		let mut k = crate::key::root::all::new().encode().unwrap();
		k.extend_from_slice(&[b'!', b'h', b'b', 0x00]);
		k
	}

	pub fn suffix(ts: &Timestamp) -> Vec<u8> {
		// Add one to timestamp so we get a complete range inclusive of provided timestamp
		// Also convert type
		let tskey: KeyTimestamp = KeyTimestamp {
			value: ts.value + 1,
		};
		let mut k = crate::key::root::all::new().encode().unwrap();
		k.extend_from_slice(b"!hb");
		k.extend_from_slice(tskey.encode().unwrap().as_ref());
		k
	}
}

impl From<Timestamp> for Hb {
	fn from(ts: Timestamp) -> Self {
		let empty_uuid = uuid::Uuid::nil();
		Self::new(ts, empty_uuid)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
            let val = Hb::new(
            Timestamp { value: 123 },
            Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
        );
		let enc = Hb::encode(&val).unwrap();
		assert_eq!(
            enc,
            b"/!hb\x00\x00\x00\x00\x00\x00\x00\x7b/\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10");
		let dec = Hb::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		use super::*;
		let actual = Hb::prefix();
		assert_eq!(actual, b"/!hb\x00")
	}

	#[test]
	fn suffix() {
		use super::*;
		let ts: Timestamp = Timestamp {
			value: 456,
		};
		let actual = Hb::suffix(&ts);
		assert_eq!(actual, b"/!hb\x00\x00\x00\x00\x00\x00\x01\xc9") // 457, because we add 1 to the timestamp
	}
}
