use crate::dbs::cl::{KeyTimestamp, Timestamp};
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
	pub nd: Uuid,
}

impl Hb {
	pub fn new(hb: Timestamp, nd: Uuid) -> Self {
		// lib setup has override for warning and prefix/suffix is technically dead code for some reason
		// let _ = Hb::prefix();
		// let _ = Hb::suffix(&hb);
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
		let mut k = super::kv::new().encode().unwrap();
		k.extend_from_slice(&[b'!', b'h', b'b', 0x00]);
		k
	}

	pub fn suffix(ts: &Timestamp) -> Vec<u8> {
		// Add one to timestmap so we get a complete range inclusive of provided timestamp
		// Also convert type
		let tskey: KeyTimestamp = KeyTimestamp {
			value: ts.value + 1,
		};
		let mut k = super::kv::new().encode().unwrap();
		k.extend_from_slice(&[b'!', b'h', b'b']);
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
            Uuid::default(),
        );
		let enc = Hb::encode(&val).unwrap();
		let dec = Hb::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	// fn prefix() {
	// 	use super::*;
	// 	let actual = Hb::prefix();
	// 	let expected = vec![b'/', b'!', b'h', b'b', 0];
	// 	assert_eq!(actual, expected)
	// }
	//
	// fn suffix() {
	// 	use super::*;
	// 	let ts: Timestamp = Timestamp {
	// 		value: 456,
	// 	};
	// 	let actual = Hb::suffix(&ts);
	// 	let expected = vec![b'/', b'!', b'h', b'b', 0]; // Incorrect, should be adjusted
	// 	assert_eq!(actual, expected)
	// }
}
