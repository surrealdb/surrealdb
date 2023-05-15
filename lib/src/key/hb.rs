use crate::sql::cluster_timestamp::{KeyTimestamp, Timestamp};
use derive::Key;
use serde::{Deserialize, Serialize};
use std::io::empty;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Hb {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	_d: u8,
	pub hb: Timestamp,
	pub nd: Uuid,
}

pub fn new(hb: Timestamp, nd: &Uuid) -> Hb {
	Hb::new(hb, nd.to_owned())
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
		let mut k = super::kv::new().encode().unwrap();
		k.extend_from_slice(&[b'!', b'h', b'b', 0x00]);
		k
	}

	pub fn suffix(ts: &Timestamp) -> Vec<u8> {
		let mut k = super::kv::new().encode().unwrap();
		k.extend_from_slice(&[b'!', b'h', b'b']);
		let tskey: KeyTimestamp = ts.into();
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
}
