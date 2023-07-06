use crate::dbs::cl::Timestamp;
use derive::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Hb {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	_d: u8,
	pub hb: Timestamp,
	#[serde(with = "uuid::serde::compact")]
	pub nd: Uuid,
}

impl Hb {
	pub fn new(hb: Timestamp, nd: Uuid) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x21, // !
			_b: 0x68, // h
			_c: 0x62, // b
			hb,
			_d: 0x2f, // /
			nd,
		}
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
