use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct St {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	_d: u8,
	_e: u8,
	pub sc: String,
	_f: u8,
	_g: u8,
	_h: u8,
	pub tk: String,
}

impl From<St> for Vec<u8> {
	fn from(val: St) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for St {
	fn from(val: Vec<u8>) -> Self {
		St::decode(&val).unwrap()
	}
}

impl From<&Vec<u8>> for St {
	fn from(val: &Vec<u8>) -> Self {
		St::decode(val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, sc: &str, tk: &str) -> St {
	St::new(ns.to_string(), db.to_string(), sc.to_string(), tk.to_string())
}

pub fn prefix(ns: &str, db: &str, sc: &str) -> Vec<u8> {
	let mut k = super::sc::new(ns, db, sc).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x74, 0x6b, 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, sc: &str) -> Vec<u8> {
	let mut k = super::sc::new(ns, db, sc).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x74, 0x6b, 0xff]);
	k
}

impl St {
	pub fn new(ns: String, db: String, sc: String, tk: String) -> St {
		St {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x21, // !
			_d: 0x73, // s
			_e: 0x74, // t
			sc,
			_f: 0x21, // !
			_g: 0x74, // t
			_h: 0x6b, // k
			tk,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		crate::sql::serde::beg_internal_serialization();
		let v = serialize(self);
		crate::sql::serde::end_internal_serialization();
		Ok(v?)
	}
	pub fn decode(v: &[u8]) -> Result<St, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = St::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = St::encode(&val).unwrap();
		let dec = St::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
