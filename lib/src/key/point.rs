use crate::err::Error;
use crate::sql::value::Value;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Point {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
	_d: u8,
	pub ix: String,
	pub fd: Value,
	pub id: String,
}

impl From<Point> for Vec<u8> {
	fn from(val: Point) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Point {
	fn from(val: Vec<u8>) -> Self {
		Point::decode(&val).unwrap()
	}
}

impl From<&Vec<u8>> for Point {
	fn from(val: &Vec<u8>) -> Self {
		Point::decode(val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str, ix: &str, fd: Value, id: &str) -> Point {
	Point::new(ns.to_string(), db.to_string(), tb.to_string(), ix.to_string(), fd, id.to_string())
}

pub fn prefix(ns: &str, db: &str, tb: &str, ix: &str) -> Vec<u8> {
	let mut k = super::guide::new(ns, db, tb, ix).encode().unwrap();
	k.extend_from_slice(&[0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str, ix: &str) -> Vec<u8> {
	let mut k = super::guide::new(ns, db, tb, ix).encode().unwrap();
	k.extend_from_slice(&[0xff]);
	k
}

impl Point {
	pub fn new(ns: String, db: String, tb: String, ix: String, fd: Value, id: String) -> Point {
		Point {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0xa4, // ¤
			ix,
			fd,
			id,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Point, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Point::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".into(),
			"test".into(),
		);
		let enc = Point::encode(&val).unwrap();
		let dec = Point::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
