use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use crate::sql::value::Value;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Point {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	db: String,
	_c: String,
	tb: String,
	_d: String,
	ix: String,
	fd: Value,
	id: String,
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

pub fn new(ns: &str, db: &str, tb: &str, ix: &str, fd: Value, id: &str) -> Point {
	Point::new(ns.to_string(), db.to_string(), tb.to_string(), ix.to_string(), fd, id.to_string())
}

impl Point {
	pub fn new(ns: String, db: String, tb: String, ix: String, fd: Value, id: String) -> Point {
		Point {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("*"),
			db,
			_c: String::from("*"),
			tb,
			_d: String::from("¤"),
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
