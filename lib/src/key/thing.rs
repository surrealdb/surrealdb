use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Thing {
	__: char,
	_a: char,
	pub ns: String,
	_b: char,
	pub db: String,
	_c: char,
	pub tb: String,
	_d: char,
	pub id: String,
}

impl From<Thing> for Vec<u8> {
	fn from(val: Thing) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Thing {
	fn from(val: Vec<u8>) -> Self {
		Thing::decode(&val).unwrap()
	}
}

impl From<&Vec<u8>> for Thing {
	fn from(val: &Vec<u8>) -> Self {
		Thing::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str, id: &str) -> Thing {
	Thing::new(ns.to_string(), db.to_string(), tb.to_string(), id.to_string())
}

impl Thing {
	pub fn new(ns: String, db: String, tb: String, id: String) -> Thing {
		Thing {
			__: '/',
			_a: '*',
			ns,
			_b: '*',
			db,
			_c: '*',
			tb,
			_d: '*',
			id,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Thing, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Thing::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".into(),
		);
		let enc = Thing::encode(&val).unwrap();
		let dec = Thing::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
