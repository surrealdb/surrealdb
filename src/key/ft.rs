use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Ft {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	db: String,
	_d: String,
	tb: String,
	_c: String,
	ft: String,
}

pub fn new(ns: &str, db: &str, tb: &str, ft: &str) -> Ft {
	Ft::new(ns.to_string(), db.to_string(), tb.to_string(), ft.to_string())
}

impl Ft {
	pub fn new(ns: String, db: String, tb: String, ft: String) -> Ft {
		Ft {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("*"),
			db,
			_c: String::from("*"),
			tb,
			_d: String::from("!ft"),
			ft,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Ft, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ft::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Ft::encode(&val).unwrap();
		let dec = Ft::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
