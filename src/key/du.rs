use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Du {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	db: String,
	_c: String,
	us: String,
}

pub fn new(ns: &str, db: &str, us: &str) -> Du {
	Du::new(ns.to_string(), db.to_string(), us.to_string())
}

impl Du {
	pub fn new(ns: String, db: String, us: String) -> Du {
		Du {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("*"),
			db,
			_c: String::from("!us"),
			us,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Du, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Du::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Du::encode(&val).unwrap();
		let dec = Du::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
