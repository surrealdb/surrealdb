use derive::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Represents cluster information.
// In the future, this could also include broadcast addresses and other information.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Cl {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	#[serde(with = "uuid::serde::compact")]
	pub nd: Uuid,
}

impl Cl {
	pub fn new(nd: Uuid) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x21, // !
			_b: 0x63, // c
			_c: 0x6c, // l
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
            let val = Cl::new(
            Uuid::default(),
        );
		let enc = Cl::encode(&val).unwrap();
		let dec = Cl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
