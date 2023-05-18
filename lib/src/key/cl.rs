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
	pub nd: Uuid,
}

impl Cl {
	pub fn new(nd: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'c',
			_c: b'l',
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
