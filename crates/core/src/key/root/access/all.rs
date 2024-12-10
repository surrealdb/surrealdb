//! Stores the key prefix for all keys under a root access method
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Access<'a> {
	__: u8,
	_a: u8,
	pub ac: &'a str,
}

pub fn new(ac: &str) -> Access {
	Access::new(ac)
}

impl Categorise for Access<'_> {
	fn categorise(&self) -> Category {
		Category::AccessRoot
	}
}

impl<'a> Access<'a> {
	pub fn new(ac: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'&',
			ac,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Access::new(
			"testac",
		);
		let enc = Access::encode(&val).unwrap();
		assert_eq!(enc, b"/&testac\0");

		let dec = Access::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
