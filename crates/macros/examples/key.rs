// cargo expand --example key

use serde::{Deserialize, Serialize};
use surrealdb_macros::Key;

mod err {
	#[derive(Debug)]
	pub struct Error;

	impl From<storekey::encode::Error> for Error {
		fn from(_: storekey::encode::Error) -> Self {
			unimplemented!();
		}
	}

	impl From<storekey::decode::Error> for Error {
		fn from(_: storekey::decode::Error) -> Self {
			unimplemented!();
		}
	}
}

#[derive(Serialize, Deserialize, Key)]
pub struct NsOwned {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ns: String,
}

/// WIP: Support for borrowed keys.
#[derive(Serialize, Deserialize, Key)]
pub struct NsBorrowed<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ns: &'a str,
}
fn main() {}
