//! Stores a LIVE SELECT query definition on the cluster
use anyhow::Result;
use uuid::Uuid;

use crate::catalog::NodeLiveQuery;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	/// The Lq key is used to quickly discover which live queries belong to which
	/// nodes This is used in networking for clustered environments such as
	/// discovering if an event is remote or local as well as garbage collection
	/// after dead nodes
	///
	/// The value is just the table of the live query as a String, which is the
	/// missing information from the key path
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Lq {
		b'/',
		b'$',
		pub nd: Uuid,
		b'!',
		b'l',
		b'q',
		pub lq: Uuid,
	}
}

impl_kv_key_storekey!(Lq => NodeLiveQuery);

impl Categorise for Lq {
	fn categorise(&self) -> Category {
		Category::NodeLiveQuery
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct LqPrefix {
		b'/',
		b'$',
		pub nd: Uuid,
		b'!',
		b'l',
		b'q',
	}
}
impl_kv_range_storekey!(LqPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let nd = Uuid::from_bytes([
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
			0x0f, 0x10,
		]);

		let lq = Uuid::from_bytes([
			0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
			0x1f, 0x20,
		]);
		let val = Lq {
			nd,
			lq,
		}
		.encode_key()
		.unwrap();
		assert_eq!(
			val.as_slice(),
			b"/$\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\
			!lq\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x20"
		);
	}

	#[test]
	fn test_prefix() {
		let nd = Uuid::from_bytes([
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
			0x0f, 0x10,
		]);
		let val = LqPrefix {
			nd,
		}
		.encode_range()
		.unwrap();

		assert_eq!(
			val.start.as_slice(),
			b"/$\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\
			!lq\x00"
		);
		assert_eq!(
			val.end.as_slice(),
			b"/$\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\
			!lr"
		);
	}
}
