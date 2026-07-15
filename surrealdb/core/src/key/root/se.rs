//! Stores durable RPC session state
use uuid::Uuid;

use crate::dbs::DurableSession;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	// Represents the durable copy of a client-attached RPC session, so the
	// session survives the process that attached it and is reachable from
	// any cluster node sharing the datastore.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Se {
		b'/',
		b'!',
		b's',
		b'e',
		pub id: Uuid,
	}
}
impl_kv_key_storekey!(Se => DurableSession);

impl Categorise for Se {
	fn categorise(&self) -> Category {
		Category::RpcSession
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct SePrefix {
		b'/',
		b'!',
		b's',
		b'e',
	}
}
impl_kv_range_storekey!(SePrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let val = Se {
			id: Uuid::default(),
		};
		let enc = val.encode_key().unwrap();
		assert_eq!(&*enc, b"/!se\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
	}

	#[test]
	fn test_prefix() {
		let val = SePrefix {}.encode_range().unwrap();
		assert_eq!(val.start.as_slice(), b"/!se\0");
		assert_eq!(val.end.as_slice(), b"/!sf");
	}
}
