use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::hnsw::VectorPendingUpdate;
use crate::kvs::impl_kv_key_storekey;
use crate::val::{RecordIdKey, TableName};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct HnswPending<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	pub id: Cow<'a, RecordIdKey>,
}

impl_kv_key_storekey!(HnswPending<'_> => VectorPendingUpdate);

impl<'a> HnswPending<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		id: &'a RecordIdKey,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'+',
			ix,
			_e: b'!',
			_f: b'h',
			_g: b'p',
			id: Cow::Borrowed(id),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let id = RecordIdKey::String("testid".into());
		let val = HnswPending::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), &id);
		let enc = HnswPending::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hp\x03testid\0",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
