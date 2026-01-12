//! Stores Things of an HNSW index
use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::val::{IndexFormat, RecordIdKey, TableName};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "IndexFormat")]
pub(crate) struct Hi<'a> {
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
	pub id: RecordIdKey,
}

impl crate::kvs::KVKey for Hi<'_> {
	type ValueType = u64;
	fn encode_key(&self) -> ::anyhow::Result<Vec<u8>> {
		Ok(::storekey::encode_vec_format::<IndexFormat, _>(self)
			.map_err(|_| crate::err::Error::Unencodable)?)
	}
}

impl<'a> Hi<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		id: RecordIdKey,
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
			_g: b'i',
			id,
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
		let val = Hi::new(
			NamespaceId(1),
			DatabaseId(2),
			&tb,
			IndexId(3),
			RecordIdKey::String("testid".into()),
		);
		let enc = Hi::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hi\x03testid\0",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
