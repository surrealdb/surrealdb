//! Stores the previous value of record for concurrent index building
use std::borrow::Cow;
use std::fmt::Debug;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::kvs::KVKey;
use crate::kvs::index::PrimaryAppending;
use crate::val::{IndexFormat, RecordIdKey, TableName};

#[derive(Debug, Clone, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "IndexFormat")]
pub(crate) struct Ip<'a> {
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

impl KVKey for Ip<'_> {
	type ValueType = PrimaryAppending;
	fn encode_key(&self) -> ::anyhow::Result<Vec<u8>> {
		Ok(storekey::encode_vec_format::<IndexFormat, _>(self)
			.map_err(|_| crate::err::Error::Unencodable)?)
	}
}

impl<'a> Ip<'a> {
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
			_f: b'i',
			_g: b'p',
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
		let val = Ip::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			IndexId(3),
			RecordIdKey::String("id".into()),
		);
		let enc = Ip::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ip\x03id\0",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
