//! Stores the previous value of record for concurrent index building
use std::borrow::Cow;
use std::fmt::Debug;

use crate::catalog::IndexId;
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKey, KVKeyDecode, key};
use crate::kvs::index::PrimaryAppending;
use crate::val::{IndexFormat, RecordIdKey, TableName};

key! {
	#[derive(Debug, Clone, PartialEq)]
	pub(crate) struct Ip<'a> for IndexFormat{
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'i',
		b'p',
		pub id: RecordIdKey,
	}
}

impl KVKey for Ip<'_> {
	type Value = PrimaryAppending;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> ::anyhow::Result<()> {
		storekey::encode_format::<IndexFormat, _, _>(buffer, self)
			.map_err(|_| crate::key::Error::Unencodable)?;
		Ok(())
	}

	fn value_context(&self) {}
}
impl<'a> KVKeyDecode<'a> for Ip<'a> {
	fn decode_key(bytes: &'a [u8]) -> anyhow::Result<Self> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(bytes).map_err(|_| {
			crate::key::Error::Corrupted("Index previous value key cannot be decoded")
		})?)
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Ip {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			id: RecordIdKey::String(Strand::new_static("id")),
		};
		let enc = Ip::encode_key(&val).unwrap();
		assert_eq!(
			&*enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ip\x03id\0",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
