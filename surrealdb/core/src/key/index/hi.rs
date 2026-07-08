//! Stores RecordIdkey's of an HNSW index
use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKey, KVKeyDecode, key};
use crate::val::{IndexFormat, RecordIdKey, TableName};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Hi<'a> for IndexFormat{
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b'i',
		pub id: Cow<'a, RecordIdKey>,
	}
}

impl KVKey for Hi<'_> {
	type Value = u64;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> ::anyhow::Result<()> {
		storekey::encode_format::<IndexFormat, _, _>(buffer, self)
			.map_err(|_| crate::err::Error::Unencodable)?;
		Ok(())
	}

	fn value_context(&self) {}
}
impl<'a> KVKeyDecode<'a> for Hi<'a> {
	fn decode_key(bytes: &'a [u8]) -> anyhow::Result<Self> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(bytes)
			.map_err(|_| crate::err::Error::Corrupted("HNSW Index key cannot be decoded"))?)
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
		let id = RecordIdKey::String(Strand::new_static("testid"));
		let val = Hi {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			id: Cow::Borrowed(&id),
		};
		let enc = Hi::encode_key(&val).unwrap();
		assert_eq!(
			&*enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hi\x03testid\0",
			"got: {}",
			String::from_utf8_lossy(&enc)
		);
	}
}
