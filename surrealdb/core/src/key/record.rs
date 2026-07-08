//! Stores a record document
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::Record;
use crate::err::Error;
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKey, KVKeyDecode, impl_kv_range_storekey, key};
use crate::val::{RecordId, RecordIdKey, TableName};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct RecordKey<'a> {
		pub root: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'*',
		pub id: Cow<'a,RecordIdKey>,
	}
}

impl KVKey for RecordKey<'_> {
	type Value = Record;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		storekey::encode(buffer, self).map_err(|_| Error::Unencodable)?;
		Ok(())
	}

	fn value_context(&self) -> RecordId {
		RecordId {
			table: self.tb.as_ref().clone(),
			key: self.id.clone().into_owned(),
		}
	}
}

impl<'a> KVKeyDecode<'a> for RecordKey<'a> {
	fn decode_key(bytes: &'a [u8]) -> Result<Self> {
		Ok(storekey::decode_borrow(bytes).map_err(|_| Error::Corrupted("Record id key"))?)
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct RecordKeyPrefix<'a> {
		pub root: DatabaseRoot,
		b'*',
		pub table: Cow<'a, TableName>,
		b'*',
	}
}
impl_kv_range_storekey!(RecordKeyPrefix<'_>);

#[cfg(test)]
mod tests {

	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::syn;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = RecordKey {
			root: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			id: Cow::Owned(RecordIdKey::String(Strand::new_static("testid"))),
		};
		let enc = RecordKey::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\x03testid\0");
	}
	#[test]
	fn key_complex() {
		//
		let id1 = "foo:['test']";
		let record_id = syn::record_id(id1).expect("Failed to parse the ID");
		let id1 = record_id.key.into();
		let tb = TableName::from("testtb");
		let val = RecordKey {
			root: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			id: Cow::Borrowed(&id1),
		};
		let enc = RecordKey::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\x05\x06test\0\0");

		let id2 = "foo:[u'f8e238f2-e734-47b8-9a16-476b291bd78a']";
		let record_id = syn::record_id(id2).expect("Failed to parse the ID");
		let id2 = record_id.key.into();
		let tb = TableName::from("testtb");
		let val = RecordKey {
			root: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			id: Cow::Borrowed(&id2),
		};
		let enc = RecordKey::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\x05\x09\xf8\xe2\x38\xf2\xe7\x34\x47\xb8\x9a\x16\x47\x6b\x29\x1b\xd7\x8a\x00");
	}
}
