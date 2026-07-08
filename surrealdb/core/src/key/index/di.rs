//! Stores RecordId to DocId mappings for a DiskANN index.

use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKey, KVKeyDecode, key};
use crate::val::{IndexFormat, RecordIdKey, TableName};

key! {
	/// Maps a SurrealDB record key to its compact DiskANN document ID.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Di<'a> for IndexFormat {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'i',
		pub id: Cow<'a, RecordIdKey>,
	}
}

impl KVKey for Di<'_> {
	type Value = u64;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> anyhow::Result<()> {
		storekey::encode_format::<IndexFormat, _, _>(buffer, self)
			.map_err(|_| crate::err::Error::Unencodable)?;
		Ok(())
	}

	fn value_context(&self) {}
}

impl<'a> KVKeyDecode<'a> for Di<'a> {
	fn decode_key(bytes: &'a [u8]) -> anyhow::Result<Self> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(bytes).map_err(|_| {
			crate::err::Error::Corrupted("DiskANN DocID mapping key cannot be decoded")
		})?)
	}
}
