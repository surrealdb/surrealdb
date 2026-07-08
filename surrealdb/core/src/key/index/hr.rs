//! Record-keyed pending updates for HNSW indexes.

use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::IndexId;
use crate::idx::trees::hnsw::HnswRecordPendingUpdate;
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKey, KVKeyDecode, impl_kv_range_storekey, key};
use crate::val::{IndexFormat, RecordIdKey, TableName};

key! {
/// Pending HNSW update keyed by the owning record.
///
/// There is at most one live `!hr` key for a record. Repeated writes replace
/// the desired vectors while preserving the original graph baseline, making
/// pending HNSW work independent of cross-node append ordering.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct HnswRecordPending<'a> for IndexFormat{
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b'r',
		pub id: Cow<'a, RecordIdKey>,
	}
}

impl KVKey for HnswRecordPending<'_> {
	type Value = HnswRecordPendingUpdate;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		storekey::encode_format::<IndexFormat, _, _>(buffer, self)
			.map_err(|_| crate::err::Error::Unencodable)?;
		Ok(())
	}

	fn value_context(&self) {}
}
impl<'a> KVKeyDecode<'a> for HnswRecordPending<'a> {
	fn decode_key(bytes: &'a [u8]) -> Result<Self> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(bytes)
			.map_err(|_| crate::err::Error::Corrupted("HnswRecordPending cannot be decoded"))?)
	}
}

key! {
	/// Prefix for all record-keyed HNSW pending updates for an index.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct HnswRecordPendingPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b'r',
	}
}

impl_kv_range_storekey!(HnswRecordPendingPrefix<'_>);

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let id = RecordIdKey::String(Strand::new_static("testid"));
		let val = HnswRecordPending {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			id: Cow::Borrowed(&id),
		};
		let enc = HnswRecordPending::encode_key(&val).unwrap();
		assert_eq!(
			&*enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hr\x03testid\0"
		);
	}
}
