//! Record-keyed pending updates for DiskANN indexes.

use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::IndexId;
use crate::idx::trees::diskann::DiskAnnRecordPendingUpdate;
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKey, KVKeyDecode, impl_kv_range_storekey, key};
use crate::val::{IndexFormat, RecordIdKey, TableName};

key! {
	/// Stores the coalesced pending update for one DiskANN indexed record.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DiskAnnRecordPending<'a> for IndexFormat{
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		 b'!',
		 b'd',
		 b'r',
		pub id: Cow<'a, RecordIdKey>,
	}
}

impl KVKey for DiskAnnRecordPending<'_> {
	type Value = DiskAnnRecordPendingUpdate;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		storekey::encode_format::<IndexFormat, _, _>(buffer, self)
			.map_err(|_| crate::key::Error::Unencodable)?;
		Ok(())
	}

	fn value_context(&self) {}
}

impl<'a> KVKeyDecode<'a> for DiskAnnRecordPending<'a> {
	fn decode_key(bytes: &'a [u8]) -> Result<Self> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(bytes).map_err(|_| {
			crate::key::Error::Corrupted("DiskANNRecordPending key could not be decoded")
		})?)
	}
}

key! {
	/// Prefix used to build the range covering all `!dr` pending updates for one DiskANN index.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DiskAnnRecordPendingPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		 b'!',
		 b'd',
		 b'r',
	}
}

impl_kv_range_storekey!(DiskAnnRecordPendingPrefix<'_>);
