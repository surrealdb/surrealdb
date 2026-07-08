//! Shard-prefixed pending updates for DiskANN indexes.
//!
//! This is the sharded successor to the legacy [`crate::key::index::dr`] (`!dr`) layout. The
//! record key is prefixed with the writer's pending-state shard (the same shard the writer bumps
//! in `!dp`), so compaction can drain — and lookup can scan — one shard at a time instead of
//! sweeping the whole index's pending range on every query while a write backlog exists.
//!
//! Legacy `!dr` keys written by an older binary are migrated lazily: writers only ever emit `!dw`
//! keys, while compaction and lookup keep reading the legacy `!dr` range until it drains empty
//! (the dual-read transition). The two layouts use distinct key tags so their ranges never
//! overlap and a scan can decode each unambiguously.

use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::IndexId;
use crate::err::Error;
use crate::idx::trees::diskann::DiskAnnRecordPendingUpdate;
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKey, KVKeyDecode, impl_kv_range_storekey, key};
use crate::val::{IndexFormat, RecordIdKey, TableName};

key! {
	/// Stores the coalesced pending update for one DiskANN indexed record, prefixed by its shard.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DiskAnnRecordPendingShard<'a> for IndexFormat {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'w',
		pub shard: u16,
		pub id: Cow<'a, RecordIdKey>,
	}
}

impl KVKey for DiskAnnRecordPendingShard<'_> {
	type Value = DiskAnnRecordPendingUpdate;

	fn value_context(&self) {}

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		storekey::encode_format::<IndexFormat, _, _>(buffer, self)
			.map_err(|_| crate::err::Error::Unencodable)?;
		Ok(())
	}
}

impl<'a> KVKeyDecode<'a> for DiskAnnRecordPendingShard<'a> {
	fn decode_key(bytes: &'a [u8]) -> Result<Self> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(bytes)
			.map_err(|_| Error::Corrupted("Cannot decode DiskAnnRecordPendingShard key"))?)
	}
}

key! {
	/// Prefix used to build the range covering all `!dw` pending updates for one shard of one index.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DiskAnnRecordPendingShardPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'w',
		pub shard: u16,
	}
}

impl_kv_range_storekey!(DiskAnnRecordPendingShardPrefix<'_>);

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVRange;
	use crate::key::index::dr::DiskAnnRecordPendingPrefix;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let id = RecordIdKey::String(Strand::new_static("testid"));
		let enc = DiskAnnRecordPendingShard {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			shard: 7,
			id: Cow::Borrowed(&id),
		}
		.encode_key()
		.unwrap();
		let dec = DiskAnnRecordPendingShard::decode_key(&enc).unwrap();
		assert_eq!(dec.shard, 7);
		assert_eq!(dec.id.as_ref(), &id);
	}

	#[test]
	fn shard_range_is_disjoint_per_shard_and_from_legacy() {
		let tb = TableName::from("testtb");
		// One shard's range must not contain a key from an adjacent shard.
		let id = RecordIdKey::Number(42);
		let key_shard_7 = DiskAnnRecordPendingShard {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			shard: 7,
			id: Cow::Borrowed(&id),
		}
		.encode_key()
		.unwrap();
		let range_7 = DiskAnnRecordPendingShardPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			shard: 7,
		}
		.encode_range()
		.unwrap();
		let range_8 = DiskAnnRecordPendingShardPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			shard: 8,
		}
		.encode_range()
		.unwrap();
		assert!(range_7.start <= key_shard_7 && key_shard_7 < range_7.end);
		assert!(!(range_8.start <= key_shard_7 && key_shard_7 < range_8.end));

		// The legacy `!dr` range and the sharded `!dw` range must be disjoint so a scan of one
		// never decodes a key from the other.
		let legacy_range = DiskAnnRecordPendingPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
		}
		.encode_range()
		.unwrap();
		assert!(!(legacy_range.start <= key_shard_7 && key_shard_7 < legacy_range.end));
	}
}
