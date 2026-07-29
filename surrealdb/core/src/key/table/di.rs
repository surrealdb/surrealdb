//! Stores RecordId → DocId mappings for a table's shared doc-ID space.
//!
//! Table-scoped counterpart of the (retired) per-index record→doc mappings. The
//! key layout is `/*{ns}*{db}*{tb}!di{record_id}` — note there is **no**
//! `+{index}` segment, so a single mapping is shared by every index on the table.
//! See [`crate::idx::docids`] for the owning component.

use std::borrow::Cow;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idx::docids::DocId;
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKey, KVKeyDecode, impl_kv_range_storekey, key};
use crate::val::{IndexFormat, RecordIdKey, TableName};

key! {
	/// Maps a SurrealDB record key to its compact table-level document ID.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Di<'a> for IndexFormat {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'd',
		b'i',
		pub id: Cow<'a, RecordIdKey>,
	}
}

impl KVKey for Di<'_> {
	type Value = DocId;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> anyhow::Result<()> {
		storekey::encode_format::<IndexFormat, _, _>(buffer, self)
			.map_err(|_| crate::key::Error::Unencodable)?;
		Ok(())
	}

	fn value_context(&self) {}
}

impl<'a> KVKeyDecode<'a> for Di<'a> {
	fn decode_key(bytes: &'a [u8]) -> anyhow::Result<Self> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(bytes).map_err(|_| {
			crate::key::Error::Corrupted("Table record→doc-ID key cannot be decoded")
		})?)
	}
}

impl<'a> Di<'a> {
	/// Creates the `!di{record_key}` lookup key for one record.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
	) -> Self {
		Self {
			prefix: DatabaseRoot {
				ns,
				db,
			},
			tb: Cow::Borrowed(tb),
			id: Cow::Borrowed(id),
		}
	}
}

key! {
	/// Prefix over every `!di` record→doc mapping on a table.
	///
	/// Encodes the header of every [`Di`] key on `tb` (up to and including the
	/// `!di` sigil, with no record segment), so a single range delete reclaims
	/// the whole record→doc range. Used when the table's last doc-ID-consuming
	/// index is dropped (see [`crate::idx::docids::TableDocIds::remove_all`]).
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Prefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'd',
		b'i',
	}
}

impl_kv_range_storekey!(Prefix<'_>);

impl<'a> Prefix<'a> {
	/// Creates the `!di` prefix covering every record→doc mapping on `tb`.
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName) -> Self {
		Self {
			prefix: DatabaseRoot {
				ns,
				db,
			},
			tb: Cow::Borrowed(tb),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let id = RecordIdKey::from("id".to_owned());
		let val = Di::new(NamespaceId(1), DatabaseId(2), &tb, &id);
		let enc = val.encode_key().unwrap();
		assert_eq!(
			&*enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!di\x03id\0",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}

	#[test]
	fn prefix_is_header_of_full_key() {
		let tb = TableName::from("testtb");
		let id = RecordIdKey::from("id".to_owned());
		let full = Di::new(NamespaceId(1), DatabaseId(2), &tb, &id).encode_key().unwrap();
		let pre = Prefix::new(NamespaceId(1), DatabaseId(2), &tb).encode_bound().unwrap();
		assert_eq!(&*pre, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!di");
		// The prefix range must cover every `Di` key on the table.
		assert!(full.starts_with(&pre), "prefix must be a strict prefix of the full key");
	}
}
