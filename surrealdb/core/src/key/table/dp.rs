//! Stores pending doc-ID reclaim markers for a table's shared doc-ID space.
//!
//! A `!dp{record_id}` marker records that the record was deleted while more than
//! one doc-ID-consuming index was building, so the shared `!di`/`!dd` mapping
//! could not be reclaimed at replay time (a still-building sibling may need it
//! to replay its own copy of the delete). The marker is written in the **same
//! transaction** that consumes the delete from the builder's queue, so it
//! survives builder errors, aborts, crashes and cross-node takeovers.
//!
//! Markers are drained by [`Building::reclaim_deferred_doc_ids`] once no doc-ID
//! index on the table is building any more: the mapping of a still-absent record
//! is reclaimed, a re-created record keeps its live mapping, and the marker is
//! deleted in both cases. Leftover markers (e.g. after a failed sweep) are
//! reclaimed by the next doc-ID index build on the table — including `REBUILD` —
//! or by [`crate::idx::docids::TableDocIds::remove_all`] when the table's last
//! doc-ID index is dropped.
//!
//! The key layout is `/*{ns}*{db}*{tb}!dp{record_id}` — table-scoped like
//! `!di`/`!dd`, with no `+{index}` segment.
//!
//! [`Building::reclaim_deferred_doc_ids`]: crate::kvs::index

use std::borrow::Cow;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKey, KVKeyDecode, impl_kv_range_storekey, key};
use crate::val::{IndexFormat, RecordIdKey, TableName};

key! {
	/// Marks a deleted record whose shared doc-ID mapping reclaim was deferred.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Dp<'a> for IndexFormat {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'd',
		b'p',
		pub id: Cow<'a, RecordIdKey>,
	}
}

impl KVKey for Dp<'_> {
	type Value = ();

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> anyhow::Result<()> {
		storekey::encode_format::<IndexFormat, _, _>(buffer, self)
			.map_err(|_| crate::err::Error::Unencodable)?;
		Ok(())
	}

	fn value_context(&self) {}
}

impl<'a> KVKeyDecode<'a> for Dp<'a> {
	fn decode_key(bytes: &'a [u8]) -> anyhow::Result<Self> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(bytes).map_err(|_| {
			crate::err::Error::Corrupted("Table pending doc-ID reclaim key cannot be decoded")
		})?)
	}
}

impl<'a> Dp<'a> {
	/// Creates the `!dp{record_key}` pending-reclaim marker key for one record.
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

	/// Consumes the key, returning the owned record key it marks.
	pub(crate) fn into_id(self) -> RecordIdKey {
		self.id.into_owned()
	}
}

key! {
	/// Prefix over every `!dp` pending-reclaim marker on a table.
	///
	/// Encodes the header of every [`Dp`] key on `tb` (up to and including the
	/// `!dp` sigil, with no record segment), so the reclaim sweep can range-scan
	/// the pending markers and `remove_all` can range-delete them.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Prefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'd',
		b'p',
	}
}

impl_kv_range_storekey!(Prefix<'_>);

impl<'a> Prefix<'a> {
	/// Creates the `!dp` prefix covering every pending-reclaim marker on `tb`.
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
		let val = Dp::new(NamespaceId(1), DatabaseId(2), &tb, &id);
		let enc = val.encode_key().unwrap();
		assert_eq!(
			&*enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!dp\x03id\0",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}

	#[test]
	fn prefix_is_header_of_full_key() {
		let tb = TableName::from("testtb");
		let id = RecordIdKey::from("id".to_owned());
		let full = Dp::new(NamespaceId(1), DatabaseId(2), &tb, &id).encode_key().unwrap();
		let pre = Prefix::new(NamespaceId(1), DatabaseId(2), &tb).encode_bound().unwrap();
		assert_eq!(&*pre, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!dp");
		// The prefix range must cover every `Dp` key on the table.
		assert!(full.starts_with(&pre), "prefix must be a strict prefix of the full key");
	}

	#[test]
	fn decode_round_trip() {
		let tb = TableName::from("testtb");
		let id = RecordIdKey::from("rec".to_owned());
		let enc = Dp::new(NamespaceId(1), DatabaseId(2), &tb, &id).encode_key().unwrap();
		let dec = Dp::decode_key(&enc).unwrap();
		assert_eq!(dec.into_id(), id);
	}
}
