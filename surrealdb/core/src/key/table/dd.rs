//! Stores DocId → RecordId mappings for a table's shared doc-ID space.
//!
//! Table-scoped counterpart of the (retired) per-index doc→record mappings. The
//! key layout is `/*{ns}*{db}*{tb}!dd{doc_id}` — no `+{index}` segment, so the
//! mapping is shared by every index on the table. The `doc_id` is encoded
//! big-endian, so a range scan over the `!dd` prefix enumerates records in
//! doc-ID order. See [`crate::idx::docids`] for the owning component.

use std::borrow::Cow;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idx::docids::DocId;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::val::{RecordIdKey, TableName};

key! {
	/// Maps a compact table-level document ID back to a SurrealDB record key.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Dd<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'd',
		b'd',
		pub doc_id: DocId,
	}
}

impl_kv_key_storekey!(Dd<'a> => RecordIdKey);

impl<'a> Dd<'a> {
	/// Creates the `!dd{doc_id}` lookup key for one compact document ID.
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, doc_id: DocId) -> Self {
		Self {
			prefix: DatabaseRoot {
				ns,
				db,
			},
			tb: Cow::Borrowed(tb),
			doc_id,
		}
	}
}

key! {
	/// Prefix over every `!dd` doc→record mapping on a table.
	///
	/// Encodes the header of every [`Dd`] key on `tb` (up to and including the
	/// `!dd` sigil, with no doc-id segment), so a single range delete reclaims
	/// the whole doc→record range. Used when the table's last doc-ID-consuming
	/// index is dropped (see [`crate::idx::docids::TableDocIds::remove_all`]).
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Prefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'd',
		b'd',
	}
}

impl_kv_range_storekey!(Prefix<'_>);

impl<'a> Prefix<'a> {
	/// Creates the `!dd` prefix covering every doc→record mapping on `tb`.
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
		let val = Dd::new(NamespaceId(1), DatabaseId(2), &tb, 1);
		let enc = val.encode_key().unwrap();
		assert_eq!(&*enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!dd\0\0\0\0\0\0\0\x01");
	}

	#[test]
	fn prefix_is_header_of_full_key() {
		let tb = TableName::from("testtb");
		let full = Dd::new(NamespaceId(1), DatabaseId(2), &tb, 1).encode_key().unwrap();
		let pre = Prefix::new(NamespaceId(1), DatabaseId(2), &tb).encode_bound().unwrap();
		assert_eq!(&*pre, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!dd");
		// The prefix range must cover every `Dd` key on the table.
		assert!(full.starts_with(&pre), "prefix must be a strict prefix of the full key");
	}
}
