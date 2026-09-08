//! Background reclaim queue
//!
//! `REMOVE NAMESPACE` / `REMOVE DATABASE` / `REMOVE INDEX` delete only the
//! catalog definition inside the user transaction (so the object becomes
//! immediately invisible) and enqueue a reclaim job. Dropping a table's last
//! doc-ID-consuming index enqueues that table's shared doc-ID space the same
//! way. A background task (see
//! [`crate::kvs::Datastore::reclaim_tombstones`]) periodically drains the queue
//! and destroys the now-orphaned data prefix out-of-band — in one
//! out-of-transaction range destroy on a backend that offers one, otherwise in
//! bounded pages, each committed before the next is scanned.
//!
//! Because the queue entry is written in the same transaction that removes the
//! catalog definition, a cancelled/rolled-back transaction leaves neither the
//! removal nor the reclaim job behind, preserving ACID. The data is only ever
//! destroyed after the removal has committed.
//!
//! The reclaim job lives at the **root** level (`/!rc...`) precisely so it
//! survives the deletion of the namespace/database prefix it refers to.
use std::borrow::Cow;
use std::io;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::{impl_kv_key_storekey, impl_kv_value_revisioned};
use crate::val::TableName;

/// Mutable state stored as the value of a [`ReclaimKey`]: when the reclaim
/// task first observed the entry, and how far it has got destroying the prefix.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReclaimState {
	/// Wall-clock unix-millis at which the background reclaim task first
	/// *observed* this entry. `0` means "not yet observed".
	///
	/// The reclaim task only ever reads committed entries, so this is
	/// necessarily at or after the removal's commit — unlike the key's `uid`,
	/// which is a UUIDv7 stamped while the `REMOVE` statement runs (before
	/// commit, and arbitrarily early inside a long `BEGIN`/`COMMIT` block). The
	/// snapshot-safety grace is therefore measured from `observed_ms`, never
	/// from the pre-commit `uid`.
	///
	/// Every cursor write carries the stamp through unchanged: a prefix too
	/// large for one pass must keep ageing against its first observation rather
	/// than resetting its own grace on every page.
	pub observed_ms: u64,
	/// Reclaim continuation cursor: the last key of this entry's prefix whose
	/// deletion is durable. `None` before the first page commits.
	///
	/// Written in the same transaction as the page of deletions it accounts
	/// for, so every key at or before it is gone and a reclaim interrupted by a
	/// restart, a lost lease or an exhausted pass budget resumes here instead
	/// of rescanning the prefix from the start.
	#[revision(start = 2)]
	pub cursor: Option<Vec<u8>>,
}

impl ReclaimState {
	/// The state a freshly-enqueued entry carries: never observed, no progress.
	pub(crate) fn enqueued() -> Self {
		Self {
			observed_ms: 0,
			cursor: None,
		}
	}
}

impl_kv_value_revisioned!(ReclaimState);

/// Represents an entry in the background reclaim queue.
impl ReclaimKind {
	/// Whether this names one of a table's three shared doc-ID prefixes.
	///
	/// These differ from every other kind in the queue in that their claim is
	/// revocable: `DEFINE INDEX` cancels the reclaim in the transaction that
	/// defines a consumer, to keep the mappings still in the space. A reclaim
	/// whose claim can be withdrawn under it has to be able to observe the
	/// withdrawal, so these kinds are always deleted through the paged path,
	/// which re-reads the claim inside every page's transaction. The
	/// out-of-transaction range destroy some backends offer cannot see the
	/// cancellation and so is never used for them.
	pub(crate) fn is_doc_id(self) -> bool {
		matches!(self, Self::DocKey | Self::DocLookup | Self::DocPending)
	}
}

/// Which kind of resource a queue entry names, and so which prefix the reclaim
/// task destroys.
///
/// Encodes itself: the discriminants on disk start at 0, where `storekey`'s
/// derived numbering would start at 2.
///
/// The three table-scoped doc-ID prefixes are named separately rather than by one
/// enclosing range, because the table's key space interleaves them with the
/// monotonic id sequence (`!dd` < `!dh` < `!di` < `!dp` < `!ds`). A range covering
/// all three would take the sequence with them, and an id space that restarts can
/// hand a second record an id the first still holds.
///
/// Each also carries its own resume cursor, since a queue entry holds one.
#[derive(Clone, Copy, Eq, PartialEq, Debug, PartialOrd)]
pub(crate) enum ReclaimKind {
	Namespace,
	Database,
	Index,
	/// A table's doc-ID to record-key direction (`!dd`).
	DocKey,
	/// A table's record-key to doc-ID direction (`!di`).
	DocLookup,
	/// A table's pending-index-entry markers (`!dp`).
	DocPending,
}

impl storekey::Encode for ReclaimKind {
	fn encode<W: io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> Result<(), storekey::EncodeError> {
		match self {
			ReclaimKind::Namespace => w.write_u8(0),
			ReclaimKind::Database => w.write_u8(1),
			ReclaimKind::Index => w.write_u8(2),
			ReclaimKind::DocKey => w.write_u8(3),
			ReclaimKind::DocLookup => w.write_u8(4),
			ReclaimKind::DocPending => w.write_u8(5),
		}
	}
}

impl<'de> storekey::BorrowDecode<'de> for ReclaimKind {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> Result<Self, storekey::DecodeError> {
		let w = r.read_u8()?;
		match w {
			0 => Ok(ReclaimKind::Namespace),
			1 => Ok(ReclaimKind::Database),
			2 => Ok(ReclaimKind::Index),
			3 => Ok(ReclaimKind::DocKey),
			4 => Ok(ReclaimKind::DocLookup),
			5 => Ok(ReclaimKind::DocPending),
			_ => Err(storekey::DecodeError::InvalidFormat),
		}
	}
}

/// Whether the data must be hard-cleared (all MVCC versions) rather than
/// soft-deleted.
///
/// Encodes itself, for the same reason as [`ReclaimKind`].
#[derive(Clone, Copy, Eq, PartialEq, Debug, PartialOrd)]
pub(crate) enum Expunge {
	Keep,
	Expunge,
}

impl storekey::Encode for Expunge {
	fn encode<W: io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> Result<(), storekey::EncodeError> {
		match self {
			Expunge::Keep => w.write_u8(0),
			Expunge::Expunge => w.write_u8(1),
		}
	}
}

impl<'de> storekey::BorrowDecode<'de> for Expunge {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> Result<Self, storekey::DecodeError> {
		let w = r.read_u8()?;
		match w {
			0 => Ok(Expunge::Keep),
			1 => Ok(Expunge::Expunge),
			_ => Err(storekey::DecodeError::InvalidFormat),
		}
	}
}

/// Represents an entry in the background reclaim queue.
///
/// The `kind` discriminant selects which prefix the reclaim task destroys; the
/// `ns`/`db`/`tb`/`ix` ids identify it. Fields not relevant to a given `kind`
/// are zero/empty. `expunge` records whether the data must be hard-cleared
/// (all MVCC versions) rather than soft-deleted. `uid` is a unique,
/// time-ordered id that disambiguates entries.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct ReclaimKey<'key> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub kind: ReclaimKind,
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: Cow<'key, TableName>,
	pub ix: IndexId,
	pub expunge: Expunge,
	pub uid: Uuid,
}

impl_kv_key_storekey!(ReclaimKey<'_> => ReclaimState);

impl Categorise for ReclaimKey<'_> {
	fn categorise(&self) -> Category {
		Category::Reclaim
	}
}

impl<'key> ReclaimKey<'key> {
	/// Enqueue reclaim of a whole namespace prefix.
	pub(crate) fn namespace(ns: NamespaceId, expunge: bool, uid: Uuid) -> Self {
		Self::new(
			ReclaimKind::Namespace,
			ns,
			DatabaseId(0),
			Cow::Owned(TableName::from("")),
			IndexId(0),
			expunge,
			uid,
		)
	}

	/// Enqueue reclaim of a whole database prefix.
	pub(crate) fn database(ns: NamespaceId, db: DatabaseId, expunge: bool, uid: Uuid) -> Self {
		Self::new(
			ReclaimKind::Database,
			ns,
			db,
			Cow::Owned(TableName::from("")),
			IndexId(0),
			expunge,
			uid,
		)
	}

	/// Enqueue reclaim of a single index prefix.
	pub(crate) fn index(
		ns: NamespaceId,
		db: DatabaseId,
		tb: Cow<'key, TableName>,
		ix: IndexId,
		expunge: bool,
		uid: Uuid,
	) -> Self {
		Self::new(ReclaimKind::Index, ns, db, tb, ix, expunge, uid)
	}

	/// A queue entry of an arbitrary kind, for tests covering a kind no path on
	/// this branch enqueues — one only a node with a newer layout could write.
	#[cfg(test)]
	// Driven only by the reclaim tests, which build an in-memory datastore.
	#[cfg_attr(not(feature = "kv-mem"), allow(dead_code))]
	pub(crate) fn of_kind(kind: ReclaimKind, ns: NamespaceId, db: DatabaseId, uid: Uuid) -> Self {
		Self::new(kind, ns, db, Cow::Owned(TableName::from("t")), IndexId(0), false, uid)
	}

	fn new(
		kind: ReclaimKind,
		ns: NamespaceId,
		db: DatabaseId,
		tb: Cow<'key, TableName>,
		ix: IndexId,
		expunge: bool,
		uid: Uuid,
	) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'r',
			_c: b'c',
			kind,
			ns,
			db,
			tb,
			ix,
			expunge: Self::expunge(expunge),
			uid,
		}
	}

	fn expunge(expunge: bool) -> Expunge {
		match expunge {
			true => Expunge::Expunge,
			false => Expunge::Keep,
		}
	}

	/// Half-open byte range covering every reclaim queue entry.
	pub(crate) fn range() -> (Vec<u8>, Vec<u8>) {
		(b"/!rc\x00".to_vec(), b"/!rc\xff".to_vec())
	}

	pub(crate) fn decode_key(k: &[u8]) -> anyhow::Result<ReclaimKey<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	/// The discriminants an in-place upgrade depends on.
	///
	/// The previous release stored `kind` and `expunge` as plain `u8`s. These are
	/// enums now, so their encodings have to stay on the same bytes: an entry a
	/// `REMOVE` queued before the upgrade is decoded by this version, and a
	/// shifted discriminant would resolve it to the wrong prefix and destroy a
	/// live one. Asserted as bytes rather than by round-trip, which cannot see
	/// both sides moving together.
	#[test]
	fn reclaim_kind_and_expunge_discriminants_are_pinned() {
		for (kind, byte) in [
			(ReclaimKind::Namespace, 0u8),
			(ReclaimKind::Database, 1),
			(ReclaimKind::Index, 2),
			(ReclaimKind::DocKey, 3),
			(ReclaimKind::DocLookup, 4),
			(ReclaimKind::DocPending, 5),
		] {
			let mut buf = Vec::new();
			let mut w = storekey::Writer::new(&mut buf);
			storekey::Encode::encode(&kind, &mut w).unwrap();
			assert_eq!(buf, vec![byte], "{kind:?} must encode as {byte}");
		}
		for (expunge, byte) in [(Expunge::Keep, 0u8), (Expunge::Expunge, 1)] {
			let mut buf = Vec::new();
			let mut w = storekey::Writer::new(&mut buf);
			storekey::Encode::encode(&expunge, &mut w).unwrap();
			assert_eq!(buf, vec![byte], "{expunge:?} must encode as {byte}");
		}
		// And in place in a whole key: `/!rc` then the kind byte.
		let enc = ReclaimKey::encode_key(&ReclaimKey::namespace(
			NamespaceId(1),
			false,
			Uuid::from_u128(7),
		))
		.unwrap();
		assert_eq!(&enc[..5], b"/!rc\x00", "the kind byte follows the `/!rc` prefix");
	}

	/// A queue entry written by the previous release still decodes.
	///
	/// `ReclaimState` gained its resume cursor at revision 2. A `REMOVE` whose
	/// reclaim had not drained before the upgrade leaves a revision-1 value
	/// behind, and failing to decode it would strand the data it names with
	/// nothing left to find it.
	#[test]
	fn reclaim_state_decodes_a_revision_1_value() {
		use revision::SerializeRevisioned;

		// Revision 1 on the wire: the revision, then the only field it had.
		let mut bytes = Vec::new();
		1u16.serialize_revisioned(&mut bytes).unwrap();
		1_234_567_u64.serialize_revisioned(&mut bytes).unwrap();

		let got = <ReclaimState as crate::kvs::KVValue>::kv_decode_value(&bytes, ()).unwrap();
		assert_eq!(got.observed_ms, 1_234_567, "the observed stamp must survive the upgrade");
		assert_eq!(
			got.cursor, None,
			"a revision-1 entry has made no paged progress, so it must resume from the start"
		);
	}

	#[test]
	fn range() {
		assert_eq!(ReclaimKey::range(), (b"/!rc\x00".to_vec(), b"/!rc\xff".to_vec()));
	}

	#[test]
	fn database_key_roundtrips() {
		let val = ReclaimKey::database(NamespaceId(1), DatabaseId(2), false, Uuid::from_u128(7));
		let enc = ReclaimKey::encode_key(&val).unwrap();
		// Inside the scannable range
		let (beg, end) = ReclaimKey::range();
		assert!(enc.as_slice() >= beg.as_slice() && enc.as_slice() < end.as_slice());
		let dec = ReclaimKey::decode_key(&enc).unwrap();
		assert_eq!(dec.kind, ReclaimKind::Database);
		assert_eq!(dec.ns, NamespaceId(1));
		assert_eq!(dec.db, DatabaseId(2));
		assert_eq!(dec.expunge, Expunge::Keep);
		assert_eq!(dec.uid, Uuid::from_u128(7));
	}

	#[test]
	fn index_key_roundtrips() {
		let val = ReclaimKey::index(
			NamespaceId(4),
			DatabaseId(5),
			Cow::Owned(TableName::from("testtb")),
			IndexId(6),
			true,
			Uuid::from_u128(9),
		);
		let enc = ReclaimKey::encode_key(&val).unwrap();
		let dec = ReclaimKey::decode_key(&enc).unwrap();
		assert_eq!(dec.kind, ReclaimKind::Index);
		assert_eq!(dec.ns, NamespaceId(4));
		assert_eq!(dec.db, DatabaseId(5));
		assert_eq!(dec.tb.as_ref(), &TableName::from("testtb"));
		assert_eq!(dec.ix, IndexId(6));
		assert_eq!(dec.expunge, Expunge::Expunge);
	}
}
