//! Field and value types of the background reclaim queue.
//!
//! `REMOVE NAMESPACE` / `REMOVE DATABASE` / `REMOVE INDEX` delete only the
//! catalog definition inside the user transaction (so the object becomes
//! immediately invisible) and enqueue a reclaim job. Dropping a table's last
//! doc-ID-consuming index enqueues that table's shared doc-ID space the same
//! way. A background task (see
//! [`crate::Datastore::reclaim_tombstones`]) periodically drains the queue
//! and destroys the now-orphaned data prefix out-of-band — in one
//! out-of-transaction range destroy on a backend that offers one, otherwise in
//! bounded pages, each committed before the next is scanned.
//!
//! Because the queue entry is written in the same transaction that removes the
//! catalog definition, a cancelled or rolled-back transaction leaves neither the
//! removal nor the reclaim job behind, preserving ACID. The data is only ever
//! destroyed after the removal has committed.
//!
//! The queue lives at the **root** level precisely so an entry survives the
//! deletion of the namespace or database prefix it refers to. Its layout is
//! declared as `reclaim` in [`crate::key::schema`]; the two discriminants here
//! are part of that layout and encode themselves, because the numbering on disk
//! predates `storekey`'s derived numbering.

use std::io;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_kvs::impl_kv_value_revisioned;

/// Mutable state stored as the value of a reclaim queue entry: when the reclaim
/// task first observed the entry, and how far it has got destroying the prefix.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReclaimState {
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
	pub fn enqueued() -> Self {
		Self {
			observed_ms: 0,
			cursor: None,
		}
	}
}

impl ReclaimKind {
	/// Whether this names one of a table's shared doc-ID prefixes.
	///
	/// These differ from every other kind in the queue in that their claim is
	/// revocable: `DEFINE INDEX` cancels the reclaim in the transaction that
	/// defines a consumer, to keep the mappings still in the space. A reclaim
	/// whose claim can be withdrawn under it has to be able to observe the
	/// withdrawal, so these kinds are always deleted through the paged path,
	/// which re-reads the claim inside every page's transaction. The
	/// out-of-transaction range destroy some backends offer cannot see the
	/// cancellation and so is never used for them.
	pub fn is_doc_id(&self) -> bool {
		matches!(self, Self::DocKey | Self::DocLookup | Self::DocLookupIdentity | Self::DocPending)
	}
}

impl_kv_value_revisioned!(ReclaimState);

/// Every table-scoped doc-ID band, for callers that must act on all of them.
///
/// Kept beside [`ReclaimKind::is_doc_id`], which it must agree with; the test
/// below holds them together. A band added to one and not the other is the kind
/// of omission that goes unnoticed, since nothing else enumerates them.
pub const ALL_DOC_ID_KINDS: [ReclaimKind; 4] = [
	ReclaimKind::DocKey,
	ReclaimKind::DocLookup,
	ReclaimKind::DocLookupIdentity,
	ReclaimKind::DocPending,
];

/// Which kind of resource a queue entry names, and so which prefix the reclaim
/// task destroys.
///
/// Encodes itself: the discriminants on disk start at 0, where `storekey`'s
/// derived numbering would start at 2.
///
/// The table-scoped doc-ID prefixes are named separately rather than by one
/// enclosing range, because the table's key space interleaves them with the
/// monotonic id sequence (`!dd` < `!dh` < `!di` < `!dj` < `!dp` < `!ds`). A range covering
/// all of them would take the sequence with them, and an id space that restarts can
/// hand a second record an id the first still holds.
///
/// Each also carries its own resume cursor, since a queue entry holds one.
#[derive(Clone, Copy, Eq, PartialEq, Debug, PartialOrd)]
pub enum ReclaimKind {
	Namespace,
	Database,
	Index,
	/// A table's doc-ID to record-key direction (`!dd`).
	DocKey,
	/// A table's record-key to doc-ID direction (`!di`).
	DocLookup,
	/// The same direction spelled injectively (`!dj`).
	DocLookupIdentity,
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
			ReclaimKind::DocLookupIdentity => w.write_u8(6),
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
			6 => Ok(ReclaimKind::DocLookupIdentity),
			_ => Err(storekey::DecodeError::InvalidFormat),
		}
	}
}

/// Whether the data must be hard-cleared (all MVCC versions) rather than
/// soft-deleted.
///
/// Encodes itself, for the same reason as [`ReclaimKind`].
#[derive(Clone, Copy, Eq, PartialEq, Debug, PartialOrd)]
pub enum Expunge {
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

#[cfg(test)]
mod reclaim_kind_tests {
	use super::*;

	/// Every table-scoped doc-ID band must be classified as one.
	///
	/// The classification is what keeps a band on the guarded paged path, which
	/// re-reads the claim inside each page's transaction; a band left out takes
	/// the out-of-transaction range destroy instead, which cannot see a
	/// concurrent cancellation and can erase mappings a new consumer has just
	/// committed to. A band added without being listed here is silently wrong,
	/// so the match is exhaustive rather than a list of the ones we remembered.
	#[test]
	fn every_table_scoped_doc_id_band_is_classified_as_one() {
		for kind in [
			ReclaimKind::Namespace,
			ReclaimKind::Database,
			ReclaimKind::Index,
			ReclaimKind::DocKey,
			ReclaimKind::DocLookup,
			ReclaimKind::DocLookupIdentity,
			ReclaimKind::DocPending,
		] {
			let expected = match kind {
				// Not table-scoped doc-ID state: these name a whole catalog
				// subtree and have no claim to race with.
				ReclaimKind::Namespace | ReclaimKind::Database | ReclaimKind::Index => false,
				ReclaimKind::DocKey
				| ReclaimKind::DocLookup
				| ReclaimKind::DocLookupIdentity
				| ReclaimKind::DocPending => true,
			};
			assert_eq!(kind.is_doc_id(), expected, "{kind:?} is classified wrongly");
			assert_eq!(
				ALL_DOC_ID_KINDS.contains(&kind),
				expected,
				"{kind:?} must be listed in ALL_DOC_ID_KINDS exactly when it is doc-ID state"
			);
		}
	}

	/// The on-disk tags are a wire format: a tag that moves makes an existing
	/// queue entry name a different band.
	#[test]
	fn the_wire_tags_are_stable() {
		for (kind, tag) in [
			(ReclaimKind::Namespace, 0u8),
			(ReclaimKind::Database, 1),
			(ReclaimKind::Index, 2),
			(ReclaimKind::DocKey, 3),
			(ReclaimKind::DocLookup, 4),
			(ReclaimKind::DocPending, 5),
			(ReclaimKind::DocLookupIdentity, 6),
		] {
			let encoded = storekey::encode_vec(&kind).expect("encode");
			assert_eq!(encoded, vec![tag], "{kind:?} must keep tag {tag}");
			// Decode has its own arm per tag, so round-trip rather than trusting
			// that adding one implies the other.
			let decoded: ReclaimKind = storekey::decode_borrow(&encoded).expect("decode");
			assert_eq!(decoded, kind, "tag {tag} must decode back to {kind:?}");
		}
	}
}
