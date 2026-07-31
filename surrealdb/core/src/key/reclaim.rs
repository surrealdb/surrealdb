//! Field and value types of the background reclaim queue.
//!
//! `REMOVE NAMESPACE` / `REMOVE DATABASE` / `REMOVE INDEX` delete only the
//! catalog definition inside the user transaction (so the object becomes
//! immediately invisible) and enqueue a reclaim job. A background task (see
//! [`crate::kvs::Datastore::reclaim_tombstones`]) periodically drains the queue
//! and destroys the now-orphaned data prefix out-of-band — via
//! `unsafe_destroy_range` on TiKV or a transactional prefix delete on other
//! backends.
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

use crate::key::impl_kv_value_revisioned;

/// Mutable state stored as the value of a reclaim queue entry.
///
/// `observed_ms` is the wall-clock unix-millis at which the background reclaim
/// task first *observed* this entry. The reclaim task only ever reads committed
/// entries, so this is necessarily at or after the removal's commit — unlike the
/// key's `uid`, which is a UUIDv7 stamped while the `REMOVE` statement runs
/// (before commit, and arbitrarily early inside a long `BEGIN`/`COMMIT` block).
/// The snapshot-safety grace is therefore measured from `observed_ms`, never from
/// the pre-commit `uid`. `0` means "not yet observed".
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReclaimState {
	pub observed_ms: u64,
}

impl_kv_value_revisioned!(ReclaimState);

/// Which kind of resource a queue entry names, and so which prefix the reclaim
/// task destroys.
///
/// Encodes itself: the discriminants on disk start at 0, where `storekey`'s
/// derived numbering would start at 2.
#[derive(Clone, Copy, Eq, PartialEq, Debug, PartialOrd)]
pub enum ReclaimKind {
	Namespace,
	Database,
	Index,
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
