//! Background reclaim queue
//!
//! `REMOVE NAMESPACE` / `REMOVE DATABASE` / `REMOVE INDEX` delete only the
//! catalog definition inside the user transaction (so the object becomes
//! immediately invisible) and enqueue a reclaim job here. A background task
//! (see [`crate::kvs::Datastore::reclaim_tombstones`]) periodically drains the
//! queue and destroys the now-orphaned data prefix out-of-band — via
//! `unsafe_destroy_range` on TiKV or a transactional prefix delete on other
//! backends.
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
use uuid::Uuid;

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, impl_kv_value_revisioned, key};
use crate::val::TableName;

/// Mutable state stored as the value of a [`ReclaimKey`].
///
/// `observed_ms` is the wall-clock unix-millis at which the background reclaim
/// task first *observed* this entry. The reclaim task only ever reads committed
/// entries, so this is necessarily at or after the removal's commit — unlike the
/// key's `uid`, which is a UUIDv7 stamped while the `REMOVE` statement runs
/// (before commit, and arbitrarily early inside a long `BEGIN`/`COMMIT` block).
/// The snapshot-safety grace is therefore measured from `observed_ms`, never
/// from the pre-commit `uid`. `0` means "not yet observed".
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReclaimState {
	pub observed_ms: u64,
}

impl_kv_value_revisioned!(ReclaimState);

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

/// Enum for the expunge field.
///
/// This enum manually implements storekey encoding as, for backwards compatiblity,
/// it needs to be serialized with a different format than the default format.
/// (derive implemention starts enum discriminant at 2, instead of 0)
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

key! {
	/// Represents an entry in the background reclaim queue.
	///
	/// The `kind` discriminant selects which prefix the reclaim task destroys; the
	/// `ns`/`db`/`tb`/`ix` ids identify it. Fields not relevant to a given `kind`
	/// are zero/empty. `expunge` records whether the data must be hard-cleared
	/// (all MVCC versions) rather than soft-deleted. `uid` is a unique,
	/// time-ordered id that disambiguates entries.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct ReclaimKey<'key> {
		b'/',
		b'!',
		b'r',
		b'c',
		pub kind: ReclaimKind,
		pub ns: NamespaceId,
		pub db: DatabaseId,
		pub tb: Cow<'key, TableName>,
		pub ix: IndexId,
		pub expunge: Expunge,
		pub uid: Uuid,
	}
}

impl<'key> ReclaimKey<'key> {
	pub fn namespace(ns: NamespaceId, expunge: bool, uuid: Uuid) -> Self {
		ReclaimKey {
			kind: ReclaimKind::Namespace,
			ns,
			db: DatabaseId(0),
			tb: Cow::Owned(TableName::default()),
			ix: IndexId(0),
			expunge: if expunge {
				Expunge::Expunge
			} else {
				Expunge::Keep
			},
			uid: uuid,
		}
	}

	pub fn database(ns: NamespaceId, db: DatabaseId, expunge: bool, uuid: Uuid) -> Self {
		ReclaimKey {
			kind: ReclaimKind::Database,
			ns,
			db,
			tb: Cow::Owned(TableName::default()),
			ix: IndexId(0),
			expunge: if expunge {
				Expunge::Expunge
			} else {
				Expunge::Keep
			},
			uid: uuid,
		}
	}
}

impl_kv_key_storekey!(ReclaimKey<'a> => ReclaimState);
impl Categorise for ReclaimKey<'_> {
	fn categorise(&self) -> Category {
		Category::Reclaim
	}
}

key! {
	pub(crate) struct ReclaimPrefix {
		b'/',
		b'!',
		b'r',
		b'c',
	}
}
impl_kv_range_storekey!(ReclaimPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::{KVKey, KVKeyDecode, KVRange};

	#[test]
	fn range() {
		let prefix = ReclaimPrefix {}.encode_range().unwrap();
		assert_eq!(prefix.start.as_slice(), b"/!rc\x00");
		assert_eq!(prefix.end.as_slice(), b"/!rd");
	}

	#[test]
	fn database_key_roundtrips() {
		let val = ReclaimKey::database(NamespaceId(1), DatabaseId(2), false, Uuid::from_u128(7));
		let enc = ReclaimKey::encode_key(&val).unwrap();
		// Inside the scannable range
		let range = ReclaimPrefix {}.encode_range().unwrap();
		assert!(enc.as_slice() >= range.start.as_slice() && enc.as_slice() < range.end.as_slice());
		let dec = ReclaimKey::decode_key(&enc).unwrap();
		assert_eq!(dec.kind, ReclaimKind::Database);
		assert_eq!(dec.ns, NamespaceId(1));
		assert_eq!(dec.db, DatabaseId(2));
		assert_eq!(dec.expunge, Expunge::Keep);
		assert_eq!(dec.uid, Uuid::from_u128(7));
	}

	#[test]
	fn index_key_roundtrips() {
		let val = ReclaimKey {
			kind: ReclaimKind::Index,
			ns: NamespaceId(4),
			db: DatabaseId(5),
			tb: Cow::Owned(TableName::from("testtb")),
			ix: IndexId(6),
			expunge: Expunge::Expunge,
			uid: Uuid::from_u128(9),
		};
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
