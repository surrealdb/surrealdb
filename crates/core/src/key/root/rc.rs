//! Reclaim Queue
//!
//! A reclaim entry names a namespace or a database whose catalog definition has
//! already been removed and whose data prefix is still on disk. `REMOVE
//! NAMESPACE` and `REMOVE DATABASE` delete only the catalog definition and
//! enqueue an entry here; the background reclaim task destroys the named prefix
//! out of band, in bounded resumable batches.
//!
//! The entry is written in the same transaction as the catalog delete, so a
//! rolled back statement leaves neither the removal nor the reclaim job, and a
//! prefix is only ever destroyed after its removal is durable.
//!
//! The entry lives at root level precisely so that it survives destruction of
//! the prefix it names.
//!
//! An entry is also what makes the name it holds unusable. Data is addressed by
//! name, so a new object of the same name would otherwise inherit the bytes the
//! entry describes, and the reclaim task would later destroy them out from under
//! it. Reusing a name therefore has to finish the entry first, in the
//! transaction that reuses it.
//!
//! The reaper destroys every version of every key under the named prefix, not
//! just the currently visible one. Names are reused on this line, so a prefix
//! that was only tombstoned would be inherited version-for-version by the next
//! object of the same name: a versioned read of the new database would return
//! records that belonged to the old one. `AND EXPUNGE` therefore selects
//! nothing here — it is what every reclaim already does.

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::ops::Range;
use uuid::Uuid;

use crate::err::Error;
use crate::key::category::{Categorise, Category};
use crate::kvs::{impl_key, KeyDecode, KeyEncode};

/// The kind of catalog object a reclaim entry names.
///
/// The variant index is serialised into the key as a big-endian `u32`, so it is
/// part of the on-disk layout: variants may be appended, but never reordered or
/// removed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) enum ReclaimKind {
	Namespace,
	Database,
}

/// An entry in the reclaim queue.
///
/// `ns` is always set. `db` is set only for [`ReclaimKind::Database`]; a
/// namespace entry names the whole namespace prefix and carries `None`.
///
/// `uid` is a `Uuid::now_v7()` allocated when the entry is enqueued. It only
/// disambiguates entries that would otherwise collide — an object removed,
/// recreated and removed again before the reaper drains the queue — and carries
/// no timing meaning, because it is stamped before the enclosing transaction
/// commits and that transaction may commit arbitrarily later, or not at all.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct ReclaimKey<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub kind: ReclaimKind,
	pub ns: Cow<'a, str>,
	pub db: Option<Cow<'a, str>>,
	pub uid: Uuid,
}

impl_key!(ReclaimKey<'a>);

impl Categorise for ReclaimKey<'_> {
	fn categorise(&self) -> Category {
		Category::Reclaim
	}
}

impl<'a> ReclaimKey<'a> {
	/// An entry naming the whole data prefix of a namespace.
	pub(crate) fn namespace(ns: Cow<'a, str>, uid: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'r',
			_c: b'c',
			kind: ReclaimKind::Namespace,
			ns,
			db: None,
			uid,
		}
	}

	/// An entry naming the whole data prefix of a database.
	pub(crate) fn database(ns: Cow<'a, str>, db: Cow<'a, str>, uid: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'r',
			_c: b'c',
			kind: ReclaimKind::Database,
			ns,
			db: Some(db),
			uid,
		}
	}

	/// The range covering every entry in the reclaim queue.
	pub(crate) fn range() -> (Vec<u8>, Vec<u8>) {
		(b"/!rc\x00".to_vec(), b"/!rc\xff".to_vec())
	}

	/// The range covering every entry naming the namespace `ns` itself.
	pub(crate) fn namespace_range(ns: &str) -> Result<Range<Vec<u8>>, Error> {
		Self::widen(storekey::serialize(&KindNsPrefix::new(ReclaimKind::Namespace, ns))?)
	}

	/// The range covering every entry naming the database `db` of `ns`.
	pub(crate) fn database_range(ns: &str, db: &str) -> Result<Range<Vec<u8>>, Error> {
		Self::widen(storekey::serialize(&DatabasePrefix::new(ns, db))?)
	}

	/// Widens a key prefix into the half-open range of the keys under it.
	///
	/// The upper bound appends `0xff`, which is sound because every field that
	/// can follow one of the prefixes above — an optional UTF-8 identifier or a
	/// `uuid` — encodes below that byte.
	fn widen(start: Vec<u8>) -> Result<Range<Vec<u8>>, Error> {
		let mut end = start.clone();
		end.push(0xff);
		Ok(Range {
			start,
			end,
		})
	}

	/// Reads an entry back from the raw key bytes returned by a scan.
	pub(crate) fn decode_key(k: &[u8]) -> Result<ReclaimKey<'_>, Error> {
		ReclaimKey::decode(k)
	}

	/// The half-open key range a name addresses, as `prefix .. prefix ++ 0xff`.
	///
	/// This is the sole mapping from a namespace or database name to the bytes
	/// it owns, which both the reaper and a reuse of the name destroy; the upper
	/// bound is sound because `0xff` is never a valid UTF-8 byte and so can never
	/// appear in an encoded identifier.
	///
	/// It is keyed by name rather than by queue entry because a name owns its
	/// prefix whether or not an entry still describes it: an entry is retired
	/// once the prefix reads as empty, and a transaction older than the removal
	/// may commit into it after that.
	pub(crate) fn prefix_range_of(ns: &str, db: Option<&str>) -> Result<Range<Vec<u8>>, Error> {
		let start = match db {
			Some(db) => crate::key::database::all::new(ns, db).encode()?,
			None => crate::key::namespace::all::new(ns).encode()?,
		};
		let mut end = start.clone();
		end.push(0xff);
		Ok(Range {
			start,
			end,
		})
	}

	/// The catalog object this entry's prefix belongs to, as the namespace and,
	/// for a database entry, the database inside it.
	///
	/// The kind decides this, so a namespace entry names the namespace however
	/// its `db` field reads, exactly as [`Self::prefix_range`] does.
	pub(crate) fn target(&self) -> (&str, Option<&str>) {
		let db = match self.kind {
			ReclaimKind::Namespace => None,
			ReclaimKind::Database => self.db.as_deref(),
		};
		(&self.ns, db)
	}

	/// The half-open key range this entry names.
	///
	/// The kind decides which prefix is named, so a namespace entry names the
	/// whole namespace however its `db` field reads.
	pub(crate) fn prefix_range(&self) -> Result<Range<Vec<u8>>, Error> {
		let db = match self.kind {
			ReclaimKind::Namespace => None,
			ReclaimKind::Database => Some(self.db.as_deref().ok_or_else(|| {
				Error::Internal("Reclaim entry for a database has no database name".to_string())
			})?),
		};
		Self::prefix_range_of(&self.ns, db)
	}
}

/// The leading fields of every entry naming an object of one kind inside one
/// namespace.
///
/// Serialised alone it is a byte prefix of each of those entries' keys, which
/// is what makes them a contiguous range.
#[derive(Serialize)]
struct KindNsPrefix<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	kind: ReclaimKind,
	ns: Cow<'a, str>,
}

impl<'a> KindNsPrefix<'a> {
	fn new(kind: ReclaimKind, ns: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'r',
			_c: b'c',
			kind,
			ns: Cow::Borrowed(ns),
		}
	}
}

/// The leading fields of every entry naming one database.
#[derive(Serialize)]
struct DatabasePrefix<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	kind: ReclaimKind,
	ns: Cow<'a, str>,
	db: Option<Cow<'a, str>>,
}

impl<'a> DatabasePrefix<'a> {
	fn new(ns: &'a str, db: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'r',
			_c: b'c',
			kind: ReclaimKind::Database,
			ns: Cow::Borrowed(ns),
			db: Some(Cow::Borrowed(db)),
		}
	}
}

/// The mutable progress of one reclaim entry.
///
/// Stored as the value of a [`ReclaimKey`], and rewritten by the reaper as it
/// makes progress. The entry is deleted once its prefix is fully destroyed.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReclaimState {
	/// Unix milliseconds at which the reaper first observed this committed
	/// entry, or `0` when it has not been observed yet.
	///
	/// A pass spends its key budget on the entries that have waited longest, so
	/// this is what puts an entry in line: a prefix too large for one pass keeps
	/// the stamp it was given and so keeps its place. It is taken from this
	/// instant and never from the key's `uid`, because the `uid` is allocated
	/// while the removing statement is still running: that statement's commit can
	/// be delayed arbitrarily or lost entirely, so ordering by `uid` would put an
	/// entry ahead of removals that became visible before it. An instant written
	/// by the reaper, in its own committed transaction, cannot precede the removal
	/// becoming durable.
	pub observed_ms: u64,
	/// The last key of this entry's prefix whose deletion is durable, or `None`
	/// before the first page commits.
	///
	/// It is written in the same transaction as the page of deletions it
	/// accounts for, so it can never claim more progress than was actually
	/// made. An interrupted reclaim resumes strictly after this key rather than
	/// restarting the prefix.
	pub cursor: Option<Vec<u8>>,
}

impl ReclaimState {
	/// The state a removing statement writes: not yet observed, no progress.
	pub(crate) fn enqueued() -> Self {
		Self {
			observed_ms: 0,
			cursor: None,
		}
	}

	/// Whether the reaper has already recorded a first observation.
	pub(crate) fn is_observed(&self) -> bool {
		self.observed_ms != 0
	}

	/// Records the first observation, leaving an existing one untouched.
	///
	/// Returns whether the state changed and so needs writing back.
	pub(crate) fn observe(&mut self, now_ms: u64) -> bool {
		if self.is_observed() {
			return false;
		}
		self.observed_ms = now_ms;
		true
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key_namespace() {
		let val = ReclaimKey::namespace(Cow::Borrowed("testns"), Uuid::from_u128(1));
		let enc = ReclaimKey::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/!rc\0\0\0\0testns\0\0\0\0\0\0\0\0\0\x10\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01"
		);
		let dec = ReclaimKey::decode_key(&enc).unwrap();
		assert_eq!(dec, val);
	}

	#[test]
	fn key_database() {
		let val = ReclaimKey::database(
			Cow::Borrowed("testns"),
			Cow::Borrowed("testdb"),
			Uuid::from_u128(2),
		);
		let enc = ReclaimKey::encode(&val).unwrap();
		let dec = ReclaimKey::decode_key(&enc).unwrap();
		assert_eq!(dec, val);
		assert_eq!(dec.kind, ReclaimKind::Database);
		assert_eq!(dec.db.as_deref(), Some("testdb"));
	}

	#[test]
	fn key_within_range() {
		let (beg, end) = ReclaimKey::range();
		for val in [
			ReclaimKey::namespace(Cow::Borrowed(""), Uuid::nil()),
			ReclaimKey::namespace(Cow::Borrowed("testns"), Uuid::from_u128(u128::MAX)),
			ReclaimKey::database(Cow::Borrowed("testns"), Cow::Borrowed("testdb"), Uuid::now_v7()),
		] {
			let enc = ReclaimKey::encode(&val).unwrap();
			assert!(beg <= enc, "{enc:?} sorts below the queue range");
			assert!(enc < end, "{enc:?} sorts above the queue range");
		}
	}

	#[test]
	fn prefix_range_namespace() {
		let val = ReclaimKey::namespace(Cow::Borrowed("testns"), Uuid::from_u128(1));
		let rng = val.prefix_range().unwrap();
		assert_eq!(rng.start, b"/*testns\0".to_vec());
		assert_eq!(rng.end, b"/*testns\0\xff".to_vec());
	}

	#[test]
	fn prefix_range_database() {
		let val = ReclaimKey::database(
			Cow::Borrowed("testns"),
			Cow::Borrowed("testdb"),
			Uuid::from_u128(1),
		);
		let rng = val.prefix_range().unwrap();
		assert_eq!(rng.start, b"/*testns\0*testdb\0".to_vec());
		assert_eq!(rng.end, b"/*testns\0*testdb\0\xff".to_vec());
	}

	#[test]
	fn object_ranges_select_exactly_their_object() {
		let enc = |k: &ReclaimKey| ReclaimKey::encode(k).unwrap();
		let holds = |r: &Range<Vec<u8>>, k: &ReclaimKey| {
			let k = enc(k);
			r.start <= k && k < r.end
		};
		let ns = ReclaimKey::namespace(Cow::Borrowed("ns"), Uuid::from_u128(1));
		let ns_again = ReclaimKey::namespace(Cow::Borrowed("ns"), Uuid::from_u128(2));
		let ns_other = ReclaimKey::namespace(Cow::Borrowed("nsx"), Uuid::from_u128(3));
		let db = ReclaimKey::database(Cow::Borrowed("ns"), Cow::Borrowed("db"), Uuid::from_u128(4));
		let db_sibling =
			ReclaimKey::database(Cow::Borrowed("ns"), Cow::Borrowed("dbx"), Uuid::from_u128(5));
		let db_other =
			ReclaimKey::database(Cow::Borrowed("nsx"), Cow::Borrowed("db"), Uuid::from_u128(6));
		// Every entry naming the namespace itself, however many there are
		let rng = ReclaimKey::namespace_range("ns").unwrap();
		assert!(holds(&rng, &ns));
		assert!(holds(&rng, &ns_again));
		assert!(!holds(&rng, &ns_other));
		assert!(!holds(&rng, &db));
		// Every entry naming one database, and no name it is a prefix of
		let rng = ReclaimKey::database_range("ns", "db").unwrap();
		assert!(holds(&rng, &db));
		assert!(!holds(&rng, &db_sibling));
		assert!(!holds(&rng, &db_other));
		assert!(!holds(&rng, &ns));
	}

	#[test]
	fn state_keeps_its_first_observation() {
		let mut state = ReclaimState::enqueued();
		assert!(!state.is_observed());
		assert!(state.observe(10_000));
		assert!(state.is_observed());
		// A second sighting leaves the first alone, so the order a pass serves
		// its candidates in does not change under it
		assert!(!state.observe(20_000));
		assert_eq!(state.observed_ms, 10_000);
	}

	#[test]
	fn state_round_trip() {
		let state = ReclaimState {
			observed_ms: 42,
			cursor: Some(b"/*testns\0*testdb\0!tb".to_vec()),
		};
		let enc = revision::to_vec(&state).unwrap();
		let dec: ReclaimState = revision::from_slice(&enc).unwrap();
		assert_eq!(dec, state);
		assert_eq!(dec.cursor.as_deref(), Some(b"/*testns\0*testdb\0!tb".as_slice()));
	}
}
