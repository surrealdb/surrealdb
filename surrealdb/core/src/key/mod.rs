//! Keys, and the traits every key type implements.
//!
//! The layout itself is declared once, in the `schema` module, and every key type
//! in the keyspace is generated from that declaration. This module holds what the
//! generated code is written against: `KVKey` and `KVKeyDecode` for a single key,
//! `KVSubspace` and `KVRange` for the regions a scan can address, and the
//! `TypedRange`/[`RawRange`] wrappers that carry a range's element type from the
//! bound that produced it to the transaction method that reads it.
//!
//! Generated names follow the layout: `XxKey` addresses one key, `XxRoot` is the
//! root of a level and prefixes everything beneath it, and `XxPrefix` is a bound
//! truncating a key at a declared point. Only the first stores a value; the
//! other two exist to be scanned, and produce a `TypedRange` when every key they
//! cover holds the same type and a `RawRange` when they span a whole region.
//!
//! Sigils, in the order a key spells them:
//!
//! - `/` the root of the keyspace, and `!` a catalog definition under it
//! - `*` a step down a level: namespace, database, table, then record
//! - `$` per-node state, `&` access grants at the root and references on a table
//! - `#` change feed, `%` live events, `~` graph edges
//! - `+` an index, whose entries are the only keys that encode under `storekey`'s `IndexFormat`
//!
//! The map below is generated from the same schema as the encoders and checked in
//! as `schema::KEYSPACE_MAP`, so it cannot drift from the code: a test fails if
//! the two disagree. Regenerate it with
//! `RESULT=OVERWRITE cargo test -p surrealdb-core keyspace_map`.
//!
//! ```text
#![doc = include_str!("keyspace.map")]
//! ```
//!

use std::fmt::Debug;
use std::marker::PhantomData;

use anyhow::Result;
pub(crate) use surrealdb_kvs::value::KVValue;

use crate::idx::planner::ScanDirection;

pub(crate) mod error;
pub(crate) mod mac;
pub(crate) mod reclaim;
pub(crate) mod schema;

pub(crate) use error::Error;
pub(crate) use mac::impl_kv_value_revisioned;
// Needs to be public for the enterprise crate.
pub use surrealdb_kvs::{Key, KeyRange};

/// KVKey is a trait that defines a key for the key-value store.
pub(crate) trait KVKey: Debug + Sized {
	/// The associated value type for this key.
	type Value: KVValue;

	/// Encodes the key into a byte vector.
	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()>;

	/// Encodes the key into a byte vector.
	fn encode_key(&self) -> Result<Key<'static>> {
		let mut res = Vec::new();
		self.encode_buffer(&mut res)?;
		Ok(Key::from(res))
	}

	/// Returns the context the value decoder needs to reconstruct fields
	/// derived from the key. For most key types this is `()`; for
	/// `RecordKey` it is the `RecordId` used to inject the canonical `id`
	/// during record decode. Encode never needs the key (the value is
	/// self-describing on encode), only decode does.
	fn value_context(&self) -> <Self::Value as KVValue>::KeyContext;
}

pub(crate) trait KVKeyDecode<'a>: Sized {
	fn decode_key(bytes: &'a [u8]) -> Result<Self>;
}

/// A byte range whose keys all store `V`.
///
/// The only way to build one is from a generated bound, so a range handed to the
/// datastore is always one the schema says exists, and the value type travels with
/// it: a scan over it returns `V`, not bytes to be guessed at.
#[derive(Debug)]
pub(crate) struct TypedRange<V> {
	range: KeyRange<'static>,
	value: PhantomData<V>,
}

/// Cloning copies the bytes; the value type is a marker and constrains nothing.
impl<V> Clone for TypedRange<V> {
	fn clone(&self) -> Self {
		TypedRange::new(self.range.clone())
	}
}

impl<V> TypedRange<V> {
	/// Wraps a range built by generated code. Private to this module so a caller
	/// cannot claim a value type for bytes that do not hold it.
	fn new(range: KeyRange<'static>) -> Self {
		TypedRange {
			range,
			value: PhantomData,
		}
	}

	/// The inclusive first key of the range. For diagnostics.
	pub(crate) fn start(&self) -> &Key<'static> {
		&self.range.start
	}

	/// The exclusive last key of the range. For diagnostics.
	pub(crate) fn end(&self) -> &Key<'static> {
		&self.range.end
	}
}

impl<V> Resumable for TypedRange<V> {
	fn resume_after(mut self, key: &[u8], dir: ScanDirection) -> Self {
		self.range = resume(self.range, key, dir);
		self
	}
}

impl RawRange {
	/// The inclusive first key of the range. For diagnostics and for tests that
	/// assert on the layout; nothing in the engine reads a region's own bounds.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn start(&self) -> &Key<'static> {
		&self.0.start
	}

	/// The exclusive last key of the range. See [`Self::start`].
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn end(&self) -> &Key<'static> {
		&self.0.end
	}
}

impl Resumable for RawRange {
	fn resume_after(mut self, key: &[u8], dir: ScanDirection) -> Self {
		self.0 = resume(self.0, key, dir);
		self
	}
}

impl RawRange {
	/// Every byte string from `0x00` up to (but excluding) `0xff`, declared or not.
	///
	/// A range no bound produced is the one thing the declared keyspace exists to
	/// prevent, so this exists for the single use that cannot be expressed from a
	/// bound by construction: looking for keys the keyspace does *not* describe.
	/// That is what the leaked-key check after a test run does, and what a scan of
	/// unknown data needs before [`schema::describe`] can name what it found.
	///
	/// The `0xff` ceiling is a limit of [`KeyRange`], which has no unbounded end: a
	/// key at or above `0xff` is outside this range. Every declared key begins with
	/// `/` or `!`, so nothing the engine writes is missed, but a stray key in that
	/// band would not be reported.
	///
	/// Hidden rather than private because the language-test harness performs that
	/// check from outside this crate. Nothing in the engine should reach for it.
	#[doc(hidden)]
	pub fn every_key() -> Self {
		RawRange(KeyRange {
			start: Key::from(&[0x00u8][..]),
			end: Key::from(&[0xffu8][..]),
		})
	}

	/// Every byte string ordering after `key`, up to the same ceiling as
	/// [`Self::every_key`].
	///
	/// Spans the whole byte space rather than a declared subtree, so a caller
	/// asking "is there anything else in this store?" is not answered only for the
	/// keys this release happens to describe.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn every_key_after(key: &Key<'_>) -> Self {
		RawRange(KeyRange {
			start: key.clone().into_static().next(),
			end: Key::from(&[0xffu8][..]),
		})
	}

	/// A range over byte strings that are not keys at all.
	///
	/// Only the storage-layer tests want this: they write synthetic keys to check
	/// batching, metrics and cursor behaviour, which has nothing to do with what the
	/// keys mean.
	#[cfg(test)]
	pub(crate) fn of_bytes(start: &[u8], end: &[u8]) -> Self {
		RawRange(KeyRange {
			start: Key::from(start.to_vec()),
			end: Key::from(end.to_vec()),
		})
	}
}

/// Moves whichever end of `range` a scan in `dir` has reached past `key`.
///
/// Forward, the start becomes the key's immediate successor; backward, the
/// exclusive end becomes the key itself, which is the same thing seen from the
/// other side.
fn resume(mut range: KeyRange<'static>, key: &[u8], dir: ScanDirection) -> KeyRange<'static> {
	match dir {
		ScanDirection::Forward => {
			range.start = Key::from(key.to_vec()).next();
		}
		ScanDirection::Backward => {
			range.end = Key::from(key.to_vec());
		}
	}
	range
}

/// A byte range over a region that holds more than one kind of key, or none.
///
/// Reading one means decoding by hand, which is why every method that accepts it
/// and returns bytes says `_raw` in its name.
#[derive(Clone, Debug)]
pub struct RawRange(KeyRange<'static>);

/// A range the datastore can act on, whatever the keys inside it store.
///
/// Deleting and counting do not read values, so they take this rather than
/// [`TypedRange`]; so do the `_raw` reads, where the caller has said in the method
/// name that bytes are what they want.
pub(crate) trait AnyRange {
	fn into_key_range(self) -> KeyRange<'static>;
}

/// A range a partly-finished scan can pick up from.
///
/// Narrowing a range to a suffix of itself keeps whatever its type promised, so
/// both kinds offer it and code that walks a range in chunks can be written once,
/// whichever kind it was handed.
pub(crate) trait Resumable: Sized {
	/// The same range, resumed just after `key`, with `dir` saying which end moves.
	fn resume_after(self, key: &[u8], dir: ScanDirection) -> Self;
}

impl<V> AnyRange for TypedRange<V> {
	fn into_key_range(self) -> KeyRange<'static> {
		self.range
	}
}

impl AnyRange for RawRange {
	fn into_key_range(self) -> KeyRange<'static> {
		self.0
	}
}

/// A named region of the keyspace: a level's whole subtree, or a bound whose
/// contents are of more than one type.
///
/// A subspace can be destroyed or counted, and that is all. It deliberately
/// offers no typed scan: the keys inside it are of several types, so a scan would
/// have to hand back bytes and leave the caller to guess which type each one is —
/// which is what the declared keyspace exists to stop. Read one with the `_raw`
/// methods on [`crate::kvs::Transaction`], which say in their name that the caller
/// has taken that decoding on.
pub(crate) trait KVSubspace {
	/// Encodes the bytes this region starts at.
	fn encode_bound(&self) -> Result<Key<'static>>;

	/// Every key strictly beneath these bytes, as a range the datastore accepts.
	///
	/// The start is these bytes followed by `0x00`, so the region's own bytes are
	/// excluded; the end is the successor of its last non-`0xFF` byte, which the
	/// schema check guarantees exists.
	fn raw_range(&self) -> Result<RawRange> {
		Ok(RawRange(self.encode_bound()?.prefix_expect()))
	}

	/// Wraps a range this region built by other means — its own bytes together with
	/// everything beneath them.
	fn raw(&self, range: KeyRange<'static>) -> RawRange {
		RawRange(range)
	}
}

/// A region of the keyspace whose every key stores the same type, so a scan over
/// it can hand back decoded values rather than bytes.
///
/// Generated for each truncation point in the schema whose subtree holds exactly
/// one value type. A bound over a mixed subtree is a [`KVSubspace`] instead.
pub(crate) trait KVRange: KVSubspace {
	/// The value every key in this range stores.
	///
	/// Bounded to values that decode without help from their key, because a range
	/// is not a key and so has no context to give. A value that carries part of its
	/// identity in the key — a record's id, for one — is read one key at a time
	/// instead, where [`KVKey::value_context`] can supply it.
	type Value: KVValue<KeyContext = ()>;

	/// Every key strictly beneath these bytes, carrying the value type they store.
	fn typed_range(&self) -> Result<TypedRange<Self::Value>> {
		Ok(TypedRange::new(self.encode_bound()?.prefix_expect()))
	}

	/// Wraps a range this bound built by other means — its own bytes together with
	/// everything beneath them, or a slice of them selected by field value.
	fn typed(&self, range: KeyRange<'static>) -> TypedRange<Self::Value> {
		TypedRange::new(range)
	}
}
