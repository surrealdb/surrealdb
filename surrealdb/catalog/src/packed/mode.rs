//! The two instantiations of the packed-row walk.
//!
//! A row is walked by one body of code, generic over a [`Mode`]. [`Build`] constructs each value
//! it reads, and is the whole decode ([`PackedRow::decode`](super::PackedRow::decode)). [`Check`]
//! reads the same bytes through the same code and constructs nothing, and is the check a row
//! passes before it is read in place ([`StoredRecord::checked_packed`]). Every refusal is made by
//! code both modes run, at the same position of the walk, so the two accept the same rows and
//! refuse them with the same error, whatever fields a reader asks of the checked row.
//!
//! Generic code never looks inside an `Out`: it combines walked values only through the methods
//! of [`Mode`], and the closures it passes run only in [`Build`].
//!
//! [`StoredRecord::checked_packed`]: crate::record::StoredRecord::checked_packed

use std::cmp::Ordering;

use super::PackedError;
use super::kind::{KindOps, Slot};
use crate::layout::Column;
use crate::val::stored::{self, ObjectRef, ValueRef};
use crate::val::{Object, RecordIdKey, Strand, Value};

/// What a walk of a packed row produces.
pub(crate) trait Mode {
	/// A walked `T`: the value itself in [`Build`], nothing in [`Check`].
	type Out<T>;
	/// The entries of an object node being walked: a `Vec` in [`Build`], nothing in [`Check`].
	type Seq<T>;
	/// A remainder's grammar value, read through [`RemainderView`].
	type Remainder<'a>: RemainderView;

	/// Wraps the value `f` constructs. `f` is not called in [`Check`].
	fn make<T>(f: impl FnOnce() -> T) -> Self::Out<T>;
	/// Applies `f` to a walked value. `f` is not called in [`Check`].
	fn map<T, U>(v: Self::Out<T>, f: impl FnOnce(T) -> U) -> Self::Out<U>;
	/// Pairs two walked values.
	fn zip<A, B>(a: Self::Out<A>, b: Self::Out<B>) -> Self::Out<(A, B)>;
	/// An empty sequence of entries with room for `capacity`.
	fn seq<T>(capacity: usize) -> Self::Seq<T>;
	/// Appends a walked entry.
	fn push<T>(seq: &mut Self::Seq<T>, item: Self::Out<T>);

	/// Walks the grammar value `bytes`, nested `depth` containers deep.
	fn value(bytes: &[u8], depth: usize) -> Result<Self::Out<Value>, stored::Error>;
	/// Walks the record id key `bytes`, nested `depth` containers deep.
	fn key(bytes: &[u8], depth: usize) -> Result<Self::Out<RecordIdKey>, stored::Error>;
	/// Walks a remainder's grammar bytes, nested `depth` containers deep.
	fn remainder(bytes: &[u8], depth: usize) -> Result<Self::Remainder<'_>, stored::Error>;
	/// Walks the present slot of a leaf column with the kind's function of this mode.
	fn slot(
		ops: &KindOps,
		column: &Column,
		index: u32,
		slot: Slot<'_>,
		depth: usize,
	) -> Result<Self::Out<Value>, PackedError>;
	/// The object node of the walked `entries` and its `remainder`, when it has one. The entries
	/// and the remainder's keys ascend and share none, which the walk has already checked.
	fn assemble(
		entries: Self::Seq<(Strand, Value)>,
		remainder: Option<Self::Remainder<'_>>,
	) -> Self::Out<Object>;
}

/// The walk that constructs the values it reads.
pub(crate) enum Build {}

/// The walk that reads and checks the bytes and constructs nothing, so it allocates nothing for
/// the kinds whose check does not need their value.
pub(crate) enum Check {}

impl Mode for Build {
	type Out<T> = T;
	type Seq<T> = Vec<T>;
	type Remainder<'a> = Value;

	#[inline(always)]
	fn make<T>(f: impl FnOnce() -> T) -> T {
		f()
	}

	#[inline(always)]
	fn map<T, U>(v: T, f: impl FnOnce(T) -> U) -> U {
		f(v)
	}

	#[inline(always)]
	fn zip<A, B>(a: A, b: B) -> (A, B) {
		(a, b)
	}

	#[inline(always)]
	fn seq<T>(capacity: usize) -> Vec<T> {
		Vec::with_capacity(capacity)
	}

	#[inline(always)]
	fn push<T>(seq: &mut Vec<T>, item: T) {
		seq.push(item);
	}

	#[inline]
	fn value(bytes: &[u8], depth: usize) -> Result<Value, stored::Error> {
		stored::decode_nested(bytes, depth)
	}

	#[inline]
	fn key(bytes: &[u8], depth: usize) -> Result<RecordIdKey, stored::Error> {
		stored::decode_key_nested(bytes, depth)
	}

	#[inline]
	fn remainder(bytes: &[u8], depth: usize) -> Result<Value, stored::Error> {
		stored::decode_nested(bytes, depth)
	}

	#[inline]
	fn slot(
		ops: &KindOps,
		column: &Column,
		index: u32,
		slot: Slot<'_>,
		depth: usize,
	) -> Result<Value, PackedError> {
		(ops.decode)(column, index, slot, depth)
	}

	#[inline]
	fn assemble(entries: Vec<(Strand, Value)>, remainder: Option<Value>) -> Object {
		match remainder {
			Some(Value::Object(remainder)) => merged(entries, remainder),
			Some(_) | None => {
				Object(surrealdb_collections::VecMap::from_sorted_vec_unchecked(entries))
			}
		}
	}
}

/// The object of the entries of a node's columns and its remainder `remainder`, in key order. The
/// two share no key, which [`Collisions`] has checked.
#[inline(never)]
fn merged(entries: Vec<(Strand, Value)>, remainder: Object) -> Object {
	use surrealdb_collections::VecMap;
	let mut merged = Vec::with_capacity(entries.len().saturating_add(remainder.len()));
	let mut left = entries.into_iter().peekable();
	let mut right = remainder.into_iter().peekable();
	loop {
		let next = match (left.peek(), right.peek()) {
			(Some((a, _)), Some((b, _))) => match a.cmp(b) {
				Ordering::Greater => right.next(),
				Ordering::Less => left.next(),
				Ordering::Equal => {
					debug_assert!(false, "a key in a column and in its node's remainder");
					left.next()
				}
			},
			(Some(_), None) => left.next(),
			(None, Some(_)) => right.next(),
			(None, None) => break,
		};
		merged.extend(next);
	}
	Object(VecMap::from_sorted_vec_unchecked(merged))
}

impl Mode for Check {
	type Out<T> = ();
	type Seq<T> = ();
	type Remainder<'a> = StoredView<'a>;

	#[inline(always)]
	fn make<T>(_: impl FnOnce() -> T) {}

	#[inline(always)]
	fn map<T, U>(_: (), _: impl FnOnce(T) -> U) {}

	#[inline(always)]
	fn zip<A, B>(_: (), _: ()) {}

	#[inline(always)]
	fn seq<T>(_: usize) {}

	#[inline(always)]
	fn push<T>(_: &mut (), _: ()) {}

	#[inline]
	fn value(bytes: &[u8], depth: usize) -> Result<(), stored::Error> {
		stored::validate_nested(bytes, depth)
	}

	#[inline]
	fn key(bytes: &[u8], depth: usize) -> Result<(), stored::Error> {
		stored::validate_key_nested(bytes, depth)
	}

	/// Validates the remainder and reads its facts in the one walk over its entries.
	#[inline]
	fn remainder(bytes: &[u8], depth: usize) -> Result<StoredView<'_>, stored::Error> {
		let mut empty = true;
		let mut id = false;
		let mut none = false;
		let object = stored::validate_entries_nested(bytes, depth, |key, tag| {
			empty = false;
			id |= key == "id";
			none |= tag == stored::tag::NONE;
		})?;
		if !object {
			return Ok(StoredView {
				object: None,
				facts: Facts::NOT_AN_OBJECT,
			});
		}
		Ok(StoredView {
			object: ValueRef::new_nested(bytes, depth)?.as_object()?,
			facts: Facts {
				object: true,
				empty,
				id,
				none,
			},
		})
	}

	#[inline]
	fn slot(
		ops: &KindOps,
		column: &Column,
		index: u32,
		slot: Slot<'_>,
		depth: usize,
	) -> Result<(), PackedError> {
		(ops.check)(column, index, slot, depth)
	}

	#[inline(always)]
	fn assemble(_: (), _: Option<StoredView<'_>>) {}
}

/// What the remainder rules read of a remainder's value.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Facts {
	/// The value is an object.
	pub(crate) object: bool,
	/// The object has no key.
	pub(crate) empty: bool,
	/// The object holds the key `"id"`.
	pub(crate) id: bool,
	/// The object holds a `NONE` among its values.
	pub(crate) none: bool,
}

impl Facts {
	/// The facts of a value that is not an object.
	const NOT_AN_OBJECT: Self = Self {
		object: false,
		empty: false,
		id: false,
		none: false,
	};
}

/// A remainder's value as the rules over remainders read it: the value built by [`Build`], or the
/// bytes [`Check`] walked. Both are answered the same way, so [`refuse_remainder`] and
/// [`Collisions`] are one body for both modes.
pub(crate) trait RemainderView {
	/// The facts the remainder rules read.
	fn facts(&self) -> Result<Facts, stored::Error>;
	/// The bytes of the key of entry `at`, in ascending key order; `None` past the last entry or
	/// when the value is no object.
	fn key(&self, at: usize) -> Result<Option<&[u8]>, stored::Error>;
}

impl RemainderView for Value {
	fn facts(&self) -> Result<Facts, stored::Error> {
		Ok(match self {
			Value::Object(object) => Facts {
				object: true,
				empty: object.is_empty(),
				id: object.contains_key("id"),
				none: object.values().any(|value| matches!(value, Value::None)),
			},
			_ => Facts::NOT_AN_OBJECT,
		})
	}

	fn key(&self, at: usize) -> Result<Option<&[u8]>, stored::Error> {
		Ok(match self {
			Value::Object(object) => {
				object.0.as_slice().get(at).map(|(key, _)| key.as_str().as_bytes())
			}
			_ => None,
		})
	}
}

/// A remainder's bytes, read in place: its object when it is one, and the facts the walk that
/// validated it read of its entries.
#[derive(Clone, Copy, Debug)]
pub(crate) struct StoredView<'a> {
	object: Option<ObjectRef<'a>>,
	facts: Facts,
}

impl RemainderView for StoredView<'_> {
	fn facts(&self) -> Result<Facts, stored::Error> {
		Ok(self.facts)
	}

	fn key(&self, at: usize) -> Result<Option<&[u8]>, stored::Error> {
		match self.object {
			Some(object) if at < object.len() => Ok(Some(object.key_bytes(at)?)),
			_ => Ok(None),
		}
	}
}

/// The refusal of a present remainder whose value breaks a rule, in the order the rules apply: the
/// value is an object, it has a key, the root's holds no `"id"`, and none of its values is `NONE`.
/// `column` is the remainder's column and `root` says whether it is the record root's.
pub(crate) fn refuse_remainder(facts: Facts, column: u32, root: bool) -> Result<(), PackedError> {
	let slot = |reason| PackedError::Slot {
		column,
		reason,
	};
	if !facts.object {
		return Err(slot("a remainder that is not an object"));
	}
	if facts.empty {
		return Err(slot("a remainder with no keys"));
	}
	if root && facts.id {
		return Err(PackedError::RemainderId);
	}
	if facts.none {
		return Err(slot("NONE in a remainder"));
	}
	Ok(())
}

/// Whether a key of an object node's columns is also a key of its remainder, found as the walk
/// visits the keys the node holds in ascending order.
///
/// The node's own keys and the remainder's both ascend, so one cursor over the remainder's keys
/// finds every key in both. A collision is only recorded as it is found, and raised by
/// [`finish`](Self::finish) when the node ends, after every column and nested node of it, where
/// the node's entries are merged with its remainder.
#[derive(Debug)]
pub(crate) struct Collisions {
	column: u32,
	next: usize,
	found: bool,
}

impl Collisions {
	/// The collisions of the node whose remainder is column `column`.
	pub(crate) const fn new(column: u32) -> Self {
		Self {
			column,
			next: 0,
			found: false,
		}
	}

	/// Visits `name`, a key the node holds, which must be above every key visited before it.
	///
	/// Kept out of line: only a node with a remainder reaches it, and inlined into the walk of the
	/// columns it costs every column of every other node a spilled register and a longer loop.
	#[inline(never)]
	pub(crate) fn visit(
		&mut self,
		remainder: &impl RemainderView,
		name: &str,
	) -> Result<(), PackedError> {
		let column = self.column;
		let bad = move |error| PackedError::Stored {
			column,
			error,
		};
		while let Some(key) = remainder.key(self.next).map_err(bad)? {
			match key.cmp(name.as_bytes()) {
				Ordering::Less => self.next += 1,
				Ordering::Equal => {
					self.found = true;
					break;
				}
				Ordering::Greater => break,
			}
		}
		Ok(())
	}

	/// Refuses the node when a key it holds is also in its remainder.
	pub(crate) fn finish(self) -> Result<(), PackedError> {
		if self.found {
			return Err(PackedError::KeyInColumnAndRemainder {
				column: self.column,
			});
		}
		Ok(())
	}
}
