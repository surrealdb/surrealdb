//! Stores durable writer reservations for an index build generation.
//!
//! Reservations are created before the user write transaction commits. They are
//! released from a short transaction after the writer commits or rolls back, and
//! the builder may also clear them once the matching durable appending is visible.
//! `Closing` waits for them so the builder cannot publish `Online` while an
//! already admitted writer is still deciding whether to commit or roll back.
use std::borrow::Cow;
use std::ops::Range;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::index::{BuildGeneration, BuildTicket, IndexBuildReservation};
use crate::kvs::{KVKey, Key, impl_kv_key_storekey};
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Br<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ix: IndexId,
	pub generation: BuildGeneration,
	pub ticket: BuildTicket,
}

impl_kv_key_storekey!(Br<'_> => IndexBuildReservation);

impl Categorise for Br<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBuildReservation
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode)]
#[storekey(format = "()")]
pub(crate) struct BrPrefix<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ix: IndexId,
}

impl_kv_key_storekey!(BrPrefix<'_> => ());

impl<'a> Br<'a> {
	/// Create a key for one writer reservation in a build generation.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		generation: BuildGeneration,
		ticket: BuildTicket,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'!',
			_e: b'b',
			_f: b'r',
			ix,
			generation,
			ticket,
		}
	}

	/// Return the ordered ticket range for reservations in one generation.
	pub(crate) fn range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		generation: BuildGeneration,
	) -> anyhow::Result<Range<Vec<u8>>> {
		let beg = Self::new(ns, db, tb, ix, generation, BuildTicket::MIN).encode_key()?;
		let end = Self::new(ns, db, tb, ix, generation, BuildTicket::MAX).encode_key()?;
		Ok(beg..end)
	}

	/// Return the range covering reservations for every generation.
	pub(crate) fn all_generations_range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
	) -> anyhow::Result<Range<Key>> {
		let mut beg = BrPrefix {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'!',
			_e: b'b',
			_f: b'r',
			ix,
		}
		.encode_key()?;
		let mut end = beg.clone();
		beg.push(0);
		end.push(0xff);
		Ok(beg..end)
	}

	/// Decode a stored reservation key back into its generation and ticket.
	pub(crate) fn decode_key(k: &[u8]) -> anyhow::Result<Br<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}
