//! Stores the primary appending ticket for a record during initial index build.
//!
//! `!bp{ix}{generation}{record}` points from a record to its first durable
//! appending ticket. During the initial scan, this lets the builder index the
//! queued old state instead of a newer record version.
use std::borrow::Cow;
use std::ops::Range;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::index::{BuildGeneration, PrimaryAppendingTicket};
use crate::kvs::{KVKey, Key, impl_kv_key_storekey};
use crate::val::{IndexFormat, RecordIdKey, TableName};

fn advance_key(key: &mut [u8]) {
	for b in key.iter_mut().rev() {
		*b = b.wrapping_add(1);
		if *b != 0 {
			break;
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "IndexFormat")]
pub(crate) struct Bp<'a> {
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
	pub id: RecordIdKey,
}

impl KVKey for Bp<'_> {
	type ValueType = PrimaryAppendingTicket;

	fn encode_key(&self) -> anyhow::Result<Vec<u8>> {
		Ok(storekey::encode_vec_format::<IndexFormat, _>(self)
			.map_err(|_| crate::err::Error::Unencodable)?)
	}

	fn value_context(&self) {}
}

impl Categorise for Bp<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBuildPrimaryAppending
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode)]
#[storekey(format = "()")]
pub(crate) struct BpPrefix<'a> {
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

impl_kv_key_storekey!(BpPrefix<'_> => ());

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode)]
#[storekey(format = "()")]
pub(crate) struct BpGenerationPrefix<'a> {
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
}

impl_kv_key_storekey!(BpGenerationPrefix<'_> => ());

impl<'a> Bp<'a> {
	/// Create a key mapping a record to its first queued mutation ticket.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		generation: BuildGeneration,
		id: RecordIdKey,
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
			_f: b'p',
			ix,
			generation,
			id,
		}
	}

	#[cfg(test)]
	/// Return the record-keyed range for primary appending markers in one generation.
	pub(crate) fn range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		generation: BuildGeneration,
	) -> anyhow::Result<Range<Key>> {
		let mut beg = BpGenerationPrefix {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'!',
			_e: b'b',
			_f: b'p',
			ix,
			generation,
		}
		.encode_key()?;
		let mut end = beg.clone();
		beg.push(0);
		end.push(0xff);
		Ok(beg..end)
	}

	/// Return a generation-scoped record-id span for primary appending markers.
	///
	/// The lower bound is exclusive when `after` is set, and the upper bound is
	/// inclusive when `through` is set. COUNT index builds use this to merge the
	/// initial live-record scan with queued old-state markers in record order.
	pub(crate) fn span_range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		generation: BuildGeneration,
		after: Option<&RecordIdKey>,
		through: Option<&RecordIdKey>,
	) -> anyhow::Result<Range<Key>> {
		let start = if let Some(after) = after {
			let mut key = Self::new(ns, db, tb, ix, generation, after.clone()).encode_key()?;
			advance_key(&mut key);
			key
		} else {
			let mut key = BpGenerationPrefix {
				__: b'/',
				_a: b'*',
				ns,
				_b: b'*',
				db,
				_c: b'*',
				tb: Cow::Borrowed(tb),
				_d: b'!',
				_e: b'b',
				_f: b'p',
				ix,
				generation,
			}
			.encode_key()?;
			key.push(0);
			key
		};
		let end = if let Some(through) = through {
			let mut key = Self::new(ns, db, tb, ix, generation, through.clone()).encode_key()?;
			advance_key(&mut key);
			key
		} else {
			let mut key = BpGenerationPrefix {
				__: b'/',
				_a: b'*',
				ns,
				_b: b'*',
				db,
				_c: b'*',
				tb: Cow::Borrowed(tb),
				_d: b'!',
				_e: b'b',
				_f: b'p',
				ix,
				generation,
			}
			.encode_key()?;
			key.push(0xff);
			key
		};
		Ok(start..end)
	}

	/// Return the range covering primary appending markers for every generation.
	pub(crate) fn all_generations_range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
	) -> anyhow::Result<Range<Key>> {
		let mut beg = BpPrefix {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'!',
			_e: b'b',
			_f: b'p',
			ix,
		}
		.encode_key()?;
		let mut end = beg.clone();
		beg.push(0);
		end.push(0xff);
		Ok(beg..end)
	}

	/// Decode a stored primary-appending marker key.
	pub(crate) fn decode_key(k: &[u8]) -> anyhow::Result<Bp<'_>> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(k)?)
	}
}
