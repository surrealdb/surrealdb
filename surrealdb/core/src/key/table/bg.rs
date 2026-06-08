//! Stores durable appended index operations for an index build generation.
//!
//! `!bg{ix}{generation}{ticket}{mutation_seq}` entries are written by user
//! transactions that were admitted while an index was building. A single
//! admitted user transaction may write multiple `!bg` entries that all share
//! the same `(generation, ticket)` — one reservation is allocated per user
//! transaction per index, and each indexed mutation in that transaction
//! receives a distinct `mutation_seq`. The builder replays them in storage
//! order and deletes each entry in the same transaction that updates index
//! data.
use std::borrow::Cow;
use std::ops::Range;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::index::{Appending, BuildGeneration, BuildTicket, BuildTicketMutationSeq};
use crate::kvs::{KVKey, Key, impl_kv_key_storekey};
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Bg<'a> {
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
	pub mutation_seq: BuildTicketMutationSeq,
}

impl_kv_key_storekey!(Bg<'_> => Appending);

impl Categorise for Bg<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBuildAppending
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode)]
#[storekey(format = "()")]
pub(crate) struct BgPrefix<'a> {
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

impl_kv_key_storekey!(BgPrefix<'_> => ());

impl<'a> Bg<'a> {
	/// Create a key for one queued mutation in a build generation.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		generation: BuildGeneration,
		ticket: BuildTicket,
		mutation_seq: BuildTicketMutationSeq,
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
			_f: b'g',
			ix,
			generation,
			ticket,
			mutation_seq,
		}
	}

	/// Return the ordered ticket range for queued mutations in one generation.
	pub(crate) fn range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		generation: BuildGeneration,
	) -> anyhow::Result<Range<Vec<u8>>> {
		let beg =
			Self::new(ns, db, tb, ix, generation, BuildTicket::MIN, BuildTicketMutationSeq::MIN)
				.encode_key()?;
		let end =
			Self::new(ns, db, tb, ix, generation, BuildTicket::MAX, BuildTicketMutationSeq::MAX)
				.encode_key()?;
		Ok(beg..end)
	}

	/// Return the range covering all queued mutations for a single
	/// `(generation, ticket)` reservation — i.e. all `mutation_seq` values.
	pub(crate) fn ticket_range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		generation: BuildGeneration,
		ticket: BuildTicket,
	) -> anyhow::Result<Range<Vec<u8>>> {
		let beg = Self::new(ns, db, tb, ix, generation, ticket, BuildTicketMutationSeq::MIN)
			.encode_key()?;
		let end = Self::new(ns, db, tb, ix, generation, ticket, BuildTicketMutationSeq::MAX)
			.encode_key()?;
		Ok(beg..end)
	}

	/// Return the range covering queued mutations for every generation.
	pub(crate) fn all_generations_range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
	) -> anyhow::Result<Range<Key>> {
		let mut beg = BgPrefix {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'!',
			_e: b'b',
			_f: b'g',
			ix,
		}
		.encode_key()?;
		let mut end = beg.clone();
		beg.push(0);
		end.push(0xff);
		Ok(beg..end)
	}

	/// Decode a stored queue key back into its generation and ticket.
	pub(crate) fn decode_key(k: &[u8]) -> anyhow::Result<Bg<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}
