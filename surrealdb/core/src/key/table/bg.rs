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

use crate::catalog::IndexId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::kvs::index::{Appending, BuildGeneration, BuildTicket, BuildTicketMutationSeq};
use crate::val::TableName;

key! {
	/// key for one queued mutation in a build generation
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Bg<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b'g',
		pub ix: IndexId,
		pub generation: BuildGeneration,
		pub ticket: BuildTicket,
		pub mutation_seq: BuildTicketMutationSeq,
	}
}

impl_kv_key_storekey!(Bg<'a> => Appending);

key! {
	/// A key prefix for mutations in a build generation
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct BgMutationPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b'g',
		pub ix: IndexId,
		pub generation: BuildGeneration,
		pub ticket: BuildTicket,
	}
}

impl_kv_range_storekey!(BgMutationPrefix<'_>);

impl Categorise for Bg<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBuildAppending
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct BgPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b'g',
		pub ix: IndexId,
	}
}
impl_kv_range_storekey!(BgPrefix<'_>);

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct BgGenerationPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b'g',
		pub ix: IndexId,
		pub generation: BuildGeneration,
	}
}
impl_kv_range_storekey!(BgGenerationPrefix<'_>);

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct BgTicketPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b'g',
		pub ix: IndexId,
		pub generation: BuildGeneration,
		pub ticket: BuildTicket,
	}
}
impl_kv_range_storekey!(BgTicketPrefix<'_>);
