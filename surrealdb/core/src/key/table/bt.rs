//! Stores the writer-admission ticket counter for one index build generation.
//!
//! Allocating a ticket is the one piece of writer admission that every indexed
//! write performs, so the counter lives on its own generation-scoped key rather
//! than on `!bs`. The builder's initial-scan batch compare-and-swaps `!bs`
//! twice — the ownership heartbeat before the batch and the progress checkpoint
//! after it — and holds that transaction open for the whole batch, so a counter
//! sharing `!bs` would let every admitted write invalidate the batch in flight.
//!
//! The key exists for exactly as long as its generation is the active one: the
//! transaction that installs a generation creates it, and the transaction that
//! installs the next generation removes it. Writers therefore always
//! compare-and-swap a key that is present, and both sides of a generation flip
//! read before they write. That is what fences an in-flight admission against a
//! concurrent flip on last-writer-wins backends (TiKV, IndexedDB, SurrealDS),
//! where a blind write never conflicts — see `multiwriter_same_keys_allow` in
//! the shared `kvs-test` suite.
use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::kvs::index::{BuildGeneration, BuildTicket};
use crate::val::TableName;

key! {
	/// A key for the ticket counter of one build generation.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Bt<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b't',
		pub ix: IndexId,
		pub generation: BuildGeneration,
	}
}

impl_kv_key_storekey!(Bt<'a> => BuildTicket);

impl Categorise for Bt<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBuildTicketCounter
	}
}

key! {
	/// A range prefix covering the ticket counters of every generation of an index.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct BtGenerationPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b't',
		pub ix: IndexId,
	}
}

impl_kv_range_storekey!(BtGenerationPrefix<'_>);
