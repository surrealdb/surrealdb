//! Stores durable writer reservations for an index build generation.
//!
//! Reservations are created before the user write transaction commits. They are
//! released from a short transaction after the writer commits or rolls back, and
//! the builder may also clear them once the matching durable appending is visible.
//! `Closing` waits for them so the builder cannot publish `Online` while an
//! already admitted writer is still deciding whether to commit or roll back.
use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::kvs::index::{BuildGeneration, BuildTicket, IndexBuildReservation};
use crate::val::TableName;

key! {
	/// A key for one writer reservation in a build generation.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Br<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b'r',
		pub ix: IndexId,
		pub generation: BuildGeneration,
		pub ticket: BuildTicket,
	}
}

impl_kv_key_storekey!(Br<'a> => IndexBuildReservation);

key! {
	/// A key for one writer reservation in a build generation.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct BrTicketPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b'r',
		pub ix: IndexId,
		pub generation: BuildGeneration,
	}
}
impl_kv_range_storekey!(BrTicketPrefix<'_>);

key! {
	/// A key for one writer reservation in a build generation.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct BrGenerationPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b'r',
		pub ix: IndexId,
	}
}
impl_kv_range_storekey!(BrGenerationPrefix<'_>);
