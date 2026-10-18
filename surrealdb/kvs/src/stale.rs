//! Bounded-staleness reads: the optional backend capability that serves a
//! read-only transaction from an already-published snapshot.
//!
//! A backend with a stale tier publishes a [`StaleReadsHandle`] through
//! [`crate::TransactionBuilder::extension`]. A backend that publishes none is
//! read at its current state, which satisfies every bound, so the capability is
//! never required for a staleness bound to be meaningful.

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use common::future::BoxFut;

use crate::api::Transactable;
use crate::err::Result;

/// The most bytes a [`SnapshotPosition`] may hold, so a transport can bound the
/// field that carries it.
pub const MAX_POSITION_BYTES: usize = 256;

/// An opaque, backend-defined token naming a point in the backend's commit
/// order.
///
/// Comparable only with tokens from the same backend. It carries no namespace,
/// database or credential material.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SnapshotPosition(Vec<u8>);

impl SnapshotPosition {
	/// Wraps `bytes`, or `None` when they exceed [`MAX_POSITION_BYTES`].
	pub fn new(bytes: Vec<u8>) -> Option<Self> {
		(bytes.len() <= MAX_POSITION_BYTES).then_some(Self(bytes))
	}

	pub fn as_bytes(&self) -> &[u8] {
		&self.0
	}
}

/// A request for a snapshot no older than `max_staleness`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadSnapshotRequest {
	pub max_staleness: Duration,
}

/// What a read-only transaction was opened on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotInfo {
	/// The position of the snapshot, when the backend has such a notion.
	pub position: Option<SnapshotPosition>,
	/// The snapshot's age when the transaction opened; zero for a current read.
	pub age: Duration,
	/// Whether the snapshot is older than the current state at open time.
	pub stale: bool,
}

impl SnapshotInfo {
	/// A read of the current state.
	pub fn current() -> Self {
		Self {
			position: None,
			age: Duration::ZERO,
			stale: false,
		}
	}
}

/// The result of asking a stale tier for a snapshot.
pub enum ReadOutcome {
	/// A read transaction over a snapshot within the bound.
	Opened {
		transaction: Box<dyn Transactable>,
		/// Whether the transaction is local to the process, as for
		/// [`crate::TransactionBuilder::new_transaction`].
		local: bool,
		info: SnapshotInfo,
	},
	/// No published snapshot is within the bound. The datastore decides
	/// whether to read the current state instead.
	BoundUnmet {
		/// The age of the newest published snapshot.
		newest_age: Duration,
	},
}

/// A backend's stale tier.
///
/// `open` never falls back to a current read itself: it reports
/// [`ReadOutcome::BoundUnmet`] and leaves the decision to the datastore's
/// fall-back policy. The transaction it returns is an ordinary read
/// transaction, so every write through it fails with
/// [`crate::Error::TransactionReadonly`].
pub trait StaleReads: Any + Send + Sync {
	/// The snapshot publication interval: the smallest bound that is usually
	/// met from a published snapshot.
	fn min_staleness(&self) -> Duration;

	/// The oldest snapshot the tier keeps readable.
	fn max_staleness(&self) -> Duration;

	/// The staleness bound the datastore applies to authentication reads.
	/// Zero where the backend can read its current state.
	fn auth_staleness(&self) -> Duration;

	/// Opens a read transaction over the newest snapshot within the bound.
	fn open(&self, request: ReadSnapshotRequest) -> BoxFut<'_, Result<ReadOutcome>>;
}

/// The [`crate::TransactionBuilder::extension`] key under which a backend
/// publishes its [`StaleReads`] implementation. The newtype is what makes the
/// capability recoverable: `Arc<dyn Any>` downcasts only to a sized type.
pub struct StaleReadsHandle(pub Arc<dyn StaleReads>);
