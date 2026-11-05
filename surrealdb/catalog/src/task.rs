use chrono::{DateTime, Utc};
use revision::revisioned;
use uuid::Uuid;

use crate::impl_kv_value_catalog;
use crate::stored::Stored;

/// Represents a distributed task lease stored in the datastore.
///
/// A TaskLease records which node currently owns the exclusive right to perform
/// a specific task, and when that right expires. The lease is stored in the
/// datastore and checked/updated atomically to ensure only one node can hold
/// the lease at any given time.
///
/// # Fields
/// * `owner` - UUID of the node that currently owns this lease
/// * `expiration` - UTC timestamp when this lease will expire
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash, Stored)]
#[stored(since = "4.0", floor_fixed)]
pub struct TaskLease {
	#[stored(n = 2)]
	pub owner: Uuid,
	#[stored(n = 4)]
	pub expiration: DateTime<Utc>,
}

impl_kv_value_catalog!(TaskLease);

impl TaskLease {
	/// Builds a lease held by `owner` until `expiration`.
	pub fn new(owner: Uuid, expiration: DateTime<Utc>) -> Self {
		Self {
			owner,
			expiration,
		}
	}
}
