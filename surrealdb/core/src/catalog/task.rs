use chrono::{DateTime, Utc};
use revision::revisioned;
use uuid::Uuid;

use crate::key::impl_kv_value_revisioned;

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
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub(crate) struct TaskLease {
	pub(crate) owner: Uuid,
	pub(crate) expiration: DateTime<Utc>,
}

impl_kv_value_revisioned!(TaskLease);

impl TaskLease {
	#[cfg(test)]
	pub(crate) fn new(owner: Uuid, expiration: DateTime<Utc>) -> Self {
		Self {
			owner,
			expiration,
		}
	}
}
