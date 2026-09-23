//! Stores a task lease to ensure only one node is running the task at a time

use serde::{Deserialize, Serialize};

use crate::key::category::{Categorise, Category};
use crate::kvs::impl_key;
use crate::kvs::tasklease::TaskLeaseType;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Tl {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub task: u16,
}

impl_key!(Tl);

impl Categorise for Tl {
	fn categorise(&self) -> Category {
		Category::TaskLease
	}
}

impl Tl {
	/// Builds the lease key for a task.
	///
	/// Each task owns a distinct discriminant, and a lease outlives the process
	/// that took it, so a discriminant may be added but never reused or
	/// renumbered: two tasks sharing one would contend for a single lease.
	pub(crate) fn new(task: &TaskLeaseType) -> Self {
		let task = match task {
			TaskLeaseType::IndexCompaction => 1,
			TaskLeaseType::ReclaimTombstones => 2,
		};
		Self {
			__: b'/',
			_a: b'!',
			_b: b't',
			_c: b'l',
			task,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KeyEncode;

	#[test]
	fn key_changefeed_cleanup() {
		#[rustfmt::skip]
		let val = Tl::new(&TaskLeaseType::IndexCompaction);
		let enc = Tl::encode(&val).unwrap();
		assert_eq!(enc, b"/!tl\0\x01");
	}

	#[test]
	fn key_reclaim_tombstones() {
		let val = Tl::new(&TaskLeaseType::ReclaimTombstones);
		let enc = Tl::encode(&val).unwrap();
		assert_eq!(enc, b"/!tl\0\x02");
	}
}
