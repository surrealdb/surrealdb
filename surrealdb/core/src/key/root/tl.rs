//! Stores a task lease to ensure only one node is running the task at a time

use storekey::{BorrowDecode, Encode};

use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::kvs::tasklease::{TaskLease, TaskLeaseType};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Tl {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub task: u16,
}

impl_kv_key_storekey!(Tl => TaskLease);

impl Categorise for Tl {
	fn categorise(&self) -> Category {
		Category::TaskLease
	}
}

impl Tl {
	pub(crate) fn new(task: &TaskLeaseType) -> Self {
		let task = match task {
			TaskLeaseType::ChangeFeedCleanup => 1,
			TaskLeaseType::IndexCompaction => 2,
			TaskLeaseType::EventProcessing => 3,
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
	use crate::kvs::KVKey;

	#[test]
	fn key_changefeed_cleanup() {
		let val = Tl::new(&TaskLeaseType::ChangeFeedCleanup);
		let enc = Tl::encode_key(&val).unwrap();
		assert_eq!(enc, b"/!tl\0\x01");
	}
}
