//! Stores a task lease to ensure only one node is running the task at a time

use serde::{Deserialize, Serialize};

use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::kvs::tasklease::{TaskLease, TaskLeaseType};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Tl {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub task: u16,
}

impl KVKey for Tl {
	type ValueType = TaskLease;
}

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

	#[test]
	fn key_changefeed_cleanup() {
		#[rustfmt::skip]
		let val = Tl::new(&TaskLeaseType::ChangeFeedCleanup);
		let enc = Tl::encode_key(&val).unwrap();
		assert_eq!(enc, b"/!tl\0\x01");
	}
}
