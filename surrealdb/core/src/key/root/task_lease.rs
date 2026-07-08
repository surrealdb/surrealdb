//! Stores a task lease to ensure only one node is running the task at a time

use crate::catalog::TaskLease as TaskLeaseValue;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, key};
use crate::kvs::tasklease::TaskLeaseType;

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct TaskLease {
		b'/',
		b'!',
		b't',
		b'l',
		pub task_id: TaskLeaseType,
	}
}

impl_kv_key_storekey!(TaskLease => TaskLeaseValue);

impl Categorise for TaskLease {
	fn categorise(&self) -> Category {
		Category::TaskLease
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;

	#[test]
	fn key_changefeed_cleanup() {
		let val = TaskLease {
			task_id: TaskLeaseType::ChangeFeedCleanup,
		};
		let enc = TaskLease::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/!tl\0\x01");
	}
}
