//! Stores a task lease to ensure only one node is running the task at a time
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use crate::kvs::tasklease::TaskLeaseType;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Tl {
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
	pub(crate) fn new(task: &TaskLeaseType) -> Self {
		let task = match task {
			TaskLeaseType::ChangeFeedCleanup => 1,
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
	use crate::kvs::{KeyDecode, KeyEncode};

	#[test]
	fn key_changefeed_cleanup() {
		use super::*;
		#[rustfmt::skip]
		let val = Tl::new(&TaskLeaseType::ChangeFeedCleanup);
		let enc = Tl::encode(&val).unwrap();
		assert_eq!(enc, b"/!tl\0\x01");
		let dec = Tl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
