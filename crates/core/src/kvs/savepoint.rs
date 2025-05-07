#![cfg_attr(
	not(any(feature = "kv-fdb", feature = "kv-tikv")),
	expect(dead_code, reason = "This is only used in FoundationDB and TiKV")
)]

use crate::err::Error;
use crate::kvs::{Key, Val};
use std::collections::{HashMap, VecDeque};

type SavePoint = HashMap<Key, SavedValue>;

#[derive(Debug)]
pub(super) enum SaveOperation {
	Set,
	Put,
	Del,
}

pub(super) struct SavedValue {
	pub(super) saved_val: Option<Val>,
	pub(super) saved_version: Option<u64>,
	pub(super) last_operation: SaveOperation,
}

impl SavedValue {
	pub(super) fn new(val: Option<Val>, version: Option<u64>, op: SaveOperation) -> Self {
		Self {
			saved_val: val,
			saved_version: version,
			last_operation: op,
		}
	}

	pub(super) fn get_val(&self) -> Option<&Val> {
		self.saved_val.as_ref()
	}
}

pub(super) enum SavePrepare {
	AlreadyPresent(Key, SaveOperation),
	NewKey(Key, SavedValue),
}

#[derive(Default)]
pub(crate) struct SavePoints {
	stack: VecDeque<SavePoint>,
	current: Option<SavePoint>,
}

impl SavePoints {
	pub(super) fn new_save_point(&mut self) {
		if let Some(c) = self.current.take() {
			self.stack.push_back(c);
		}
		self.current = Some(SavePoint::default());
	}

	pub(super) fn is_some(&self) -> bool {
		self.current.is_some()
	}

	pub(super) fn pop(&mut self) -> Result<SavePoint, Error> {
		if let Some(c) = self.current.take() {
			self.current = self.stack.pop_back();
			Ok(c)
		} else {
			Err(fail!("No current SavePoint"))
		}
	}

	pub(super) fn is_saved_key(&self, key: &Key) -> Option<bool> {
		self.current.as_ref().map(|current| current.contains_key(key))
	}

	pub(super) fn save(&mut self, prep: SavePrepare) {
		if let Some(current) = &mut self.current {
			match prep {
				SavePrepare::AlreadyPresent(key, op) => {
					if let Some(sv) = current.get_mut(&key) {
						// We keep the last operation executed in the transaction so we can do the appropriate rollback action (SET or PUT)
						sv.last_operation = op;
					}
				}
				SavePrepare::NewKey(key, sv) => {
					current.insert(key, sv);
				}
			}
		}
	}
}
