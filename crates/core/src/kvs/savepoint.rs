use std::collections::{HashMap, VecDeque};

use anyhow::Result;

use crate::kvs::{Key, Val};

type SavePoint = HashMap<Key, SavedValue>;

#[derive(Debug)]
/// Public to allow external transaction implementations (e.g. custom backends)
/// to construct and inspect savepoints when integrating with SurrealDB.
pub enum SaveOperation {
	Set,
	Put,
	Del,
}

pub struct SavedValue {
	pub(crate) saved_val: Option<Val>,
	pub(crate) saved_version: Option<u64>,
	pub(crate) last_operation: SaveOperation,
}

impl SavedValue {
	pub(crate) fn new(val: Option<Val>, version: Option<u64>, op: SaveOperation) -> Self {
		Self {
			saved_val: val,
			saved_version: version,
			last_operation: op,
		}
	}

	pub fn get_val(&self) -> Option<&Val> {
		self.saved_val.as_ref()
	}
}

pub enum SavePrepare {
	AlreadyPresent(Key, SaveOperation),
	NewKey(Key, SavedValue),
}

#[derive(Default)]
pub struct SavePoints {
	stack: VecDeque<SavePoint>,
	current: Option<SavePoint>,
}

impl SavePoints {
	/// Create a new save point
	pub fn new_save_point(&mut self) {
		if let Some(c) = self.current.take() {
			self.stack.push_back(c);
		}
		self.current = Some(SavePoint::default());
	}

	/// Check if there is a current save point
	pub fn is_some(&self) -> bool {
		self.current.is_some()
	}

	/// Remove and return the latest save point
	pub fn pop(&mut self) -> Result<SavePoint> {
		if let Some(c) = self.current.take() {
			self.current = self.stack.pop_back();
			Ok(c)
		} else {
			fail!("No current SavePoint")
		}
	}

	/// Check if a key is saved in the current save point
	pub fn is_saved_key(&self, key: &Key) -> Option<bool> {
		self.current.as_ref().map(|current| current.contains_key(key))
	}

	/// Save a key to the current save point
	pub fn save(&mut self, prep: SavePrepare) {
		if let Some(current) = &mut self.current {
			match prep {
				SavePrepare::AlreadyPresent(key, op) => {
					if let Some(sv) = current.get_mut(&key) {
						// We keep the last operation executed in the transaction so we can do the
						// appropriate rollback action (SET or PUT)
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
