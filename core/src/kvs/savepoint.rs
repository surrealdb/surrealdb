use crate::err::Error;
use crate::kvs::api::Transaction;
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
	saved_val: Option<Val>,
	saved_version: Option<u64>,
	last_operation: SaveOperation,
}

impl SavedValue {
	pub(super) fn new(val: Option<Val>, version: Option<u64>, op: SaveOperation) -> Self {
		Self {
			saved_val: val,
			saved_version: version,
			last_operation: op,
		}
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
		println!("NEW SAVE POINT {}", self.stack.len());
	}

	pub(super) fn is_some(&self) -> bool {
		self.current.is_some()
	}

	pub(super) fn pop(&mut self) -> Result<SavePoint, Error> {
		if let Some(c) = self.current.take() {
			self.current = self.stack.pop_back();
			println!("POP {}", self.stack.len());
			Ok(c)
		} else {
			Err(Error::Unreachable("No current SavePoint"))
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
			println!("SAVED KEYS: {}", current.len());
		} else {
			println!("SAVED KEYS NONE");
		}
	}

	pub(super) async fn rollback<T>(sp: SavePoint, tx: &mut T) -> Result<(), Error>
	where
		T: Transaction,
	{
		println!("ROLLBACK - keys: {} keys", sp.len());
		for (key, saved_value) in sp {
			match saved_value.last_operation {
				SaveOperation::Set | SaveOperation::Put => {
					if let Some(initial_value) = saved_value.saved_val {
						// If the last operation was a SET or PUT
						// then we just have set back the key to its initial value
						tx.set(key, initial_value, saved_value.saved_version).await?;
					} else {
						// If the last operation on this key was not a DEL operation,
						// then we have to delete the key
						tx.del(key).await?;
					}
				}
				SaveOperation::Del => {
					if let Some(initial_value) = saved_value.saved_val {
						// If the last operation was a DEL,
						// then we have to put back the initial value
						tx.put(key, initial_value, saved_value.saved_version).await?;
					}
				}
			}
		}
		Ok(())
	}
}
