use crate::err::Error;
use crate::kvs::{Key, Transactor, Val};
use std::collections::{HashMap, VecDeque};
use std::ops::Range;

pub(crate) type SavePointId = usize;
pub(crate) type SavePoint = HashMap<Key, SavedValue>;

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

pub(super) enum SavePrepare {
	AlreadyPresent(Key, SaveOperation),
	NewKey(Key, SavedValue),
}

impl Transactor {
	pub(crate) fn new_save_point(&mut self) -> SavePointId {
		let id = self.save_points.new_save_point();
		println!("NEW SAVE POINT {}", id);
		id
	}

	async fn rollback_save_point_unchecked(&mut self, id: SavePointId) -> Result<(), Error> {
		let s = self.save_points.pop(id)?;
		println!("ROLLBACK id: {id} - keys: {} keys", s.len());
		for (key, saved_value) in s {
			if let Some(val) = saved_value.saved_val {
				match saved_value.last_operation {
					SaveOperation::Set | SaveOperation::Put => {
						self.set(key, val, saved_value.saved_version).await?;
					}
					SaveOperation::Del => {
						self.put(key, val, saved_value.saved_version).await?;
					}
				}
			} else if !matches!(saved_value.last_operation, SaveOperation::Del) {
				self.del(key).await?;
			}
		}
		Ok(())
	}

	pub(crate) async fn rollback_save_point(&mut self, id: SavePointId) -> Result<(), Error> {
		self.rollback_save_point_unchecked(id)
			.await
			.map_err(|e| Error::Internal(format!("Rollback failure: {e}")))
	}

	pub(crate) fn release_save_point(&mut self, id: SavePointId) -> Result<(), Error> {
		println!("RELEASE id: {id}");
		self.save_points.pop(id)?;
		Ok(())
	}

	pub(super) async fn save_point_set(
		&mut self,
		key: &Key,
		version: Option<u64>,
	) -> Result<Option<SavePrepare>, Error> {
		self.save_point_prepare(key, version, SaveOperation::Set).await
	}

	pub(super) async fn save_point_put(
		&mut self,
		key: &Key,
		version: Option<u64>,
	) -> Result<Option<SavePrepare>, Error> {
		self.save_point_prepare(key, version, SaveOperation::Put).await
	}

	pub(super) async fn save_point_del(
		&mut self,
		key: &Key,
		version: Option<u64>,
	) -> Result<Option<SavePrepare>, Error> {
		self.save_point_prepare(key, version, SaveOperation::Del).await
	}

	async fn save_point_prepare(
		&mut self,
		key: &Key,
		version: Option<u64>,
		op: SaveOperation,
	) -> Result<Option<SavePrepare>, Error> {
		println!("PREPARE {op:?} {key:?}");
		match self.save_points.is_saved_key(key) {
			None => Ok(None),
			Some(true) => Ok(Some(SavePrepare::AlreadyPresent(key.clone(), op))),
			Some(false) => {
				let val = self.get(key.clone(), version).await?;
				Ok(Some(SavePrepare::NewKey(
					key.clone(),
					SavedValue {
						saved_val: val,
						saved_version: version,
						last_operation: op,
					},
				)))
			}
		}
	}

	pub(super) fn save_point_save(&mut self, prep: SavePrepare) {
		self.save_points.save_key(prep);
	}

	pub(super) async fn save_point_delr(
		&mut self,
		_key: Range<Key>,
	) -> Result<Option<SavePrepare>, Error> {
		todo!()
	}

	pub(super) async fn save_point_delp(
		&mut self,
		_key: &Key,
	) -> Result<Option<SavePrepare>, Error> {
		todo!()
	}
}

#[derive(Default)]
pub(crate) struct SavePoints {
	stack: VecDeque<SavePoint>,
	current: Option<SavePoint>,
}

impl SavePoints {
	pub(super) fn new_save_point(&mut self) -> SavePointId {
		if let Some(c) = self.current.take() {
			self.stack.push_back(c);
		}
		let id = self.stack.len();
		self.current = Some(SavePoint::default());
		id
	}

	pub(super) fn is_some(&self) -> bool {
		self.current.is_some()
	}

	pub(super) fn pop(&mut self, id: SavePointId) -> Result<SavePoint, Error> {
		if id != self.stack.len() {
			return Err(Error::Unreachable("Invalid SavePoint"));
		}
		if let Some(c) = self.current.take() {
			self.current = self.stack.pop_back();
			Ok(c)
		} else {
			Err(Error::Unreachable("No current SavePoint"))
		}
	}

	fn is_saved_key(&self, key: &Key) -> Option<bool> {
		self.current.as_ref().map(|current| current.contains_key(key))
	}

	fn save_key(&mut self, prep: SavePrepare) {
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
}
