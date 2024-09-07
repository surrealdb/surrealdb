use crate::err::Error;
use crate::kvs::{Key, Transactor, Val};
use std::collections::{HashMap, VecDeque};
use std::ops::Range;

pub(crate) type SavePointId = usize;
pub(crate) type SavePoint = HashMap<Key, SavedValue>;

enum SaveOperation {
	Set,
	Put,
	Del,
}

pub(super) struct SavedValue {
	saved_val: Option<Val>,
	saved_version: Option<u64>,
	last_operation: SaveOperation,
}

impl Transactor {
	pub(crate) fn new_save_point(&mut self) -> SavePointId {
		self.save_points.new_save_point()
	}

	async fn rollback_save_point_unchecked(&mut self, id: SavePointId) -> Result<(), Error> {
		let s = self.save_points.pop(id)?;
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
			} else {
				if matches!(saved_value.last_operation, SaveOperation::Del) {
					self.del(key).await?;
				}
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
		self.save_points.pop(id)?;
		Ok(())
	}

	pub(super) async fn save_point_set(
		&mut self,
		key: &Key,
		version: Option<u64>,
	) -> Result<(), Error> {
		self.save_point_operation(key, version, SaveOperation::Set).await
	}

	pub(super) async fn save_point_put(
		&mut self,
		key: &Key,
		version: Option<u64>,
	) -> Result<(), Error> {
		self.save_point_operation(key, version, SaveOperation::Put).await
	}

	pub(super) async fn save_point_del(
		&mut self,
		key: &Key,
		version: Option<u64>,
	) -> Result<(), Error> {
		self.save_point_operation(key, version, SaveOperation::Del).await
	}

	async fn save_point_operation(
		&mut self,
		key: &Key,
		version: Option<u64>,
		op: SaveOperation,
	) -> Result<(), Error> {
		let val = self.get(key.clone(), version).await?;
		self.save_points.save_key(key, version, val, op);
		Ok(())
	}

	pub(super) async fn save_point_delr(&mut self, _key: Range<Key>) -> Result<(), Error> {
		todo!()
	}

	pub(super) async fn save_point_delp(&mut self, _key: &Key) -> Result<(), Error> {
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

	fn save_key(&mut self, key: &Key, version: Option<u64>, val: Option<Val>, ope: SaveOperation) {
		if let Some(current) = &mut self.current {
			// Check if we already have the initial value
			if let Some(sv) = current.get_mut(key) {
				// We keep the last operation executed in the transaction so we can do the appropriate rollback action (SET or PUT)
				sv.last_operation = ope;
			} else {
				current.insert(
					key.clone(),
					SavedValue {
						saved_val: val,
						saved_version: version,
						last_operation: ope,
					},
				);
			}
		}
	}

	// pub(super) fn rollback_batch(
	// 	&mut self,
	// 	id: SavePointId,
	// 	tr: &mut Transactor,
	// ) -> Result<(), Error> {
	// 	if let Some(map) = &mut self.map {
	// 		if let Some(sp) = map.remove(&id) {
	// 			for (key, val_ver) in sp {
	// 				if let Some((val, version)) = val_ver {
	// 					tr.put(key, val.as_ref().clone(), version).await?;
	// 				} else {
	// 					tr.del(key).await?;
	// 				}
	// 			}
	// 		}
	// 	}
	// 	Ok(())
	// }
}
