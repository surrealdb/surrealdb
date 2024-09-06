use crate::err::Error;
use crate::kvs::{Key, Transactor, Val};
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;

pub(crate) type SavePointId = usize;
pub(crate) type SavePoint = HashMap<Key, Option<(Arc<Val>, Option<u64>)>>;

impl Transactor {
	pub(crate) fn new_save_point(&mut self) -> SavePointId {
		self.save_points.new_save_point()
	}

	pub(crate) async fn rollback_save_point(&mut self, id: SavePointId) -> Result<(), Error> {
		let s = self.save_points.pop(id)?;
		for (key, val_ver) in s {
			if let Some((val, version)) = val_ver {
				self.put(key, val.as_ref().clone(), version).await?;
			} else {
				self.del(key).await?;
			}
		}
		Ok(())
	}

	pub(crate) fn release_save_point(&mut self, id: SavePointId) -> Result<(), Error> {
		self.save_points.pop(id)?;
		Ok(())
	}

	pub(super) async fn save_point_update_key(
		&mut self,
		key: &Key,
		version: Option<u64>,
	) -> Result<(), Error> {
		let val = self.get(key.clone(), version).await?;
		self.save_points.save_key(key, version, val);
		Ok(())
	}

	pub(super) async fn save_point_update_keyr(&mut self, _key: Range<Key>) -> Result<(), Error> {
		todo!()
	}

	pub(super) async fn save_point_update_keyp(&mut self, _key: &Key) -> Result<(), Error> {
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

	pub(super) fn save_key(&mut self, key: &Key, version: Option<u64>, val: Option<Val>) {
		let val_ver = val.map(|val| (val.into(), version));
		if let Some(current) = &mut self.current {
			if !current.contains_key(key) {
				current.insert(key.clone(), val_ver.clone());
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
