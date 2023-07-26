use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::vector::Vector;
use crate::idx::{IndexKeyBase, StoreType};
use crate::kvs::{Key, Transaction, Val};
use crate::sql::index::VectorType;
use lru::LruCache;
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(super) enum PointsStore {
	/// caches every read nodes, and keeps track of updated and created nodes
	Write(VectorType, PointsWriteCache),
	/// Uses an LRU cache to keep in memory the last node read
	Read(VectorType, PointsReadCache),
	/// Read the nodes from the KV store without any cache
	Traversal(VectorType, PointKeyProvider),
}

impl PointsStore {
	pub fn new(
		keys: PointKeyProvider,
		store_type: StoreType,
		read_size: usize,
		vt: VectorType,
	) -> Arc<Mutex<Self>> {
		Arc::new(Mutex::new(match store_type {
			StoreType::Write => Self::Write(vt, PointsWriteCache::new(keys)),
			StoreType::Read => Self::Read(vt, PointsReadCache::new(keys, read_size)),
			StoreType::Traversal => Self::Traversal(vt, keys),
		}))
	}

	pub(super) async fn _get(
		&mut self,
		tx: &mut Transaction,
		doc_id: DocId,
	) -> Result<Option<StoredPoint>, Error> {
		match self {
			Self::Write(t, w) => w._get(tx, doc_id, t).await,
			Self::Read(t, r) => r._get(tx, doc_id, t).await,
			Self::Traversal(t, keys) => keys._load(tx, doc_id, t).await,
		}
	}

	pub(super) fn _set(&mut self, val: StoredPoint, updated: bool) -> Result<(), Error> {
		match self {
			Self::Write(_, w) => w._set(val, updated),
			Self::Read(_, r) => {
				if updated {
					Err(Error::Unreachable)
				} else {
					r._set(val);
					Ok(())
				}
			}
			Self::Traversal(_, _) => {
				if updated {
					Err(Error::Unreachable)
				} else {
					Ok(())
				}
			}
		}
	}

	pub(super) fn put(&mut self, id: DocId, point: Vector) -> Result<StoredPoint, Error> {
		match self {
			Self::Write(_, w) => Ok(w.put(id, point)),
			_ => Err(Error::Unreachable),
		}
	}

	pub(super) fn remove(&mut self, id: DocId, key: Option<Key>) -> Result<(), Error> {
		match self {
			Self::Write(_, w) => w.remove(id, key),
			_ => Err(Error::Unreachable),
		}
	}

	pub(in crate::idx) async fn finish(&mut self, tx: &mut Transaction) -> Result<bool, Error> {
		if let Self::Write(_, w) = self {
			w.finish(tx).await
		} else {
			Err(Error::Unreachable)
		}
	}
}

pub(super) struct PointsWriteCache {
	keys: PointKeyProvider,
	values: HashMap<DocId, StoredPoint>,
	updated: HashSet<DocId>,
	none: RoaringTreemap,
	removed: HashMap<DocId, Key>,
	#[cfg(debug_assertions)]
	out: HashSet<DocId>,
}

impl PointsWriteCache {
	fn new(keys: PointKeyProvider) -> Self {
		Self {
			keys,
			values: HashMap::new(),
			updated: HashSet::new(),
			removed: HashMap::new(),
			none: RoaringTreemap::new(),
			#[cfg(debug_assertions)]
			out: HashSet::new(),
		}
	}

	async fn _get(
		&mut self,
		tx: &mut Transaction,
		id: DocId,
		vt: &VectorType,
	) -> Result<Option<StoredPoint>, Error> {
		#[cfg(debug_assertions)]
		self.out.insert(id);
		if let Some(n) = self.values.remove(&id) {
			return Ok(Some(n));
		}
		if self.none.contains(id) {
			return Ok(None);
		}
		let res = self.keys._load(tx, id, vt).await?;
		if res.is_none() {
			// Keep track of non existing key/values
			self.none.insert(id);
		}
		Ok(res)
	}

	fn _set(&mut self, val: StoredPoint, updated: bool) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		self.out.remove(&val._id);
		if updated {
			self.updated.insert(val._id);
		}
		if self.removed.contains_key(&val._id) {
			return Err(Error::Unreachable);
		}
		self.values.insert(val._id, val);
		Ok(())
	}

	fn put(&mut self, id: DocId, point: Vector) -> StoredPoint {
		#[cfg(debug_assertions)]
		self.out.insert(id);
		StoredPoint {
			_id: id,
			key: self.keys.get_key(id),
			point,
		}
	}

	fn remove(&mut self, id: DocId, key: Option<Key>) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		{
			if self.values.contains_key(&id) {
				return Err(Error::Unreachable);
			}
			self.out.remove(&id);
		}
		self.updated.remove(&id);
		if !self.none.contains(id) {
			let key = key.unwrap_or(self.keys.get_key(id));
			self.removed.insert(id, key);
		}
		Ok(())
	}

	async fn finish(&mut self, tx: &mut Transaction) -> Result<bool, Error> {
		let update = !self.updated.is_empty() || !self.removed.is_empty();
		#[cfg(debug_assertions)]
		{
			if !self.out.is_empty() {
				return Err(Error::Unreachable);
			}
		}
		for id in &self.updated {
			if let Some(val) = self.values.remove(id) {
				val.write(tx).await?;
			} else {
				return Err(Error::Unreachable);
			}
		}
		self.updated.clear();
		let doc_ids: Vec<DocId> = self.removed.keys().copied().collect();
		for doc_id in doc_ids {
			if let Some(key) = self.removed.remove(&doc_id) {
				tx.del(key).await?;
			}
		}
		Ok(update)
	}
}

pub(super) struct PointsReadCache {
	_keys: PointKeyProvider,
	_values: LruCache<DocId, StoredPoint>,
	_none: RoaringTreemap,
}

impl PointsReadCache {
	fn new(keys: PointKeyProvider, size: usize) -> Self {
		Self {
			_keys: keys,
			_values: LruCache::new(NonZeroUsize::new(size).unwrap()),
			_none: RoaringTreemap::new(),
		}
	}

	async fn _get(
		&mut self,
		tx: &mut Transaction,
		id: DocId,
		vt: &VectorType,
	) -> Result<Option<StoredPoint>, Error> {
		if let Some(n) = self._values.pop(&id) {
			return Ok(Some(n));
		}
		if self._none.contains(id) {
			return Ok(None);
		}
		let res = self._keys._load(tx, id, vt).await?;
		if res.is_none() {
			// Keep track of non existing key/value
			self._none.insert(id);
		}
		Ok(res)
	}

	fn _set(&mut self, val: StoredPoint) {
		self._values.put(val._id, val);
	}
}

#[derive(Clone)]
pub enum PointKeyProvider {
	Point(IndexKeyBase),
	_Debug,
}

impl PointKeyProvider {
	pub(in crate::idx) fn get_key(&self, id: DocId) -> Key {
		match self {
			Self::Point(ikb) => ikb.new_vp_key(id),
			Self::_Debug => id.to_be_bytes().to_vec(),
		}
	}

	async fn _load(
		&self,
		tx: &mut Transaction,
		id: DocId,
		vt: &VectorType,
	) -> Result<Option<StoredPoint>, Error> {
		let key = self.get_key(id);
		StoredPoint::_load(tx, id, key, vt).await
	}
}

pub(super) struct StoredPoint {
	pub(super) _id: DocId,
	pub(super) point: Vector,
	pub(super) key: Key,
}

impl StoredPoint {
	async fn _load(
		tx: &mut Transaction,
		id: DocId,
		key: Key,
		vt: &VectorType,
	) -> Result<Option<Self>, Error> {
		if let Some(val) = tx.get(key.clone()).await? {
			let point = Vector::_try_from(&val, vt)?;
			Ok(Some(Self {
				_id: id,
				point,
				key,
			}))
		} else {
			Ok(None)
		}
	}

	async fn write(&self, tx: &mut Transaction) -> Result<(), Error> {
		let val: Val = (&self.point).try_into()?;
		tx.set(self.key.clone(), val).await
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::vector::store::{PointKeyProvider, PointsStore};
	use crate::idx::vector::Vector;
	use crate::idx::StoreType;
	use crate::sql::index::VectorType;
	use test_log::test;

	#[test(tokio::test)]
	// This check node splitting. CLRS: Figure 18.7, page 498.
	async fn test_points_store() {
		let s = PointsStore::new(PointKeyProvider::_Debug, StoreType::Write, 20, VectorType::I64);
		s.lock().await.put(1, Vector::I64(vec![1, 2, 3, 4])).unwrap();
	}
}
