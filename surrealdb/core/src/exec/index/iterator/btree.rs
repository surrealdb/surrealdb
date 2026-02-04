//! B-tree index iterators for Idx and Uniq indexes.
//!
//! These iterators provide efficient record retrieval using B-tree index structures.
//! They support equality lookups, range scans, and union operations.

use anyhow::Result;

use crate::catalog::{DatabaseId, IndexDefinition, NamespaceId};
use crate::exec::index::access_path::RangeBound;
use crate::key::index::Index;
use crate::kvs::{KVKey, Key, Transaction};
use crate::val::{Array, RecordId, Value};

/// Batch size for index scans.
const INDEX_BATCH_SIZE: u32 = 1000;

/// Iterator for equality lookups on non-unique indexes.
///
/// Scans all records matching a specific key value.
pub struct IndexEqualIterator {
	/// Current scan position (begin key)
	beg: Vec<u8>,
	/// End key (exclusive)
	end: Vec<u8>,
	/// Whether iteration is complete
	done: bool,
}

impl IndexEqualIterator {
	/// Create a new equality iterator.
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		value: &Value,
	) -> Result<Self> {
		let array = Array::from(vec![value.clone()]);
		let beg = Index::prefix_ids_beg(ns, db, &ix.table_name, ix.index_id, &array)?;
		let end = Index::prefix_ids_end(ns, db, &ix.table_name, ix.index_id, &array)?;
		Ok(Self {
			beg,
			end,
			done: false,
		})
	}

	/// Fetch the next batch of record IDs.
	pub async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		if self.done {
			return Ok(Vec::new());
		}

		let res = tx.scan(self.beg.clone()..self.end.clone(), INDEX_BATCH_SIZE, None).await?;

		if res.is_empty() {
			self.done = true;
			return Ok(Vec::new());
		}

		// Update begin key for next batch
		if let Some((key, _)) = res.last() {
			self.beg = key.clone();
			self.beg.push(0x00);
		}

		// Decode record IDs from values
		let mut records = Vec::with_capacity(res.len());
		for (_, val) in res {
			let rid: RecordId = revision::from_slice(&val)?;
			records.push(rid);
		}

		Ok(records)
	}
}

/// Iterator for equality lookups on unique indexes.
///
/// Returns at most one record.
pub struct UniqueEqualIterator {
	/// The key to look up
	key: Option<Key>,
}

impl UniqueEqualIterator {
	/// Create a new unique equality iterator.
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		value: &Value,
	) -> Result<Self> {
		let array = Array::from(vec![value.clone()]);
		let key = Index::new(ns, db, &ix.table_name, ix.index_id, &array, None).encode_key()?;
		Ok(Self {
			key: Some(key),
		})
	}

	/// Fetch the record ID (if any).
	pub async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		let Some(key) = self.key.take() else {
			return Ok(Vec::new());
		};

		if let Some(val) = tx.get(&key, None).await? {
			let rid: RecordId = revision::from_slice(&val)?;
			Ok(vec![rid])
		} else {
			Ok(Vec::new())
		}
	}
}

/// Iterator for range scans on non-unique indexes.
pub struct IndexRangeIterator {
	/// Current scan range
	beg: Key,
	end: Key,
	/// Inclusivity flags
	beg_inclusive: bool,
	end_inclusive: bool,
	/// Whether we've checked the begin boundary
	beg_checked: bool,
	/// Whether iteration is complete
	done: bool,
}

impl IndexRangeIterator {
	/// Create a new range iterator.
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Option<&RangeBound>,
		to: Option<&RangeBound>,
	) -> Result<Self> {
		let (beg, beg_inclusive) = if let Some(from) = from {
			let array = Array::from(vec![from.value.clone()]);
			if from.inclusive {
				(Index::prefix_ids_beg(ns, db, &ix.table_name, ix.index_id, &array)?, true)
			} else {
				(Index::prefix_ids_end(ns, db, &ix.table_name, ix.index_id, &array)?, false)
			}
		} else {
			(Index::prefix_beg(ns, db, &ix.table_name, ix.index_id)?, true)
		};

		let (end, end_inclusive) = if let Some(to) = to {
			let array = Array::from(vec![to.value.clone()]);
			if to.inclusive {
				(Index::prefix_ids_end(ns, db, &ix.table_name, ix.index_id, &array)?, true)
			} else {
				(Index::prefix_ids_beg(ns, db, &ix.table_name, ix.index_id, &array)?, false)
			}
		} else {
			(Index::prefix_end(ns, db, &ix.table_name, ix.index_id)?, true)
		};

		Ok(Self {
			beg,
			end,
			beg_inclusive,
			end_inclusive,
			beg_checked: beg_inclusive, // If inclusive, no need to check
			done: false,
		})
	}

	/// Create a full-range iterator (all values in the index).
	pub fn full_range(ns: NamespaceId, db: DatabaseId, ix: &IndexDefinition) -> Result<Self> {
		Self::new(ns, db, ix, None, None)
	}

	/// Fetch the next batch of record IDs.
	pub async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		if self.done {
			return Ok(Vec::new());
		}

		let res = tx.scan(self.beg.clone()..self.end.clone(), INDEX_BATCH_SIZE, None).await?;

		if res.is_empty() {
			self.done = true;
			return Ok(Vec::new());
		}

		// Update begin key for next batch
		if let Some((key, _)) = res.last() {
			self.beg = key.clone();
			self.beg.push(0x00);
		}

		// Decode record IDs, filtering boundary keys if needed
		let mut records = Vec::with_capacity(res.len());
		for (key, val) in res {
			// Skip begin key if exclusive and not yet checked
			if !self.beg_checked && key == self.beg {
				self.beg_checked = true;
				continue;
			}

			let rid: RecordId = revision::from_slice(&val)?;
			records.push(rid);
		}

		Ok(records)
	}
}

/// Iterator for range scans on unique indexes.
pub struct UniqueRangeIterator {
	/// Current scan range
	beg: Key,
	end: Key,
	/// Inclusivity flags
	beg_inclusive: bool,
	end_inclusive: bool,
	/// Whether iteration is complete
	done: bool,
}

impl UniqueRangeIterator {
	/// Create a new unique range iterator.
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Option<&RangeBound>,
		to: Option<&RangeBound>,
	) -> Result<Self> {
		let (beg, beg_inclusive) = if let Some(from) = from {
			let array = Array::from(vec![from.value.clone()]);
			let key = Index::new(ns, db, &ix.table_name, ix.index_id, &array, None).encode_key()?;
			(key, from.inclusive)
		} else {
			(Index::prefix_beg(ns, db, &ix.table_name, ix.index_id)?, true)
		};

		let (end, end_inclusive) = if let Some(to) = to {
			let array = Array::from(vec![to.value.clone()]);
			let key = Index::new(ns, db, &ix.table_name, ix.index_id, &array, None).encode_key()?;
			(key, to.inclusive)
		} else {
			(Index::prefix_end(ns, db, &ix.table_name, ix.index_id)?, true)
		};

		Ok(Self {
			beg,
			end,
			beg_inclusive,
			end_inclusive,
			done: false,
		})
	}

	/// Fetch the next batch of record IDs.
	pub async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		if self.done {
			return Ok(Vec::new());
		}

		// For unique indexes, we need to handle boundaries carefully
		let limit = INDEX_BATCH_SIZE + 1; // Extra to check boundaries
		let res = tx.scan(self.beg.clone()..self.end.clone(), limit, None).await?;

		if res.is_empty() {
			self.done = true;
			// Check end key if inclusive
			if self.end_inclusive {
				if let Some(val) = tx.get(&self.end, None).await? {
					let rid: RecordId = revision::from_slice(&val)?;
					return Ok(vec![rid]);
				}
			}
			return Ok(Vec::new());
		}

		// Update begin key for next batch
		if let Some((key, _)) = res.last() {
			self.beg = key.clone();
		}

		// Decode record IDs
		let mut records = Vec::with_capacity(res.len());
		for (key, val) in res {
			// Skip begin if exclusive
			if !self.beg_inclusive && key == self.beg {
				continue;
			}
			let rid: RecordId = revision::from_slice(&val)?;
			records.push(rid);
		}

		Ok(records)
	}
}

/// Iterator for union of multiple equality lookups.
///
/// Used for IN clauses: `field IN [a, b, c]`
pub struct IndexUnionIterator {
	/// Namespace ID
	ns: NamespaceId,
	/// Database ID
	db: DatabaseId,
	/// Index definition
	ix: IndexDefinition,
	/// Values to look up
	values: Vec<Value>,
	/// Current position in values
	current_idx: usize,
	/// Current sub-iterator
	current_iter: Option<IndexEqualIterator>,
}

impl IndexUnionIterator {
	/// Create a new union iterator.
	pub fn new(ns: NamespaceId, db: DatabaseId, ix: IndexDefinition, values: Vec<Value>) -> Self {
		Self {
			ns,
			db,
			ix,
			values,
			current_idx: 0,
			current_iter: None,
		}
	}

	/// Fetch the next batch of record IDs.
	pub async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		loop {
			// Try current iterator
			if let Some(ref mut iter) = self.current_iter {
				let batch = iter.next_batch(tx).await?;
				if !batch.is_empty() {
					return Ok(batch);
				}
			}

			// Move to next value
			if self.current_idx >= self.values.len() {
				return Ok(Vec::new());
			}

			let value = &self.values[self.current_idx];
			self.current_idx += 1;
			self.current_iter = Some(IndexEqualIterator::new(self.ns, self.db, &self.ix, value)?);
		}
	}
}
