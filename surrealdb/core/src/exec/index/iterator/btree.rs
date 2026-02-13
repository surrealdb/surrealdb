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

		let res = tx.scan(self.beg.clone()..self.end.clone(), INDEX_BATCH_SIZE, 0, None).await?;

		if res.is_empty() {
			self.done = true;
			return Ok(Vec::new());
		}

		// Update begin key for next batch
		if let Some((key, _)) = res.last() {
			self.beg.clone_from(key);
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

		let end = if let Some(to) = to {
			let array = Array::from(vec![to.value.clone()]);
			if to.inclusive {
				Index::prefix_ids_end(ns, db, &ix.table_name, ix.index_id, &array)?
			} else {
				Index::prefix_ids_beg(ns, db, &ix.table_name, ix.index_id, &array)?
			}
		} else {
			Index::prefix_end(ns, db, &ix.table_name, ix.index_id)?
		};

		Ok(Self {
			beg,
			end,
			beg_checked: beg_inclusive, // If inclusive, no need to check
			done: false,
		})
	}

	/// Fetch the next batch of record IDs.
	pub async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		if self.done {
			return Ok(Vec::new());
		}

		// Save original begin key before the scan mutates it, so exclusive
		// boundary comparison works correctly on the first batch.
		let check_exclusive_beg = if self.beg_checked {
			None
		} else {
			Some(self.beg.clone())
		};

		let res = tx.scan(self.beg.clone()..self.end.clone(), INDEX_BATCH_SIZE, 0, None).await?;

		if res.is_empty() {
			self.done = true;
			return Ok(Vec::new());
		}

		// Update begin key for next batch
		if let Some((key, _)) = res.last() {
			self.beg.clone_from(key);
			self.beg.push(0x00);
		}

		// After the first batch, the exclusive boundary has been handled
		self.beg_checked = true;

		// Decode record IDs, filtering boundary keys if needed
		let mut records = Vec::with_capacity(res.len());
		for (key, val) in res {
			// Skip begin key if exclusive and this is the first batch
			if let Some(ref exclusive_beg) = check_exclusive_beg {
				if key == *exclusive_beg {
					continue;
				}
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
		let res = tx.scan(self.beg.clone()..self.end.clone(), limit, 0, None).await?;

		if res.is_empty() {
			self.done = true;
			// Check end key if inclusive
			if self.end_inclusive
				&& let Some(val) = tx.get(&self.end, None).await?
			{
				let rid: RecordId = revision::from_slice(&val)?;
				return Ok(vec![rid]);
			}
			return Ok(Vec::new());
		}

		// Store original beg key to check exclusive boundary
		let original_beg = self.beg.clone();

		// Update begin key for next batch - increment to move past scanned records
		if let Some((key, _)) = res.last() {
			self.beg.clone_from(key);
			self.beg.push(0x00);
		}

		// Decode record IDs
		let mut records = Vec::with_capacity(res.len());
		for (key, val) in res {
			// Skip boundary key if exclusive
			if !self.beg_inclusive && key == original_beg {
				continue;
			}
			let rid: RecordId = revision::from_slice(&val)?;
			records.push(rid);
		}

		// Mark that we've handled the begin boundary
		self.beg_inclusive = true;

		Ok(records)
	}
}
