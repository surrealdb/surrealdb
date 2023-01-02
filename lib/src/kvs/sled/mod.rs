#![cfg(feature = "kv-sled")]

mod bench;
mod testing;

use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use sled::{Db, Iter};
use std::collections::btree_map::Range as BTreeMapRange;
use std::collections::{BTreeMap, HashSet};
use std::ops::Range;

pub struct Datastore {
	db: Db,
}

pub struct Transaction {
	// Is the transaction complete?
	ok: bool,
	// Is the transaction read+write?
	rw: bool,
	// The DB this translation is related to
	db: Db,
	// Key/Value pair to set
	set: Option<BTreeMap<Key, Val>>,
	// Keys to delete
	del: Option<HashSet<Key>>,
}

impl Datastore {
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		let db = sled::open(path).unwrap();
		Ok(Datastore {
			db,
		})
	}

	/// Start a new transaction
	pub async fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		Ok(Transaction {
			ok: false,
			rw: write,
			db: self.db.clone(),
			set: None,
			del: None,
		})
	}
}

impl Transaction {
	/// Check if closed
	pub fn closed(&self) -> bool {
		self.ok
	}
	/// Cancel a transaction
	pub fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Mark this transaction as done
		self.ok = true;
		// Cancel this transaction
		self.set = None;
		self.del = None;
		// Continue
		Ok(())
	}
	/// Commit a transaction
	pub fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Mark this transaction as done
		self.ok = true;
		// Commit this transaction

		self.db.transaction(|db| {
			// Insert the set keys (if any)
			if let Some(set) = &self.set {
				for (key, val) in set {
					db.insert(key.as_slice(), val.as_slice())?;
				}
			}
			// Remove the deleted keys (if any)
			if let Some(del) = &self.del {
				for key in del {
					db.remove(key.as_slice())?;
				}
			}
			Ok(())
		})?;
		self.set = None;
		self.del = None;
		// Continue
		Ok(())
	}

	fn _key_updated_by_tx(&self, key: &Key) -> bool {
		if let Some(set) = &self.set {
			if set.contains_key(key) {
				return true;
			}
		}
		if let Some(del) = &self.del {
			if del.contains(key) {
				return true;
			}
		}
		false
	}

	/// Check if the key exists (without any pre-check)
	fn _exi(&self, key: &Key) -> Result<bool, Error> {
		if let Some(del) = &self.del {
			if del.contains(key) {
				return Ok(false);
			}
		}
		// We check in key set in the transaction
		if let Some(set) = &self.set {
			if set.contains_key(key) {
				return Ok(true);
			}
		}
		// Then we check in the db
		Ok(self.db.contains_key(key)?)
	}

	/// Check if a key exists
	pub fn exi<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check if the key exists
		self._exi(&key.into())
	}
	/// Fetch a key from the database (without any pre-check)
	fn _get(&mut self, key: &Key) -> Result<Option<Val>, Error> {
		if let Some(del) = &self.del {
			if del.contains(key) {
				// If the key has been deleted in the transaction, it is considere as not found
				return Ok(None);
			}
		}
		if let Some(set) = &self.set {
			if let Some(val) = set.get(key) {
				// The key/value has been set in the transaction
				return Ok(Some(val.clone()));
			}
		}
		if let Some(val) = self.db.get(key)? {
			// The key has been found in the database
			return Ok(Some(val.to_vec()));
		}
		Ok(None)
	}
	/// Fetch a key from the database
	pub fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Get the key
		self._get(&key.into())
	}

	fn _set(&mut self, key: Key, val: Val) -> Result<(), Error> {
		// Remove the key from previous deletion that occurred in this transaction  (if any)
		if let Some(del) = &mut self.del {
			del.remove(&key);
		}
		// Add the key/value to the transaction
		match &mut self.set {
			None => {
				// Create an hashmap if it didn't exist
				self.set = Some(BTreeMap::from([(key, val)]));
			}
			Some(set) => {
				// Update the hashmap
				set.insert(key, val);
			}
		}
		Ok(())
	}

	/// Insert or update a key in the database
	pub fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Set the key
		self._set(key.into(), val.into())
	}
	/// Insert a key if it doesn't exist in the database
	pub fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Set the key
		let key = key.into();
		if !self._exi(&key)? {
			self._set(key, val.into())?;
		}
		// Return result
		Ok(())
	}
	/// Insert a key if it doesn't exist in the database
	pub fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Set the key

		let key = key.into();
		let val = val.into();
		let chk = chk.map(Into::into);
		// Set the key if valid
		match (self._get(&key)?, chk) {
			(Some(v), Some(w)) if v == w => self._set(key, val)?,
			(None, None) => self._set(key, val)?,
			_ => return Err(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}
	/// Delete a key (without any pre-check)
	fn _del(&mut self, key: Key) -> Result<(), Error> {
		// Remove any previously set with the same key
		if let Some(set) = &mut self.set {
			set.remove(&key);
		}
		if let Some(del) = &mut self.del {
			del.insert(key);
		} else {
			self.del = Some(HashSet::from([key]));
		}
		Ok(())
	}
	/// Delete a key
	pub fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Remove the key
		self._del(key.into())?;
		// Return result
		Ok(())
	}
	/// Delete a key
	pub fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}

		// Get the arguments
		let key = key.into();
		let chk = chk.map(Into::into);
		// Delete the key if valid
		match (self._get(&key)?, chk) {
			(Some(v), Some(w)) if v == w => self._del(key)?,
			(None, None) => self._del(key)?,
			_ => return Err(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Retrieve a range of keys from the databases
	pub fn scan<K>(&self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Convert the range to bytes
		let rng: Range<Key> = Range {
			start: rng.start.into(),
			end: rng.end.into(),
		};

		// Scan the keys
		let mut res = Vec::new();
		let mut iterator = ParallelIterator::new(&self, rng)?;
		while let Some(key_val) = iterator.try_next()? {
			if res.len() == limit as usize {
				break;
			}
			res.push(key_val);
		}
		// Return result
		Ok(res)
	}
}

/// This iterator is a specialized iterator.
/// It iterates over a pair of keys/value iterators:
/// 1. Key/values extracted from the transaction (In memory).
/// 2. Key/values extracted from the store (Sled).
/// If a key is present in both iterators,
/// the key/value extracted from the transaction is returned,
/// the one extracted from the store is ignored.
struct ParallelIterator<'a> {
	db_iter: SledIterator<'a>,
	tx_iter: TxIterator<'a>,
}

impl<'a> ParallelIterator<'a> {
	fn new(tx: &'a Transaction, rng: Range<Key>) -> Result<ParallelIterator, Error> {
		Ok(Self {
			db_iter: SledIterator::new(tx, rng.clone())?,
			tx_iter: TxIterator::new(tx, rng),
		})
	}

	fn try_next(&mut self) -> Result<Option<(Key, Val)>, Error> {
		if let Some(db_next_key) = &self.db_iter.next_key() {
			if let Some(tx_next_key) = &self.tx_iter.next_key() {
				if tx_next_key.le(db_next_key) {
					return Ok(self.tx_iter.next());
				}
				return self.db_iter.try_next();
			}
			return self.db_iter.try_next();
		}
		Ok(self.tx_iter.next())
	}
}

struct TxIterator<'a> {
	iter: Option<BTreeMapRange<'a, Key, Val>>,
	next: Option<(Key, Val)>,
}

impl<'a> TxIterator<'a> {
	fn new(tx: &'a Transaction, rng: Range<Key>) -> Self {
		// Extract the key/values meeting the range in the transaction
		// and the initial key/value
		if let Some(set) = &tx.set {
			// Treemap.range panics if start is greater than end
			if rng.start.le(&rng.end) {
				let mut range = set.range(rng);
				let next = range.next().map(|(key, val)| (key.clone(), val.clone()));
				return Self {
					iter: Some(range),
					next,
				};
			}
		}
		Self {
			iter: None,
			next: None,
		}
	}

	fn next_key(&self) -> &Option<(Key, Val)> {
		&self.next
	}

	fn next(&mut self) -> Option<(Key, Val)> {
		if let Some(iter) = &mut self.iter {
			let kv = self.next.take();
			if kv.is_none() {
				return None;
			}
			self.next = iter.next().map(|(key, val)| (key.clone(), val.clone()));
			return kv;
		}
		None
	}
}
struct SledIterator<'a> {
	tx: &'a Transaction,
	iter: Iter,
	next: Option<(Key, Val)>,
}

impl<'a> SledIterator<'a> {
	fn new(tx: &'a Transaction, rng: Range<Key>) -> Result<Self, Error> {
		// Extract key/values meeting the range in the store
		let mut iter = Self {
			tx,
			iter: tx.db.range(rng),
			next: None,
		};
		iter._advance_db()?;
		Ok(iter)
	}

	fn next_key(&self) -> &Option<(Key, Val)> {
		&self.next
	}

	/// advance the iterator to prepare the next iteration
	fn _advance_db(&mut self) -> Result<(), Error> {
		// We use a loop here, because we want to ignore a key
		// if it exists in the transaction
		while let Some(kv) = self.iter.next() {
			let (key_vec, value_vec) = kv?;
			let key = key_vec.to_vec();
			// Check if the key is not updated in the transaction...
			if !self.tx._key_updated_by_tx(&key) {
				// ... if not we can use it for the next candidate value
				self.next = Some((key, value_vec.to_vec()));
				break;
			}
		}
		Ok(())
	}

	fn try_next(&mut self) -> Result<Option<(Key, Val)>, Error> {
		let kv = self.next.take();
		if kv.is_none() {
			return Ok(None);
		}
		self._advance_db()?;
		Ok(kv)
	}
}
