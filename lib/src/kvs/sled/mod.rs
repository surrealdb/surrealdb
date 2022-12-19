#![cfg(feature = "kv-sled")]

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

#[cfg(test)]
mod tests {
	use crate::kvs::tx::Transaction;
	use crate::kvs::{Key, Val};
	use std::fs;
	use std::path::PathBuf;
	use std::sync::atomic::{AtomicU16, Ordering};

	/// This value is automatically incremented for each test
	/// so that each test has a dedicated id
	static TEST_ID: AtomicU16 = AtomicU16::new(1);

	pub fn next_test_id() -> usize {
		TEST_ID.fetch_add(1, Ordering::SeqCst) as usize
	}

	pub fn new_tmp_path(path: &str, delete_existing: bool) -> PathBuf {
		let mut path_buf = PathBuf::from("/tmp");
		if !path_buf.exists() {
			fs::create_dir(path_buf.as_path()).unwrap();
		}
		path_buf.push(path);
		if delete_existing && path_buf.exists() {
			if path_buf.is_dir() {
				fs::remove_dir_all(&path_buf).unwrap();
			} else if path_buf.is_file() {
				fs::remove_file(&path_buf).unwrap()
			}
		}
		path_buf
	}

	fn new_store_path() -> String {
		let store_path = format!("/tmp/sled.{}", next_test_id());
		new_tmp_path(&store_path, true);
		store_path
	}

	async fn get_transaction(store_path: &str) -> crate::Transaction {
		let datastore = crate::Datastore::new(&format!("sled:{}", store_path)).await.unwrap();
		datastore.transaction(true, false).await.unwrap()
	}

	#[tokio::test]
	async fn test_transaction_sled_put_exi_get() {
		let store_path = new_store_path();
		{
			let mut tx = get_transaction(&store_path).await;
			// The key should not exist
			assert_eq!(tx.exi("flip").await.unwrap(), false);
			assert_eq!(tx.get("flip").await.unwrap(), None);
			tx.put("flip", "flop").await.unwrap();
			// Check existence against memory
			assert_eq!(tx.exi("flip").await.unwrap(), true);
			// Read from memory
			assert_eq!(tx.get("flip").await.unwrap(), Some("flop".as_bytes().to_vec()));
			// Commit in storage
			tx.commit().await.unwrap();
		}
		{
			// New transaction with the committed data
			let mut tx = get_transaction(&store_path).await;
			// The key exists
			assert_eq!(tx.exi("flip").await.unwrap(), true);
			// And the value can be retrieved
			assert_eq!(tx.get("flip").await.unwrap(), Some("flop".as_bytes().to_vec()));
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_putc_err() {
		let store_path = new_store_path();
		{
			let mut tx = get_transaction(&store_path).await;
			tx.put("flip", "flop").await.unwrap();
			assert_eq!(
				tx.putc("flip", "flap", Some("nada")).await.err().unwrap().to_string(),
				"Value being checked was not correct"
			);
			// Checked the value did not change in memory
			assert_eq!(tx.get("flip").await.unwrap(), Some("flop".as_bytes().to_vec()));
			// Commit in storage
			tx.commit().await.unwrap();
		}
		{
			// New transaction with the committed data
			let mut tx = get_transaction(&store_path).await;
			// Check the value did not change on storage
			assert_eq!(tx.get("flip").await.unwrap(), Some("flop".as_bytes().to_vec()));
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_putc_ok() {
		let store_path = new_store_path();
		{
			let mut tx = get_transaction(&store_path).await;
			tx.put("flip", "flop").await.unwrap();
			tx.putc("flip", "flap", Some("flop")).await.unwrap();
			tx.commit().await.unwrap();
		}
		{
			// New transaction with the committed data
			let mut tx = get_transaction(&store_path).await;
			assert_eq!(tx.get("flip").await.unwrap(), Some("flap".as_bytes().to_vec()));
		}
	}

	fn check_scan_result(result: Vec<(Key, Val)>, expected: Vec<(&'static str, &'static str)>) {
		assert_eq!(result.len(), expected.len());
		let mut i = 0;
		for (key, val) in result {
			let (expected_key, expected_value) = expected.get(i).unwrap();
			assert_eq!(&String::from_utf8(key).unwrap(), *expected_key);
			assert_eq!(&String::from_utf8(val).unwrap(), *expected_value);
			i += 1;
		}
	}

	async fn scan_suite_checks(tx: &mut Transaction) {
		// I can retrieve the key/values with using scan with several ranges
		check_scan_result(
			tx.scan("k1".."k9", 100).await.unwrap(),
			vec![("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4"), ("k5", "v5")],
		);
		check_scan_result(tx.scan("k1".."k2", 100).await.unwrap(), vec![("k1", "v1")]);
		check_scan_result(
			tx.scan("k1".."k3", 100).await.unwrap(),
			vec![("k1", "v1"), ("k2", "v2")],
		);
		check_scan_result(tx.scan("k2".."k3", 100).await.unwrap(), vec![("k2", "v2")]);
		check_scan_result(tx.scan("k3".."k4", 100).await.unwrap(), vec![("k3", "v3")]);
		check_scan_result(tx.scan("k4".."k5", 100).await.unwrap(), vec![("k4", "v4")]);
		check_scan_result(
			tx.scan("k4".."k6", 100).await.unwrap(),
			vec![("k4", "v4"), ("k5", "v5")],
		);
		check_scan_result(tx.scan("k5".."k7", 100).await.unwrap(), vec![("k5", "v5")]);
		check_scan_result(tx.scan("k2".."k1", 100).await.unwrap(), vec![]);

		// I can retrieve the key/values using scan with several limits
		check_scan_result(tx.scan("k1".."k9", 0).await.unwrap(), vec![]);
		check_scan_result(tx.scan("k1".."k9", 1).await.unwrap(), vec![("k1", "v1")]);
		check_scan_result(tx.scan("k2".."k9", 1).await.unwrap(), vec![("k2", "v2")]);
		check_scan_result(tx.scan("k5".."k9", 1).await.unwrap(), vec![("k5", "v5")]);
		check_scan_result(tx.scan("k6".."k9", 1).await.unwrap(), vec![]);
		check_scan_result(tx.scan("k1".."k4", 2).await.unwrap(), vec![("k1", "v1"), ("k2", "v2")]);
		check_scan_result(tx.scan("k2".."k4", 2).await.unwrap(), vec![("k2", "v2"), ("k3", "v3")]);
		check_scan_result(tx.scan("k3".."k4", 2).await.unwrap(), vec![("k3", "v3")]);
	}

	#[tokio::test]
	async fn test_transaction_sled_scan_in_transaction() {
		let store_path = new_store_path();
		{
			// Given a set of key/values added in a transaction
			let mut tx = get_transaction(&store_path).await;
			tx.put("k3", "v3").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.put("k1", "v1").await.unwrap();
			tx.put("k5", "v5").await.unwrap();
			tx.put("k4", "v4").await.unwrap();

			// Then, I can successfully use the range method on in memory key/values
			scan_suite_checks(&mut tx).await;
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_scan_in_storage() {
		let store_path = new_store_path();
		{
			// Given three key/values added in the transaction...
			let mut tx = get_transaction(&store_path).await;
			tx.put("k3", "v3").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.put("k1", "v1").await.unwrap();
			tx.put("k5", "v5").await.unwrap();
			tx.put("k4", "v4").await.unwrap();
			// ... and stored
			tx.commit().await.unwrap();
		}
		{
			// Then, I can successfully use the range method on stored key/values
			let mut tx = get_transaction(&store_path).await;
			scan_suite_checks(&mut tx).await;
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_scan_mixed() {
		let store_path = new_store_path();
		{
			// Given three key/values added in the transaction and stored
			let mut tx = get_transaction(&store_path).await;
			tx.put("k3", "v3").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.put("k5", "v5").await.unwrap();
			tx.commit().await.unwrap();
		}
		{
			// then, given two key/values added in the transaction
			let mut tx = get_transaction(&store_path).await;
			tx.put("k1", "v1").await.unwrap();
			tx.put("k4", "v4").await.unwrap();

			// Then, I can successfully use the range method on mixed key/values
			scan_suite_checks(&mut tx).await;
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_scan_mixed_with_deletion() {
		let store_path = new_store_path();
		{
			// Given three key/values added in the transaction and stored
			let mut tx = get_transaction(&store_path).await;
			tx.put("k3", "v3").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.put("k5", "v5").await.unwrap();
			tx.put("k6", "v6").await.unwrap();
			tx.commit().await.unwrap();
		}
		{
			// then, given two key/values added in the transaction
			let mut tx = get_transaction(&store_path).await;
			tx.del("k6").await.unwrap();
			tx.put("k1", "v1").await.unwrap();
			tx.put("k4", "v4").await.unwrap();

			// Then, I can successfully use the range method on mixed key/values
			scan_suite_checks(&mut tx).await;
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_del_in_transaction() {
		let store_path = new_store_path();
		{
			let mut tx = get_transaction(&store_path).await;
			tx.put("k1", "v1").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.del("k1").await.unwrap();
			assert_eq!(tx.exi("k1").await.unwrap(), false);
			assert_eq!(tx.get("k1").await.unwrap(), None);
			tx.commit().await.unwrap();
		}
		{
			let mut tx = get_transaction(&store_path).await;
			assert_eq!(tx.exi("k1").await.unwrap(), false);
			assert_eq!(tx.get("k2").await.unwrap(), Some("v2".as_bytes().to_vec()));
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_del_in_storage() {
		let store_path = new_store_path();
		{
			let mut tx = get_transaction(&store_path).await;
			tx.put("k1", "v1").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.commit().await.unwrap();
		}
		{
			let mut tx = get_transaction(&store_path).await;
			tx.del("k1").await.unwrap();
			assert_eq!(tx.exi("k1").await.unwrap(), false);
			assert_eq!(tx.get("k1").await.unwrap(), None);
			tx.commit().await.unwrap();
		}
		{
			let mut tx = get_transaction(&store_path).await;
			assert_eq!(tx.exi("k1").await.unwrap(), false);
			assert_eq!(tx.get("k2").await.unwrap(), Some("v2".as_bytes().to_vec()));
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_cancel_put_and_del() {
		let store_path = new_store_path();
		// Given a store with two keys
		{
			let mut tx = get_transaction(&store_path).await;
			tx.put("k1", "v1").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.commit().await.unwrap();
		}
		{
			// When cancelling a transaction adding k3 and deleting k1
			let mut tx = get_transaction(&store_path).await;
			tx.put("k3", "v3").await.unwrap();
			tx.del("k1").await.unwrap();
			assert_eq!(tx.exi("k1").await.unwrap(), false);
			assert_eq!(tx.get("k3").await.unwrap(), Some("v3".as_bytes().to_vec()));

			tx.cancel().await.unwrap();
		}
		{
			// Then k3 has not been added, and k1 as not been deleted
			let mut tx = get_transaction(&store_path).await;
			assert_eq!(tx.exi("k3").await.unwrap(), false);
			assert_eq!(tx.get("k1").await.unwrap(), Some("v1".as_bytes().to_vec()));
			assert_eq!(tx.get("k2").await.unwrap(), Some("v2".as_bytes().to_vec()));
		}
	}
}
