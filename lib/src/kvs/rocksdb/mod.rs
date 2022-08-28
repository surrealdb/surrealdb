#![cfg(feature = "kv-rocksdb")]

use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use futures::lock::Mutex;
use rocksdb::Direction;
use rocksdb::IteratorMode;
use rocksdb::OptimisticTransactionDB;
use rocksdb::ReadOptions;
use std::ops::Range;
use std::sync::Arc;

pub struct Datastore {
	db: rocksdb::OptimisticTransactionDB,
}

pub struct Transaction {
	// Is the transaction complete?
	ok: bool,
	// Is the transaction read+write?
	rw: bool,
	// The distributed datastore transaction
	tx: Arc<Mutex<Option<rocksdb::Transaction<'static, OptimisticTransactionDB>>>>,
}

impl Datastore {
	// Open a new database
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		Ok(Datastore {
			db: OptimisticTransactionDB::open_default(path)?,
		})
	}
	// Start a new transaction
	pub async fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		// Create a new transaction
		let tx = self.db.transaction();
		// The database reference must always outlive
		// the transaction. If it doesn't then this
		// is undefined behaviour. This unsafe block
		// ensures that the transaction reference is
		// static, but will cause a crash if the
		// datastore is dropped prematurely.
		let tx = unsafe {
			std::mem::transmute::<
				rocksdb::Transaction<'_, OptimisticTransactionDB>,
				rocksdb::Transaction<'static, OptimisticTransactionDB>,
			>(tx)
		};
		// Return the transaction
		Ok(Transaction {
			ok: false,
			rw: write,
			tx: Arc::new(Mutex::new(Some(tx))),
		})
	}
}

impl Transaction {
	// Check if closed
	pub fn closed(&self) -> bool {
		self.ok
	}
	// Cancel a transaction
	pub async fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Mark this transaction as done
		self.ok = true;
		// Cancel this transaction
		match self.tx.lock().await.take() {
			Some(tx) => tx.rollback()?,
			None => unreachable!(),
		};
		// Continue
		Ok(())
	}
	// Commit a transaction
	pub async fn commit(&mut self) -> Result<(), Error> {
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
		// Cancel this transaction
		match self.tx.lock().await.take() {
			Some(tx) => tx.commit()?,
			None => unreachable!(),
		};
		// Continue
		Ok(())
	}
	// Check if a key exists
	pub async fn exi<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check the key
		let res = self.tx.lock().await.as_ref().unwrap().get(key.into())?.is_some();
		// Return result
		Ok(res)
	}
	// Fetch a key from the database
	pub async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Get the key
		let res = self.tx.lock().await.as_ref().unwrap().get(key.into())?;
		// Return result
		Ok(res)
	}
	// Insert or update a key in the database
	pub async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
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
		self.tx.lock().await.as_ref().unwrap().put(key.into(), val.into())?;
		// Return result
		Ok(())
	}
	// Insert a key if it doesn't exist in the database
	pub async fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
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
		// Get the transaction
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Get the arguments
		let key = key.into();
		let val = val.into();
		// Set the key if empty
		match tx.get(&key)? {
			None => tx.put(key, val)?,
			_ => return Err(Error::TxKeyAlreadyExists),
		};
		// Return result
		Ok(())
	}
	// Insert a key if it doesn't exist in the database
	pub async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
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
		// Get the transaction
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Get the arguments
		let key = key.into();
		let val = val.into();
		let chk = chk.map(Into::into);
		// Set the key if valid
		match (tx.get(&key)?, chk) {
			(Some(v), Some(w)) if v == w => tx.put(key, val)?,
			(None, None) => tx.put(key, val)?,
			_ => return Err(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}
	// Delete a key
	pub async fn del<K>(&mut self, key: K) -> Result<(), Error>
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
		self.tx.lock().await.as_ref().unwrap().delete(key.into())?;
		// Return result
		Ok(())
	}
	// Delete a key
	pub async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
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
		// Get the transaction
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Get the arguments
		let key = key.into();
		let chk = chk.map(Into::into);
		// Delete the key if valid
		match (tx.get(&key)?, chk) {
			(Some(v), Some(w)) if v == w => tx.delete(key)?,
			(None, None) => tx.delete(key)?,
			_ => return Err(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}
	// Retrieve a range of keys from the databases
	pub async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Get the transaction
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Convert the range to bytes
		let rng: Range<Key> = Range {
			start: rng.start.into(),
			end: rng.end.into(),
		};
		// Create result set
		let mut res = vec![];
		// Iterate forwards
		let dir = Direction::Forward;
		// Set the start key
		let cnf = IteratorMode::From(&rng.start, dir);
		// Set the maximum key
		let mut opt = ReadOptions::default();
		opt.set_iterate_range(..rng.end);
		// Create the iterator
		let ite = tx.iterator_opt(cnf, opt);
		// Scan the keys in the iterator
		for item in ite.take(limit as usize) {
			let (k, v) = item?;
			res.push((k.into_vec(), v.into_vec()));
		}
		// Return result
		Ok(res)
	}
}
