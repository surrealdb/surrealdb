#![cfg(feature = "kv-tikv")]

use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use std::ops::Range;
use tikv::CheckLevel;
use tikv::TransactionOptions;

pub struct Datastore {
	db: tikv::TransactionClient,
}

pub struct Transaction {
	// Is the transaction complete?
	ok: bool,
	// Is the transaction read+write?
	rw: bool,
	// The distributed datastore transaction
	tx: tikv::Transaction,
}

impl Datastore {
	// Open a new database
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		match tikv::TransactionClient::new(vec![path], None).await {
			Ok(db) => Ok(Datastore {
				db,
			}),
			Err(e) => Err(Error::Ds(e.to_string())),
		}
	}
	// Start a new transaction
	pub async fn transaction(&self, write: bool, lock: bool) -> Result<Transaction, Error> {
		match lock {
			true => {
				// Set the behaviour when dropping an unfinished transaction
				let opt = TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn);
				// Create a new optimistic transaction
				match self.db.begin_with_options(opt).await {
					Ok(tx) => Ok(Transaction {
						ok: false,
						rw: write,
						tx,
					}),
					Err(e) => Err(Error::Tx(e.to_string())),
				}
			}
			false => {
				// Set the behaviour when dropping an unfinished transaction
				let opt = TransactionOptions::new_pessimistic().drop_check(CheckLevel::Warn);
				// Create a new pessimistic transaction
				match self.db.begin_with_options(opt).await {
					Ok(tx) => Ok(Transaction {
						ok: false,
						rw: write,
						tx,
					}),
					Err(e) => Err(Error::Tx(e.to_string())),
				}
			}
		}
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
		self.tx.rollback().await?;
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
		self.tx.commit().await?;
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
		let res = self.tx.key_exists(key.into()).await?;
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
		let res = self.tx.get(key.into()).await?;
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
		self.tx.put(key.into(), val.into()).await?;
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
		// Set the key
		self.tx.insert(key.into(), val.into()).await?;
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
		// Get the key
		let key = key.into();
		// Get the val
		let val = val.into();
		// Get the check
		let chk = chk.map(Into::into);
		// Delete the key
		match (self.tx.get(key.clone()).await?, chk) {
			(Some(v), Some(w)) if v == w => self.tx.put(key, val).await?,
			(None, None) => self.tx.put(key, val).await?,
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
		// Delete the key
		self.tx.delete(key.into()).await?;
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
		// Get the key
		let key = key.into();
		// Get the check
		let chk = chk.map(Into::into);
		// Delete the key
		match (self.tx.get(key.clone()).await?, chk) {
			(Some(v), Some(w)) if v == w => self.tx.delete(key).await?,
			(None, None) => self.tx.delete(key).await?,
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
		// Convert the range to bytes
		let rng: Range<Key> = Range {
			start: rng.start.into(),
			end: rng.end.into(),
		};
		// Scan the keys
		let res = self.tx.scan(rng, limit).await?;
		let res = res.map(|kv| (Key::from(kv.0), kv.1)).collect();
		// Return result
		Ok(res)
	}
}
