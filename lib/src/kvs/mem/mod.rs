#![cfg(feature = "kv-mem")]

use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use log::Level::Trace;
use std::ops::Range;

const LOG: &str = "surrealdb::kvs::mem";

pub struct Datastore {
	db: echodb::Db<Key, Val>,
}

pub struct Transaction {
	/// Is the transaction complete?
	ok: bool,
	/// Is the transaction read+write?
	rw: bool,
	/// The distributed datastore transaction
	tx: echodb::Tx<Key, Val>,
}

impl Datastore {
	/// Open a new database
	pub async fn new() -> Result<Datastore, Error> {
		Ok(Datastore {
			db: echodb::db::new(),
		})
	}
	/// Start a new transaction
	pub async fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		match self.db.begin(write).await {
			Ok(tx) => Ok(Transaction {
				ok: false,
				rw: write,
				tx,
			}),
			Err(e) => Err(Error::Tx(e.to_string())),
		}
	}
}

impl Transaction {
	/// Check if closed
	pub fn closed(&self) -> bool {
		self.ok
	}
	/// Cancel a transaction
	pub fn cancel(&mut self) -> Result<(), Error> {
		trace!(target: LOG, "cancel");
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Mark this transaction as done
		self.ok = true;
		// Cancel this transaction
		self.tx.cancel()?;
		// Continue
		Ok(())
	}
	/// Commit a transaction
	pub fn commit(&mut self) -> Result<(), Error> {
		trace!(target: LOG, "commit");
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
		self.tx.commit()?;
		// Continue
		Ok(())
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
		// Check the key
		let key = key.into();
		trace!(target: LOG, "exi {:?}", key);
		let res = self.tx.exi(key)?;
		// Return result
		Ok(res)
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
		let key = key.into();
		if log_enabled!(Trace) {
			trace!(target: LOG, "get {}", String::from_utf8_lossy(&key));
		}
		let res = self.tx.get(key)?;
		// Return result
		Ok(res)
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
		let key = key.into();
		let val = val.into();
		if log_enabled!(Trace) {
			trace!(target: LOG, "set {}=>{}", String::from_utf8_lossy(&key), val.len());
		}
		self.tx.set(key, val)?;
		// Return result
		Ok(())
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
		let val = val.into();
		if log_enabled!(Trace) {
			trace!(target: LOG, "put {}=>{:?}", String::from_utf8_lossy(&key), val.len());
		}
		self.tx.put(key, val)?;
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
		if log_enabled!(Trace) {
			trace!(target: LOG, "putc <{}> => {}", String::from_utf8_lossy(&key), val.len());
		}
		self.tx.putc(key, val, chk.map(Into::into))?;
		// Return result
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
		let key = key.into();
		if log_enabled!(Trace) {
			trace!(target: LOG, "del <{}>", String::from_utf8_lossy(&key));
		}
		self.tx.del(key)?;
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
		// Remove the key
		let key = key.into();
		if log_enabled!(Trace) {
			trace!(target: LOG, "delc {}", String::from_utf8_lossy(&key));
		}
		self.tx.delc(key, chk.map(Into::into))?;
		// Return result
		Ok(())
	}
	/// Retrieve a range of keys from the databases
	pub fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		let start = rng.start.into();
		let end = rng.end.into();
		// Convert the range to bytes
		if log_enabled!(Trace) {
			trace!(
				target: LOG,
				"scan {}-{}",
				String::from_utf8_lossy(&start),
				String::from_utf8_lossy(&end)
			);
		}
		let rng: Range<Key> = Range {
			start,
			end,
		};

		// Scan the keys
		let res = self.tx.scan(rng, limit)?;
		// Return result
		Ok(res)
	}
}
