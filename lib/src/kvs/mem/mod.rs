#![cfg(feature = "kv-mem")]

use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::{try_to_u64_be, u64_to_versionstamp, Versionstamp};
use std::ops::Range;

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
		let res = self.tx.exi(key.into())?;
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
		let res = self.tx.get(key.into())?;
		// Return result
		Ok(res)
	}
	/// Obtain a new change timestamp for a key
	/// which is replaced with the current timestamp when the transaction is committed.
	/// NOTE: This should be called when composing the change feed entries for this transaction,
	/// which should be done immediately before the transaction commit.
	/// That is to keep other transactions commit delay(pessimistic) or conflict(optimistic) as less as possible.
	#[allow(unused)]
	pub fn get_timestamp<K>(&mut self, key: K) -> Result<Versionstamp, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Write the timestamp to the "last-write-timestamp" key
		// to ensure that no other transactions can commit with older timestamps.
		let k: Key = key.into();
		let prev = self.tx.get(k.clone())?;
		let ver = match prev {
			Some(prev) => {
				let slice = prev.as_slice();
				let res: Result<[u8; 10], Error> = match slice.try_into() {
					Ok(ba) => Ok(ba),
					Err(e) => Err(Error::Ds(e.to_string())),
				};
				let array = res?;
				let prev = try_to_u64_be(array)?;
				prev + 1
			}
			None => 1,
		};

		let verbytes = u64_to_versionstamp(ver);

		self.tx.set(k, verbytes.to_vec())?;
		// Return the uint64 representation of the timestamp as the result
		Ok(verbytes)
	}
	/// Obtain a new key that is suffixed with the change timestamp
	pub async fn get_versionstamped_key<K>(
		&mut self,
		ts_key: K,
		prefix: K,
		suffix: K,
	) -> Result<Vec<u8>, Error>
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

		let ts_key: Key = ts_key.into();
		let prefix: Key = prefix.into();
		let suffix: Key = suffix.into();

		let ts = self.get_timestamp(ts_key.clone())?;
		let mut k: Vec<u8> = prefix.clone();
		k.append(&mut ts.to_vec());
		k.append(&mut suffix.clone());

		trace!("get_versionstamped_key; {ts_key:?} {prefix:?} {ts:?} {suffix:?} {k:?}",);

		Ok(k)
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
		self.tx.set(key.into(), val.into())?;
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
		self.tx.put(key.into(), val.into())?;
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
		self.tx.putc(key.into(), val.into(), chk.map(Into::into))?;
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
		self.tx.del(key.into())?;
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
		self.tx.delc(key.into(), chk.map(Into::into))?;
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
		// Convert the range to bytes
		let rng: Range<Key> = Range {
			start: rng.start.into(),
			end: rng.end.into(),
		};
		// Scan the keys
		let res = self.tx.scan(rng, limit)?;
		// Return result
		Ok(res)
	}
}
