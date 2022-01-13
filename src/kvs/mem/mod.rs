use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use std::ops::Range;

pub struct Datastore {
	db: echodb::Db<Key, Val>,
}

pub struct Transaction<'a> {
	// Is the transaction complete?
	ok: bool,
	// Is the transaction read+write?
	rw: bool,
	// The distributed datastore transaction
	tx: echodb::Tx<'a, Key, Val>,
}

impl Datastore {
	// Open a new database
	pub fn new() -> Result<Datastore, Error> {
		Ok(Datastore {
			db: echodb::db::new(),
		})
	}
	// Start a new transaction
	pub fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		match self.db.begin(write) {
			Ok(tx) => Ok(Transaction {
				ok: false,
				rw: write,
				tx,
			}),
			Err(_) => Err(Error::TxError),
		}
	}
}

impl<'a> Transaction<'a> {
	// Check if closed
	pub fn closed(&self) -> bool {
		self.ok
	}
	// Cancel a transaction
	pub fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Mark this transaction as done
		self.ok = true;
		// Cancel this transaction
		self.tx.cancel()?;
		// Continue
		Ok(())
	}
	// Commit a transaction
	pub fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Check to see if transaction is writable
		if self.rw {
			return Err(Error::TxReadonlyError);
		}
		// Mark this transaction as done
		self.ok = true;
		// Cancel this transaction
		self.tx.commit()?;
		// Continue
		Ok(())
	}
	// Delete a key
	pub fn del(&mut self, key: Key) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Check to see if transaction is writable
		if self.rw {
			return Err(Error::TxReadonlyError);
		}
		// Remove the key
		let res = self.tx.del(key)?;
		// Return result
		Ok(res)
	}
	// Check if a key exists
	pub fn exi(&mut self, key: Key) -> Result<bool, Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Check the key
		let res = self.tx.exi(key)?;
		// Return result
		Ok(res)
	}
	// Fetch a key from the database
	pub fn get(&mut self, key: Key) -> Result<Option<Val>, Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Get the key
		let res = self.tx.get(key)?;
		// Return result
		Ok(res)
	}
	// Insert or update a key in the database
	pub fn set(&mut self, key: Key, val: Val) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Check to see if transaction is writable
		if self.rw {
			return Err(Error::TxReadonlyError);
		}
		// Set the key
		self.tx.set(key, val)?;
		// Return result
		Ok(())
	}
	// Insert a key if it doesn't exist in the database
	pub fn put(&mut self, key: Key, val: Val) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Check to see if transaction is writable
		if self.rw {
			return Err(Error::TxReadonlyError);
		}
		// Set the key
		self.tx.put(key, val)?;
		// Return result
		Ok(())
	}
	// Retrieve a range of keys from the databases
	pub fn scan(&mut self, rng: Range<Key>, limit: u32) -> Result<Vec<(Key, Val)>, Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Scan the keys
		let res = self.tx.scan(rng, limit)?;
		// Return result
		Ok(res)
	}
}
