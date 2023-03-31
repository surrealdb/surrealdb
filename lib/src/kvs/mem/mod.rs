use crate::{
	err::Error,
	kvs::{DatastoreFacade, DatastoreMetadata, Key, TransactionFacade, Val},
};
use async_trait_fn::async_trait;
use std::ops::Range;

pub struct MemoryDatastoreMetadata;

pub struct MemoryDatastore {
	db: echodb::Db<Key, Val>,
}

pub struct MemoryTransaction {
	/// Is the transaction complete?
	ok: bool,
	/// Is the transaction read+write?
	rw: bool,
	/// The distributed datastore transaction
	tx: echodb::Tx<Key, Val>,
}

#[async_trait]
impl DatastoreMetadata for MemoryDatastoreMetadata {
	/// Open a new database
	async fn new(&self, _: &str) -> Result<Box<dyn DatastoreFacade + Send + Sync>, Error> {
		Ok(Box::new(MemoryDatastore {
			db: echodb::db::new(),
		}))
	}

	fn name(&self) -> &'static str {
		"In-Memory"
	}

	fn scheme(&self) -> &'static [&'static str] {
		&["mem"]
	}

	fn connection_string_match_prefix(&self, url: &str) -> bool {
		url == "memory"
	}

	fn trim_connection_string(&self, url: &str) -> String {
		url.to_string()
	}
}

#[async_trait]
impl DatastoreFacade for MemoryDatastore {
	/// Start a new transaction
	async fn transaction(
		&self,
		write: bool,
		_: bool,
	) -> Result<Box<dyn TransactionFacade + Send + Sync>, Error> {
		match self.db.begin(write).await {
			Ok(tx) => Ok(Box::new(MemoryTransaction {
				ok: false,
				rw: write,
				tx,
			})),
			Err(e) => Err(Error::Tx(e.to_string())),
		}
	}
}

#[async_trait]
impl TransactionFacade for MemoryTransaction {
	/// Check if closed
	fn closed(&self) -> bool {
		self.ok
	}
	/// Cancel a transaction
	async fn cancel(&mut self) -> Result<(), Error> {
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
	async fn commit(&mut self) -> Result<(), Error> {
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
	async fn exi(&mut self, key: Key) -> Result<bool, Error> {
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
	async fn get(&mut self, key: Key) -> Result<Option<Val>, Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Get the key
		let res = self.tx.get(key.into())?;
		// Return result
		Ok(res)
	}
	/// Insert or update a key in the database
	async fn set(&mut self, key: Key, val: Val) -> Result<(), Error> {
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
	async fn put(&mut self, key: Key, val: Val) -> Result<(), Error> {
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
	async fn putc(&mut self, key: Key, val: Val, chk: Option<Val>) -> Result<(), Error> {
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
	async fn del(&mut self, key: Key) -> Result<(), Error> {
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
	async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<(), Error> {
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
	async fn scan(&mut self, rng: Range<Key>, limit: u32) -> Result<Vec<(Key, Val)>, Error> {
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
