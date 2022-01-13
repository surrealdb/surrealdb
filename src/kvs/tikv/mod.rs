use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use std::ops::Range;

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
	pub fn new(path: &str) -> Result<Datastore, Error> {
		let db = tikv::TransactionClient::new(vec![path]);
		let db = futures::executor::block_on(db)?;
		Ok(Datastore {
			db,
		})
	}
	// Start a new transaction
	pub async fn transaction(&self, write: bool, lock: bool) -> Result<Transaction, Error> {
		match lock {
			true => match self.db.begin_optimistic().await {
				Ok(tx) => Ok(Transaction {
					ok: false,
					rw: write,
					tx,
				}),
				Err(_) => Err(Error::TxError),
			},
			false => match self.db.begin_pessimistic().await {
				Ok(tx) => Ok(Transaction {
					ok: false,
					rw: write,
					tx,
				}),
				Err(_) => Err(Error::TxError),
			},
		}
	}
}

impl Transaction {
	// Check if closed
	pub async fn closed(&self) -> bool {
		self.ok
	}
	// Cancel a transaction
	pub async fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
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
			return Err(Error::TxFinishedError);
		}
		// Check to see if transaction is writable
		if self.rw {
			return Err(Error::TxReadonlyError);
		}
		// Mark this transaction as done
		self.ok = true;
		// Cancel this transaction
		self.tx.commit().await?;
		// Continue
		Ok(())
	}
	// Delete a key
	pub async fn del(&mut self, key: Key) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Check to see if transaction is writable
		if self.rw {
			return Err(Error::TxReadonlyError);
		}
		// Delete the key
		self.tx.delete(key).await?;
		// Return result
		Ok(())
	}
	// Check if a key exists
	pub async fn exi(&mut self, key: Key) -> Result<bool, Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Check the key
		let res = self.tx.key_exists(key).await?;
		// Return result
		Ok(res)
	}
	// Fetch a key from the database
	pub async fn get(&mut self, key: Key) -> Result<Option<Val>, Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Get the key
		let res = self.tx.get(key).await?;
		// Return result
		Ok(res)
	}
	// Insert or update a key in the database
	pub async fn set(&mut self, key: Key, val: Val) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Check to see if transaction is writable
		if self.rw {
			return Err(Error::TxReadonlyError);
		}
		// Set the key
		self.tx.put(key, val).await?;
		// Return result
		Ok(())
	}
	// Insert a key if it doesn't exist in the database
	pub async fn put(&mut self, key: Key, val: Val) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Check to see if transaction is writable
		if self.rw {
			return Err(Error::TxReadonlyError);
		}
		// Set the key
		self.tx.insert(key, val).await?;
		// Return result
		Ok(())
	}
	// Retrieve a range of keys from the databases
	pub async fn scan(&mut self, rng: Range<Key>, limit: u32) -> Result<Vec<(Key, Val)>, Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinishedError);
		}
		// Scan the keys
		let res = self.tx.scan(rng, limit).await?;
		let res = res.map(|kv| (Key::from(kv.0), kv.1)).collect();
		// Return result
		Ok(res)
	}
}
