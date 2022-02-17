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
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		Ok(Datastore {
			db: tikv::TransactionClient::new(vec![path]).await?,
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
		if self.ok == true {
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
		if self.ok == true {
			return Err(Error::TxFinishedError);
		}
		// Check to see if transaction is writable
		if self.rw == false {
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
	pub async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxFinishedError);
		}
		// Check to see if transaction is writable
		if self.rw == false {
			return Err(Error::TxReadonlyError);
		}
		// Delete the key
		self.tx.delete(key.into()).await?;
		// Return result
		Ok(())
	}
	// Check if a key exists
	pub async fn exi<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxFinishedError);
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
		if self.ok == true {
			return Err(Error::TxFinishedError);
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
		if self.ok == true {
			return Err(Error::TxFinishedError);
		}
		// Check to see if transaction is writable
		if self.rw == false {
			return Err(Error::TxReadonlyError);
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
		if self.ok == true {
			return Err(Error::TxFinishedError);
		}
		// Check to see if transaction is writable
		if self.rw == false {
			return Err(Error::TxReadonlyError);
		}
		// Set the key
		self.tx.insert(key.into(), val.into()).await?;
		// Return result
		Ok(())
	}
	// Retrieve a range of keys from the databases
	pub async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok == true {
			return Err(Error::TxFinishedError);
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
