use crate::{
	err::Error,
	kvs::{DatastoreFacade, DatastoreMetadata, Key, TransactionFacade, Val},
};
use async_trait_fn::async_trait;
use std::ops::Range;
use tikv::CheckLevel;
use tikv::TransactionOptions;

pub struct TikvDatastoreMetadata;

pub struct TikvDatastore {
	db: tikv::TransactionClient,
}

pub struct TikvTransaction {
	// Is the transaction complete?
	ok: bool,
	// Is the transaction read+write?
	rw: bool,
	// The distributed datastore transaction
	tx: tikv::Transaction,
}

#[async_trait]
impl DatastoreMetadata for TikvDatastoreMetadata {
	async fn new(&self, path: &str) -> Result<Box<dyn DatastoreFacade + Send + Sync>, Error> {
		match tikv::TransactionClient::new(vec![path]).await {
			Ok(db) => Ok(Box::new(TikvDatastore {
				db,
			})),
			Err(e) => Err(Error::Ds(e.to_string())),
		}
	}

	fn name(&self) -> &'static str {
		"TiKV"
	}

	fn scheme(&self) -> &'static [&'static str] {
		&["tikv"]
	}

	fn trim_connection_string(&self, url: &str) -> String {
		url.trim_start_matches("tikv://").trim_start_matches("tikv:").to_string()
	}
}

impl DatastoreFacade for TikvDatastore {
	/// Start a new transaction
	async fn transaction(
		&self,
		write: bool,
		lock: bool,
	) -> Result<Box<dyn TransactionFacade + Send + Sync>, Error> {
		// Set whether this should be an optimistic or pessimistic transaction
		let mut opt = if lock {
			TransactionOptions::new_pessimistic()
		} else {
			TransactionOptions::new_optimistic()
		};
		// Set the behaviour when dropping an unfinished transaction
		opt = opt.drop_check(CheckLevel::Warn);
		// Set this transaction as read only if possible
		if !write {
			opt = opt.read_only();
		}
		// Create a new distributed transaction
		match self.db.begin_with_options(opt).await {
			Ok(tx) => Ok(Box::new(TikvTransaction {
				ok: false,
				rw: write,
				tx,
			})),
			Err(e) => Err(Error::Tx(e.to_string())),
		}
	}
}

#[async_trait]
impl TransactionFacade for TikvTransaction {
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
		self.tx.rollback().await?;
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
		self.tx.commit().await?;
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
		let res = self.tx.key_exists(key.into()).await?;
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
		let res = self.tx.get(key.into()).await?;
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
		self.tx.put(key.into(), val.into()).await?;
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
		self.tx.insert(key.into(), val.into()).await?;
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
		// Delete the key
		self.tx.delete(key.into()).await?;
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
		let res = self.tx.scan(rng, limit).await?;
		let res = res.map(|kv| (Key::from(kv.0), kv.1)).collect();
		// Return result
		Ok(res)
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::tests::transaction::verify_transaction_isolation;
	use test_log::test;

	#[test(tokio::test(flavor = "multi_thread", worker_threads = 3))]
	async fn tikv_transaction() {
		verify_transaction_isolation("tikv://127.0.0.1:2379").await;
	}
}
