use crate::{
	err::Error,
	kvs::{DatastoreFacade, DatastoreMetadata, Key, TransactionFacade, Val},
};
use async_trait_fn::async_trait;
use futures::lock::Mutex;
use rocksdb::{OptimisticTransactionDB, OptimisticTransactionOptions, ReadOptions, WriteOptions};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

pub struct RocksDbDatastoreMetadata;

#[derive(Clone)]
pub struct RocksDbDatastore {
	db: Pin<Arc<OptimisticTransactionDB>>,
}

pub struct RocksDbTransaction {
	// Is the transaction complete?
	ok: bool,
	// Is the transaction read+write?
	rw: bool,
	// The distributed datastore transaction
	tx: Arc<Mutex<Option<rocksdb::Transaction<'static, OptimisticTransactionDB>>>>,
	// The read options containing the Snapshot
	ro: ReadOptions,
	// the above, supposedly 'static, transaction actually points here, so keep the memory alive
	// note that this is dropped last, as it is declared last
	_db: Pin<Arc<OptimisticTransactionDB>>,
}

#[async_trait]
impl DatastoreMetadata for RocksDbDatastoreMetadata {
	/// Open a new database
	async fn new(&self, path: &str) -> Result<Box<dyn DatastoreFacade + Send + Sync>, Error> {
		Ok(Box::new(RocksDbDatastore {
			db: Arc::pin(OptimisticTransactionDB::open_default(path)?),
		}))
	}

	fn name(&self) -> &'static str {
		"RocksDB"
	}

	fn scheme(&self) -> &'static [&'static str] {
		&["rocksdb", "file"]
	}

	fn trim_connection_string(&self, url: &str) -> String {
		(if url.starts_with("rocksdb:") {
			url.trim_start_matches("rocksdb://").trim_start_matches("rocksdb:")
		} else if url.starts_with("file:") {
			url.trim_start_matches("file://").trim_start_matches("file:")
		} else {
			unreachable!()
		})
		.to_string()
	}
}

#[async_trait]
impl DatastoreFacade for RocksDbDatastore {
	/// Start a new transaction
	async fn transaction(
		&self,
		write: bool,
		_: bool,
	) -> Result<Box<dyn TransactionFacade + Send + Sync>, Error> {
		// Activate the snapshot options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Create a new transaction
		let tx = self.db.transaction_opt(&WriteOptions::default(), &to);
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
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&tx.snapshot());
		// Return the transaction
		Ok(Box::new(RocksDbTransaction {
			ok: false,
			rw: write,
			tx: Arc::new(Mutex::new(Some(tx))),
			ro,
			_db: self.db.clone(),
		}))
	}
}

#[async_trait]
impl TransactionFacade for RocksDbTransaction {
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
		match self.tx.lock().await.take() {
			Some(tx) => tx.rollback()?,
			None => unreachable!(),
		};
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
		match self.tx.lock().await.take() {
			Some(tx) => tx.commit()?,
			None => unreachable!(),
		};
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
		let res = self.tx.lock().await.as_ref().unwrap().get_opt(key, &self.ro)?.is_some();
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
		let res = self.tx.lock().await.as_ref().unwrap().get_opt(key, &self.ro)?;
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
		self.tx.lock().await.as_ref().unwrap().put(key, val)?;
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
		// Get the transaction
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Get the arguments
		let key = key;
		let val = val;
		// Set the key if empty
		match tx.get_opt(&key, &self.ro)? {
			None => tx.put(key, val)?,
			_ => return Err(Error::TxKeyAlreadyExists),
		};
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
		// Get the transaction
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Set the key if valid
		match (tx.get_opt(&key, &self.ro)?, chk) {
			(Some(v), Some(w)) if v == w => tx.put(key, val)?,
			(None, None) => tx.put(key, val)?,
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
		// Remove the key
		self.tx.lock().await.as_ref().unwrap().delete(key)?;
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
		// Get the transaction
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Get the arguments
		// Delete the key if valid
		match (tx.get_opt(&key, &self.ro)?, chk) {
			(Some(v), Some(w)) if v == w => tx.delete(key)?,
			(None, None) => tx.delete(key)?,
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
		// Get the transaction
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Create result set
		let mut res = vec![];
		// Set the key range
		let beg = rng.start.as_slice();
		let end = rng.end.as_slice();
		// Set the ReadOptions with the snapshot
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&tx.snapshot());
		// Create the iterator
		let mut iter = tx.raw_iterator_opt(ro);
		// Seek to the start key
		iter.seek(&rng.start);
		// Scan the keys in the iterator
		while iter.valid() {
			// Check the scan limit
			if res.len() < limit as usize {
				// Get the key and value
				let (k, v) = (iter.key(), iter.value());
				// Check the key and value
				if let (Some(k), Some(v)) = (k, v) {
					if k >= beg && k < end {
						res.push((k.to_vec(), v.to_vec()));
						iter.next();
						continue;
					}
				}
			}
			// Exit
			break;
		}
		// Return result
		Ok(res)
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::tests::transaction::verify_transaction_isolation;
	use temp_dir::TempDir;

	// https://github.com/surrealdb/surrealdb/issues/76
	#[tokio::test]
	async fn soundness() {
		let mut transaction = get_transaction().await;
		transaction.put("uh", "oh").await.unwrap();

		async fn get_transaction() -> crate::kvs::Transaction {
			let datastore = crate::kvs::Datastore::new("rocksdb:/tmp/rocks.db").await.unwrap();
			datastore.transaction(true, false).await.unwrap()
		}
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
	async fn rocksdb_transaction() {
		let p = TempDir::new().unwrap().path().to_string_lossy().to_string();
		verify_transaction_isolation(&format!("file:{}", p)).await;
	}
}
