#![cfg(feature = "kv-fdb")]

use futures::TryStreamExt;

use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use std::ops::Range;
use std::sync::Arc;
// https://rust-lang.github.io/wg-async/vision/submitted_stories/status_quo/alan_thinks_he_needs_async_locks.html
use tokio::sync::Mutex;

pub struct Datastore {
	db: foundationdb::Database,
}

pub struct Transaction {
	// Is the transaction complete?
	ok: bool,
	// Is the transaction read+write?
	rw: bool,
	lock: bool,
	// The distributed datastore transaction
	tx: Arc<Mutex<Option<foundationdb::Transaction>>>,
}

impl Datastore {
	// Open a new database
	//
	// path must be an empty string or a local file path to a FDB cluster file.
	// An empty string results in using the default cluster file placed
	// at a system-dependent location defined by FDB.
	// See https://apple.github.io/foundationdb/administration.html#default-cluster-file for more information on that.
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		match foundationdb::Database::from_path(path) {
			Ok(db) => Ok(Datastore {
				db,
			}),
			Err(e) => Err(Error::Ds(e.to_string())),
		}
	}
	// Start a new transaction
	pub async fn transaction(&self, write: bool, lock: bool) -> Result<Transaction, Error> {
		match self.db.create_trx() {
			Ok(tx) => Ok(Transaction {
				ok: false,
				rw: write,
				lock: lock,
				tx: Arc::new(Mutex::new(Some(tx))),
			}),
			Err(e) => Err(Error::Tx(e.to_string())),
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
		//
		// To overcome the limitation in the rust fdb client that
		// it's `cancel` and `commit` methods require you to move the
		// whole tx object to the method, we wrap it inside a Arc<Mutex<Option<_>>>
		// so that we can atomically `take` the tx out of the container and
		// replace it with the new `reset`ed tx.
		let tx = match self.tx.lock().await.take() {
			Some(tx) => {
				let tc =  tx.cancel();
				tc.reset()
			}
			_ => {
				return Err(Error::Ds("Unexpected error".to_string()))
			}
		};
		self.tx = Arc::new(Mutex::new(Some(tx)));
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
		//
		// To overcome the limitation in the rust fdb client that
		// it's `cancel` and `commit` methods require you to move the
		// whole tx object to the method, we wrap it inside a Arc<Mutex<Option<_>>>
		// so that we can atomically `take` the tx out of the container and
		// replace it with the new `reset`ed tx.
		let r = match self.tx.lock().await.take() {
			Some(tx) => {
				tx.commit().await
			}
			_ => {
				return Err(Error::Ds("Unexpected error".to_string()))
			}
		};
		match r {
			Ok(_r) => {},
			Err(e) => {
				return Err(Error::Tx(format!("Transaction commit error: {}", e).to_string()));
			},
		}
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
		let key:Vec<u8>=key.into();
		let key:&[u8]=&key[..];
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Assuming the `lock` argument passed to the datastore creation function
		// is meant for conducting a pessimistic lock on the underlying kv store to
		// make the transaction serializable, we use the inverse of it to enable the snapshot isolation
		// on the get request.
		// See https://apple.github.io/foundationdb/api-c.html#snapshot-reads for more information on how the snapshot get is supposed to work in FDB.
		tx.get(key, !self.lock).await
			.map(|v| v.is_some())
			.map_err(|e| Error::Tx(format!("Unable to get kv from FDB: {}", e)))
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
		let key: Vec<u8> = key.into();
		let key = &key[..];
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Assuming the `lock` argument passed to the datastore creation function
		// is meant for conducting a pessimistic lock on the underlying kv store to
		// make the transaction serializable, we use the inverse of it to enable the snapshot isolation
		// on the get request.
		// See https://apple.github.io/foundationdb/api-c.html#snapshot-reads for more information on how the snapshot get is supposed to work in FDB.
		let res = tx.get(key, !self.lock).await
			.map(|v| v.as_ref().map(|v| Val::from(v.to_vec())))
			.map_err(|e| Error::Tx(format!("Unable to get kv from FDB: {}", e)));
		res
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
		let key: Vec<u8> = key.into();
		let key = &key[..];
		let val:Vec<u8>=val.into();
		let val = &val[..];
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		tx.set(key, val);
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
		let key: Vec<u8> = key.into();
		if !self.exi(key.clone().as_slice()).await? {
			return Err(Error::TxKeyAlreadyExists);
		}
		// Set the key
		let key: &[u8] = &key[..];
		let val: Vec<u8> = val.into();
		let val : &[u8] = &val[..];
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		tx.set(key, val);
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
		let key:Vec<u8>=key.into();
		let key: &[u8] = key.as_slice();
		// Get the val
		let val:Vec<u8> = val.into();
		let val:&[u8] = val.as_slice();
		// Get the check
		let chk = chk.map(Into::into);
		// Delete the key
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Assuming the `lock` argument passed to the datastore creation function
		// is meant for conducting a pessimistic lock on the underlying kv store to
		// make the transaction serializable, we use the inverse of it to enable the snapshot isolation
		// on the get request.
		// See https://apple.github.io/foundationdb/api-c.html#snapshot-reads for more information on how the snapshot get is supposed to work in FDB.
		let res = tx.get(key, !self.lock).await;
		let res = res.map_err(|e| Error::Tx(format!("Unable to get kv from FDB: {}", e)));
		match (res, chk) {
			(Ok(Some(v)), Some(w)) if Val::from(v.as_ref()) == w => tx.set(key, val),
			(Ok(None), None) => tx.set(key, val),
			(Err(e), _) => return Err(e),
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
		let key:Vec<u8>=key.into();
		let key:&[u8]=key.as_slice();
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		tx.clear(key);
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
		let key: Vec<u8> = key.into();
		let key: &[u8] = key.as_slice();
		// Get the check
		let chk: Option<Val> = chk.map(Into::into);
		// Delete the key
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		let res = tx.get(key, false).await.map_err(|e| Error::Tx(format!("FDB tx failure: {}", e)));
		match (res, chk) {
			(Ok(Some(v)), Some(w)) if Val::from(v.as_ref()) == w => tx.clear(key),
			(Ok(None), None) => tx.clear(key),
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
		let begin:Vec<u8> = rng.start.into();
		let end: Vec<u8> = rng.end.into();
		let opt = foundationdb::RangeOption {
			limit: Some(limit.try_into().unwrap()),
			..foundationdb::RangeOption::from((begin.as_slice(), end.as_slice()))
		};
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Assuming the `lock` argument passed to the datastore creation function
		// is meant for conducting a pessimistic lock on the underlying kv store to
		// make the transaction serializable, we use the inverse of it to enable the snapshot isolation
		// on the get request.
		// See https://apple.github.io/foundationdb/api-c.html#snapshot-reads for more information on how the snapshot get is supposed to work in FDB.
		let mut stream = tx.get_ranges_keyvalues(opt, !self.lock);
		let mut res: Vec<(Key, Val)> = vec!();
		loop {
			let x = stream.try_next().await;
			match x {
				Ok(Some(v)) => {
					let x = (Key::from(v.key()), Val::from(v.value()));
					res.push(x)
				}
				Ok(None) => {
					break
				}
				Err(e) => {
					return Err(Error::Tx(format!("GetRanges failed: {}", e).to_string()))
				}
			}
		}
		return Ok(res)
	}
}
