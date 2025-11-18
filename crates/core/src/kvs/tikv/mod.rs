#![cfg(feature = "kv-tikv")]

mod cnf;
mod savepoint;

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use savepoint::{Operation, Savepoint};
use tikv::{CheckLevel, Config, TimestampExt, TransactionClient, TransactionOptions};
use tokio::sync::RwLock;

use super::err::{Error, Result};
use crate::key::debug::Sprintable;
use crate::kvs::api::Transactable;
use crate::kvs::{Key, Timestamp, Val};

const TARGET: &str = "surrealdb::core::kvs::tikv";

pub struct Datastore {
	db: Pin<Arc<TransactionClient>>,
}

pub struct Transaction {
	// Is the transaction complete?
	done: AtomicBool,
	// Is the transaction writeable?
	write: bool,
	/// The underlying datastore transaction
	inner: RwLock<TransactionInner>,
	// The above, supposedly 'static transaction
	// actually points here, so we need to ensure
	// the memory is kept alive. This pointer must
	// be declared last, so that it is dropped last.
	db: Pin<Arc<TransactionClient>>,
}

pub struct Timecode(tikv::Timestamp);

impl Timestamp for Timecode {
	/// Convert the timestamp to a version
	fn to_versionstamp(&self) -> u128 {
		self.0.version() as u128
	}
	/// Create a timestamp from a version
	fn from_versionstamp(version: u128) -> Result<Self> {
		Ok(Timecode(tikv::Timestamp::from_version(version as u64)))
	}
	/// Convert the timestamp to a datetime
	fn to_datetime(&self) -> DateTime<Utc> {
		DateTime::from_timestamp_nanos(self.0.physical)
	}
	/// Create a timestamp from a datetime
	fn from_datetime(datetime: DateTime<Utc>) -> Result<Self> {
		Ok(Timecode(tikv::Timestamp {
			physical: datetime.timestamp_millis() as i64,
			..Default::default()
		}))
	}
	/// Convert the timestamp to a byte array
	fn to_ts_bytes(&self) -> Vec<u8> {
		self.0.version().to_be_bytes().to_vec()
	}
	/// Create a timestamp from a byte array
	fn from_ts_bytes(bytes: &[u8]) -> Result<Self> {
		match bytes.try_into() {
			Ok(v) => Ok(Timecode(tikv::Timestamp::from_version(u64::from_be_bytes(v)))),
			Err(_) => Err(Error::TimestampInvalid("timestamp should be 8 bytes".to_string())),
		}
	}
}

struct TransactionInner {
	/// The underlying datastore transaction
	tx: tikv::Transaction,
	/// Stack of savepoints for nested rollback support
	savepoints: Vec<Savepoint>,
	/// Current undo operations since the last savepoint
	operations: Vec<Operation>,
}

impl Datastore {
	/// Open a new database
	pub(crate) async fn new(path: &str) -> Result<Datastore> {
		// Configure the client and keyspace
		let config = match *cnf::TIKV_API_VERSION {
			2 => match *cnf::TIKV_KEYSPACE {
				Some(ref keyspace) => {
					info!(target: TARGET, "Connecting to keyspace with cluster API V2: {keyspace}");
					Config::default().with_keyspace(keyspace)
				}
				None => {
					info!(target: TARGET, "Connecting to default keyspace with cluster API V2");
					Config::default().with_default_keyspace()
				}
			},
			1 => {
				info!(target: TARGET, "Connecting with cluster API V1");
				Config::default()
			}
			_ => return Err(Error::Datastore("Invalid TiKV API version".into())),
		};
		// Set the default request timeout
		let config = config.with_timeout(Duration::from_secs(*cnf::TIKV_REQUEST_TIMEOUT));
		// Set the max decoding message size
		let config =
			config.with_grpc_max_decoding_message_size(*cnf::TIKV_GRPC_MAX_DECODING_MESSAGE_SIZE);
		// Create the client with the config
		let client = TransactionClient::new_with_config(vec![path], config);
		// Check for errors with the client
		match client.await {
			Ok(db) => Ok(Datastore {
				db: Arc::pin(db),
			}),
			Err(e) => Err(Error::Datastore(e.to_string())),
		}
	}

	/// Shutdown the database
	pub(crate) async fn shutdown(&self) -> Result<()> {
		// Nothing to do here
		Ok(())
	}

	/// Start a new transaction
	pub(crate) async fn transaction(
		&self,
		write: bool,
		lock: bool,
	) -> Result<Box<dyn Transactable>> {
		// Set whether this should be an optimistic or pessimistic transaction
		let mut opt = if lock {
			TransactionOptions::new_pessimistic()
		} else {
			TransactionOptions::new_optimistic()
		};
		// Use async commit to determine transaction state earlier
		opt = match *cnf::TIKV_ASYNC_COMMIT {
			true => opt.use_async_commit(),
			_ => opt,
		};
		// Try to use one-phase commit if writing to only one region
		opt = match *cnf::TIKV_ONE_PHASE_COMMIT {
			true => opt.try_one_pc(),
			_ => opt,
		};
		// Set the behaviour when dropping an unfinished transaction
		opt = opt.drop_check(CheckLevel::Warn);
		// Set this transaction as read only if possible
		if !write {
			opt = opt.read_only();
		}
		// Create a new transaction
		match self.db.begin_with_options(opt).await {
			Ok(txn) => Ok(Box::new(Transaction {
				done: AtomicBool::new(false),
				write,
				inner: RwLock::new(TransactionInner {
					tx: txn,
					savepoints: Vec::new(),
					operations: Vec::new(),
				}),
				db: self.db.clone(),
			})),
			Err(e) => Err(Error::from(e)),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl Transactable for Transaction {
	fn kind(&self) -> &'static str {
		"tikv"
	}

	/// Check if closed
	fn closed(&self) -> bool {
		self.done.load(Ordering::Relaxed)
	}

	/// Check if writeable
	fn writeable(&self) -> bool {
		self.write
	}

	/// Cancel a transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn cancel(&self) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Mark this transaction as done
		self.done.store(true, Ordering::Release);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Cancel this transaction
		inner.tx.rollback().await?;
		// Continue
		Ok(())
	}

	/// Commit a transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn commit(&self) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Mark this transaction as done
		self.done.store(true, Ordering::Release);
		// Get the inner transaction
		let mut inner = self.inner.write().await;
		// Commit this transaction
		if let Err(err) = inner.tx.commit().await {
			if let Err(inner_err) = inner.tx.rollback().await {
				error!("Transaction commit failed {err} and rollback failed: {inner_err}");
			}
			return Err(err.into());
		}
		// Continue
		Ok(())
	}

	/// Check if a key exists
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists(&self, key: Key, version: Option<u64>) -> Result<bool> {
		// TiKV does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Check the key
		let res = inner.tx.key_exists(key).await?;
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get(&self, key: Key, version: Option<u64>) -> Result<Option<Val>> {
		// TiKV does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Get the key
		let res = inner.tx.get(key).await?;
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// TiKV does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Get the old value if we need to track operations
		let old_val = if !inner.savepoints.is_empty() || !inner.operations.is_empty() {
			inner.tx.get(key.clone()).await?
		} else {
			None
		};
		// Set the key
		inner.tx.put(key.clone(), val).await?;
		// Record operation after successful operation
		if !inner.savepoints.is_empty() || !inner.operations.is_empty() {
			match old_val {
				Some(existing_val) => {
					// Key existed, record operation to restore old value
					inner.operations.push(Operation::RestoreValue(key, existing_val));
				}
				None => {
					// Key didn't exist, record operation to delete it
					inner.operations.push(Operation::DeleteKey(key));
				}
			}
		}
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// TiKV does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Check if key exists
		let exists = inner.tx.key_exists(key.clone()).await?;
		if exists {
			return Err(Error::TransactionKeyAlreadyExists);
		}
		// Set the key
		inner.tx.put(key.clone(), val).await?;
		// Record operation after successful operation
		if !inner.savepoints.is_empty() || !inner.operations.is_empty() {
			// Key didn't exist (we just checked), record operation to delete it
			inner.operations.push(Operation::DeleteKey(key));
		}
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn putc(&self, key: Key, val: Val, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Get the current value
		let current = inner.tx.get(key.clone()).await?;
		// Check if condition is met
		match (&current, &chk) {
			(Some(v), Some(w)) if v == w => {}
			(None, None) => {}
			_ => return Err(Error::TrandsactionConditionNotMet),
		};
		// Set the key
		inner.tx.put(key.clone(), val).await?;
		// Record operation after successful operation
		if !inner.savepoints.is_empty() || !inner.operations.is_empty() {
			match current {
				Some(existing_val) => {
					// Key existed, record operation to restore old value
					inner.operations.push(Operation::RestoreValue(key, existing_val));
				}
				None => {
					// Key didn't exist, record operation to delete it
					inner.operations.push(Operation::DeleteKey(key));
				}
			}
		}
		// Return result
		Ok(())
	}

	/// Delete a key
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn del(&self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Get the old value if we need to track operations
		let old_val = if !inner.savepoints.is_empty() || !inner.operations.is_empty() {
			inner.tx.get(key.clone()).await?
		} else {
			None
		};
		// Delete the key
		inner.tx.delete(key.clone()).await?;
		// Record operation after successful operation
		if let Some(existing_val) = old_val {
			// Key existed, record operation to restore it
			inner.operations.push(Operation::RestoreDeleted(key, existing_val));
		}
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delc(&self, key: Key, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Get the current value
		let current = inner.tx.get(key.clone()).await?;
		// Check if condition is met
		match (&current, &chk) {
			(Some(v), Some(w)) if v == w => {}
			(None, None) => {}
			_ => return Err(Error::TrandsactionConditionNotMet),
		};
		// Delete the key
		inner.tx.delete(key.clone()).await?;
		// Record operation after successful operation
		if let Some(existing_val) = current {
			// Key existed, record operation to restore it
			inner.operations.push(Operation::RestoreDeleted(key, existing_val));
		}
		// Return result
		Ok(())
	}

	/// Delete a range of keys from the databases
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn delr(&self, rng: Range<Key>) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Delete the key range
		self.db.unsafe_destroy_range(rng.start..rng.end).await?;
		// Return result
		Ok(())
	}

	/// Retrieve a range of keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(&self, rng: Range<Key>, limit: u32, version: Option<u64>) -> Result<Vec<Key>> {
		// TiKV does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Scan the keys
		let res = inner.tx.scan_keys(rng, limit).await?.map(Key::from).collect();
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keysr(&self, rng: Range<Key>, limit: u32, version: Option<u64>) -> Result<Vec<Key>> {
		// TiKV does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Scan the keys
		let res = inner.tx.scan_keys_reverse(rng, limit).await?.map(Key::from).collect();
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan(
		&self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// TiKV does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Scan the keys
		let res = inner.tx.scan(rng, limit).await?.map(|kv| (Key::from(kv.0), kv.1)).collect();
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the database in reverse order
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scanr(
		&self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// TiKV does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Scan the keys
		let res =
			inner.tx.scan_reverse(rng, limit).await?.map(|kv| (Key::from(kv.0), kv.1)).collect();
		// Return result
		Ok(res)
	}

	// --------------------------------------------------
	// Savepoint functions
	// --------------------------------------------------

	/// Set a new save point on the transaction.
	async fn new_save_point(&self) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Take the current operations
		let operations = std::mem::take(&mut inner.operations);
		// Create a new savepoint with those operations
		inner.savepoints.push(Savepoint {
			operations,
		});
		// Continue
		Ok(())
	}

	/// Release the last save point.
	async fn release_last_save_point(&self) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Release the last savepoint
		inner.savepoints.pop();
		// Continue
		Ok(())
	}

	/// Rollback to the last save point.
	async fn rollback_to_save_point(&self) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Check if there are any savepoints
		if inner.savepoints.is_empty() {
			return Err(Error::Transaction("No savepoint to rollback to".to_string()));
		}
		// Get the most recent savepoint
		let savepoint = inner.savepoints.pop().expect("No savepoint to rollback to");
		// Take ownership of operations to avoid borrow checker issues
		let operations = std::mem::take(&mut inner.operations);
		// Execute undo operations in reverse order
		for op in operations.iter().rev() {
			match op {
				// Delete the key that was inserted
				Operation::DeleteKey(key) => {
					inner.tx.delete(key.clone()).await?;
				}
				// Restore the previous value
				Operation::RestoreValue(key, val) => {
					inner.tx.put(key.clone(), val.clone()).await?;
				}
				// Restore the deleted key
				Operation::RestoreDeleted(key, val) => {
					inner.tx.put(key.clone(), val.clone()).await?;
				}
			}
		}
		// Restore the savepoint's operations as the current ones
		inner.operations = savepoint.operations;
		// Continue
		Ok(())
	}

	// --------------------------------------------------
	// Timestamp functions
	// --------------------------------------------------

	/// Get the current monotonic timestamp
	async fn timestamp(&self) -> Result<Box<dyn Timestamp>> {
		Ok(Box::new(Timecode(self.inner.write().await.tx.current_timestamp().await?)))
	}

	/// Convert a versionstamp to timestamp bytes for this storage engine
	async fn timestamp_bytes_from_versionstamp(&self, version: u128) -> Result<Vec<u8>> {
		Ok(<Timecode as Timestamp>::from_versionstamp(version)?.to_ts_bytes())
	}

	/// Convert a datetime to timestamp bytes for this storage engine
	async fn timestamp_bytes_from_datetime(&self, datetime: DateTime<Utc>) -> Result<Vec<u8>> {
		Ok(<Timecode as Timestamp>::from_datetime(datetime)?.to_ts_bytes())
	}
}
