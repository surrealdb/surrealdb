#![cfg(feature = "kv-tikv")]

mod cnf;

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Result, bail, ensure};
use tikv::{CheckLevel, Config, TimestampExt, TransactionClient, TransactionOptions};
use tokio::sync::RwLock;

use crate::err::Error;
use crate::key::database::vs::VsKey;
use crate::key::debug::Sprintable;
use crate::kvs::key::KVKey;
use crate::kvs::savepoint::{SaveOperation, SavePoints, SavePrepare};
use crate::kvs::{Key, Val};
use crate::vs::VersionStamp;

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

struct TransactionInner {
	/// The underlying datastore transaction
	tx: tikv::Transaction,
	/// The savepoints for this transaction
	sp: SavePoints,
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
			_ => bail!(Error::Ds("Invalid TiKV API version".into())),
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
			Err(e) => Err(anyhow::Error::new(Error::Ds(e.to_string()))),
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
	) -> Result<Box<dyn crate::kvs::api::Transaction>> {
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
					sp: Default::default(),
				}),
				db: self.db.clone(),
			})),
			Err(e) => Err(anyhow::Error::new(Error::Tx(e.to_string()))),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl super::api::Transaction for Transaction {
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
		ensure!(!self.closed(), Error::TxFinished);
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
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
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
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
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
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
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
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Prepare the savepoint if any
		let prepared = self.prepare_save_point(&key, version, SaveOperation::Set).await?;
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Set the key
		inner.tx.put(key, val).await?;
		// Store the save point
		if let Some(prepared) = prepared {
			inner.sp.save(prepared);
		}
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// TiKV does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Hydrate the savepoint if any
		let prepared = self.prepare_save_point(&key, version, SaveOperation::Put).await?;
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Set the key if empty
		match inner.tx.key_exists(key.clone()).await? {
			false => inner.tx.put(key, val).await?,
			true => bail!(Error::TxKeyAlreadyExists),
		};
		// Store the save point
		if let Some(prepared) = prepared {
			inner.sp.save(prepared);
		}
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn putc(&self, key: Key, val: Val, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Hydrate the savepoint if any
		let prepared = self.prepare_save_point(&key, None, SaveOperation::Put).await?;
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Set the key if valid
		match (inner.tx.get(key.clone()).await?, chk) {
			(Some(v), Some(w)) if v == w => inner.tx.put(key, val).await?,
			(None, None) => inner.tx.put(key, val).await?,
			_ => bail!(Error::TxConditionNotMet),
		};
		// Confirm the save point
		if let Some(prepared) = prepared {
			inner.sp.save(prepared);
		}
		// Return result
		Ok(())
	}

	/// Delete a key
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn del(&self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Hydrate the savepoint if any
		let prepared = self.prepare_save_point(&key, None, SaveOperation::Del).await?;
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Delete the key
		inner.tx.delete(key).await?;
		// Confirm the save point
		if let Some(prepared) = prepared {
			inner.sp.save(prepared);
		}
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delc(&self, key: Key, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Hydrate the savepoint if any
		let prepared = self.prepare_save_point(&key, None, SaveOperation::Del).await?;
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Set the key if valie
		match (inner.tx.get(key.clone()).await?, chk) {
			(Some(v), Some(w)) if v == w => inner.tx.delete(key).await?,
			(None, None) => inner.tx.delete(key).await?,
			_ => bail!(Error::TxConditionNotMet),
		};
		// Confirm the save point
		if let Some(prepared) = prepared {
			inner.sp.save(prepared);
		}
		// Return result
		Ok(())
	}

	/// Delete a range of keys from the databases
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn delr(&self, rng: Range<Key>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Delete the key range
		self.db.unsafe_destroy_range(rng.start..rng.end).await?;
		// Return result
		Ok(())
	}

	/// Retrieve a range of keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(&self, rng: Range<Key>, limit: u32, version: Option<u64>) -> Result<Vec<Key>> {
		// TiKV does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
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
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
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
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
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
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Scan the keys
		let res =
			inner.tx.scan_reverse(rng, limit).await?.map(|kv| (Key::from(kv.0), kv.1)).collect();
		// Return result
		Ok(res)
	}

	/// Obtain a new change timestamp for a key
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn get_versionstamp(&self, key: VsKey) -> Result<VersionStamp> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Get the transaction version
		let ver = self.inner.write().await.tx.current_timestamp().await?.version();
		// Get the encoded key
		let enc = key.encode_key()?;
		// Calculate the previous version value
		if let Some(prev) = self.get(enc.clone(), None).await? {
			let prev = VersionStamp::from_slice(prev.as_slice())?.try_into_u64()?;
			ensure!(
				prev < ver,
				Error::Tx(format!("Previous version {prev} is greater than current version {ver}"))
			);
		};
		// Convert the timestamp to a versionstamp
		let ver = VersionStamp::from_u64(ver);
		// Store the timestamp to prevent other transactions from committing
		self.set(enc, ver.to_vec(), None).await?;
		// Return the uint64 representation of the timestamp as the result
		Ok(ver)
	}

	/// Set a new save point on the transaction.
	async fn new_save_point(&self) -> Result<()> {
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Create a new savepoint
		inner.sp.new_save_point();
		// All ok
		Ok(())
	}

	/// Release the last save point.
	async fn release_last_save_point(&self) -> Result<()> {
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Release the last savepoint
		inner.sp.pop();
		// All ok
		Ok(())
	}

	/// Rollback to the last save point.
	async fn rollback_to_save_point(&self) -> Result<()> {
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Release the last savepoint
		let sp = inner.sp.pop()?;
		// Loop over the savepoint entries
		for (key, val) in sp {
			match val.last_operation {
				SaveOperation::Set | SaveOperation::Put => {
					if let Some(initial_value) = val.saved_val {
						// If the last operation was a SET or PUT
						// then we just have set back the key to its initial value
						inner.tx.put(key, initial_value).await?;
					} else {
						// If the last operation on this key was not a DEL operation,
						// then we have to delete the key
						inner.tx.delete(key).await?;
					}
				}
				SaveOperation::Del => {
					if let Some(initial_value) = val.saved_val {
						// If the last operation was a DEL,
						// then we have to put back the initial value
						inner.tx.put(key, initial_value).await?;
					}
				}
			}
		}
		// All ok
		Ok(())
	}
}

impl Transaction {
	/// Prepare a save point for a key.
	async fn prepare_save_point(
		&self,
		key: &Key,
		version: Option<u64>,
		op: SaveOperation,
	) -> Result<Option<SavePrepare>> {
		// Import traits
		use crate::kvs::api::Transaction;
		use crate::kvs::savepoint::SavedValue;
		// Load the inner transaction
		let inner = self.inner.write().await;
		// Check if we have a savepoint
		if inner.sp.is_some() {
			//
			let is_saved_key = inner.sp.is_saved_key(key);
			//
			let prepared = match is_saved_key {
				None => None,
				Some(true) => Some(SavePrepare::AlreadyPresent(key.clone(), op)),
				Some(false) => {
					let val = self.get(key.clone(), version).await?;
					Some(SavePrepare::NewKey(key.clone(), SavedValue::new(val, version, op)))
				}
			};
			// Return the
			return Ok(prepared);
		}
		// Noting prepared
		Ok(None)
	}
}
