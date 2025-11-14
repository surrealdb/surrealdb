#![cfg(feature = "kv-surrealkv")]

mod cnf;

use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, bail, ensure};
use surrealkv::{Durability, Mode, Transaction as Tx, Tree, TreeBuilder};
use tokio::sync::RwLock;

use crate::err::Error;
use crate::key::debug::Sprintable;
use crate::kvs::{Key, Val, Version};

const TARGET: &str = "surrealdb::core::kvs::surrealkv";

pub struct Datastore {
	db: Tree,
}

pub struct Transaction {
	/// Is the transaction complete?
	done: AtomicBool,
	/// Is the transaction writeable?
	write: bool,
	/// The underlying datastore transaction
	inner: RwLock<Tx>,
}

impl Datastore {
	/// Open a new database
	pub(crate) async fn new(path: &str, enable_versions: bool) -> Result<Datastore> {
		// Configure custom options
		let builder = TreeBuilder::new();
		// Enable separated keys and values
		info!(target: TARGET, "Setting maximum segment size: {}", *cnf::SURREALKV_MAX_SEGMENT_SIZE);
		let builder = builder.with_enable_vlog(true);
		// Configure the maximum value log file size
		info!(target: TARGET, "Setting maximum segment size: {}", *cnf::SURREALKV_MAX_SEGMENT_SIZE);
		let builder = builder.with_vlog_max_file_size(0);
		// Configure the value cache capacity
		info!(target: TARGET, "Setting maximum segment size: {}", *cnf::SURREALKV_MAX_SEGMENT_SIZE);
		let builder = builder.with_vlog_cache_capacity(0);
		// Enable the block cache capacity
		info!(target: TARGET, "Setting maximum segment size: {}", *cnf::SURREALKV_MAX_SEGMENT_SIZE);
		let builder = builder.with_block_cache_capacity(0);
		// Disable versioned queries
		info!(target: TARGET, "Setting maximum segment size: {}", *cnf::SURREALKV_MAX_SEGMENT_SIZE);
		let builder = builder.with_versioning(enable_versions, 0);
		// Set the block size to 64 KiB
		info!(target: TARGET, "Setting maximum segment size: {}", *cnf::SURREALKV_MAX_SEGMENT_SIZE);
		let builder = builder.with_block_size(64 * 1024);
		// Log if writes should be synced
		info!(target: TARGET, "Wait for disk sync acknowledgement: {}", *cnf::SYNC_DATA);
		// Set the data storage directory
		let builder = builder.with_path(path.to_string().into());
		// Create a new datastore
		match builder.build() {
			Ok(db) => Ok(Datastore {
				db,
			}),
			Err(e) => Err(anyhow::Error::new(Error::Ds(e.to_string()))),
		}
	}

	/// Shutdown the database
	pub(crate) async fn shutdown(&self) -> Result<()> {
		// Shutdown the database
		if let Err(e) = self.db.close().await {
			error!("An error occured closing the database: {e}");
		}
		// Nothing to do here
		Ok(())
	}

	/// Start a new transaction
	pub(crate) async fn transaction(
		&self,
		write: bool,
		_: bool,
	) -> Result<Box<dyn crate::kvs::api::Transaction>> {
		// Create a new transaction
		let mut txn = match write {
			true => self.db.begin_with_mode(Mode::ReadWrite),
			false => self.db.begin_with_mode(Mode::ReadOnly),
		}?;
		// Set the transaction durability
		match *cnf::SYNC_DATA {
			true => txn.set_durability(Durability::Immediate),
			false => txn.set_durability(Durability::Eventual),
		};
		// Return the new transaction
		Ok(Box::new(Transaction {
			done: AtomicBool::new(false),
			write,
			inner: RwLock::new(txn),
		}))
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl super::api::Transaction for Transaction {
	fn kind(&self) -> &'static str {
		"surrealkv"
	}

	/// Check if closed
	fn closed(&self) -> bool {
		self.done.load(Ordering::Relaxed)
	}

	/// Check if writeable
	fn writeable(&self) -> bool {
		self.write
	}

	/// Cancels the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn cancel(&self) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Mark the transaction as done.
		self.done.store(true, Ordering::Release);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Rollback this transaction
		inner.rollback();
		// Continue
		Ok(())
	}

	/// Commits the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn commit(&self) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Mark the transaction as done.
		self.done.store(true, Ordering::Release);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Commit this transaction
		inner.commit().await?;
		// Continue
		Ok(())
	}

	/// Checks if a key exists in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists(&self, key: Key, version: Option<u64>) -> Result<bool> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Get the key
		let res = match version {
			Some(ts) => inner.get_at_version(&key, ts)?.is_some(),
			None => inner.get(&key)?.is_some(),
		};
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get(&self, key: Key, version: Option<u64>) -> Result<Option<Val>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Get the key
		let res = match version {
			Some(ts) => inner.get_at_version(&key, ts)?,
			None => inner.get(&key)?,
		};
		// Return result
		Ok(res.map(|v| v.to_vec()))
	}

	/// Insert or update a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Set the key
		match version {
			Some(ts) => inner.set_at_version(&key, &val, ts)?,
			None => inner.set(&key, &val)?,
		}
		// Return result
		Ok(())
	}

	/// Insert or replace a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn replace(&mut self, key: Key, val: Val) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Replace the key
		inner.replace(&key, &val)?;
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Set the key if empty
		if let Some(ts) = version {
			inner.set_at_version(&key, &val, ts)?;
		} else {
			match inner.get(&key)? {
				None => inner.set(&key, &val)?,
				_ => bail!(Error::TxKeyAlreadyExists),
			}
		}
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn putc(&self, key: Key, val: Val, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Set the key if valid
		match (inner.get(&key)?, chk) {
			(Some(v), Some(w)) if v == w => inner.set(&key, &val)?,
			(None, None) => inner.set(&key, &val)?,
			_ => bail!(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Delete a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn del(&mut self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Delete the key
		inner.soft_delete(&key)?;
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Delete the key if valid
		match (inner.get(&key)?, chk) {
			(Some(v), Some(w)) if v == w => inner.soft_delete(&key)?,
			(None, None) => inner.soft_delete(&key)?,
			_ => bail!(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Deletes all versions of a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clr(&mut self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Delete the key
		inner.delete(&key)?;
		// Return result
		Ok(())
	}

	/// Delete all versions of a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clrc(&mut self, key: Key, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Delete the key if valid
		match (inner.get(&key)?, chk) {
			(Some(v), Some(w)) if v == w => inner.delete(&key)?,
			(None, None) => inner.delete(&key)?,
			_ => bail!(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Retrieve a range of keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Retrieve the scan range
		let res = match version {
			Some(ts) => inner
				.keys_at_version(beg, end, ts)?
				.into_iter()
				.take(limit as usize)
				.map(|r| r.map(Key::from).map_err(Into::into))
				.collect::<Result<_>>()?,
			None => inner
				.keys(beg, end)?
				.into_iter()
				.take(limit as usize)
				.map(|r| r.map(Key::from).map_err(Into::into))
				.collect::<Result<_>>()?,
		};
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keysr(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Retrieve the scan range
		let res = match version {
			Some(ts) => inner
				.keys_at_version(beg, end, ts)?
				.rev()
				.into_iter()
				.take(limit as usize)
				.map(|r| r.map(Key::from).map_err(Into::into))
				.collect::<Result<_>>()?,
			None => inner
				.keys(beg, end)?
				.rev()
				.into_iter()
				.take(limit as usize)
				.map(|r| r.map(Key::from).map_err(Into::into))
				.collect::<Result<_>>()?,
		};
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Retrieve the scan range
		let res = match version {
			Some(ts) => inner
				.range_at_version(beg, end, ts)?
				.into_iter()
				.take(limit as usize)
				.map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())).map_err(Into::into))
				.collect::<Result<_>>()?,
			None => inner
				.range(beg, end)?
				.into_iter()
				.take(limit as usize)
				.map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())).map_err(Into::into))
				.collect::<Result<_>>()?,
		};
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scanr(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Retrieve the scan range
		let res = match version {
			Some(ts) => inner
				.range_at_version(beg, end, ts)?
				.rev()
				.into_iter()
				.take(limit as usize)
				.map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())).map_err(Into::into))
				.collect::<Result<_>>()?,
			None => inner
				.range(beg, end)?
				.rev()
				.into_iter()
				.take(limit as usize)
				.map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())).map_err(Into::into))
				.collect::<Result<_>>()?,
		};
		// Return result
		Ok(res)
	}

	/// Retrieve all the versions from a range of keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan_all_versions(
		&mut self,
		rng: Range<Key>,
		limit: u32,
	) -> Result<Vec<(Key, Val, Version, bool)>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let inner = self.inner.write().await;
		// Retrieve the scan range
		let res = inner
			.scan_all_versions(beg, end, Some(limit as usize))?
			.into_iter()
			.map(|(k, v, ts, del)| (k.to_vec(), v.to_vec(), ts, del))
			.collect();
		// Return result
		Ok(res)
	}

	/// Set a new save point on the transaction.
	async fn new_save_point(&self) -> Result<()> {
		self.inner.write().await.set_savepoint()?;
		Ok(())
	}

	/// Rollback to the last save point.
	async fn rollback_to_save_point(&self) -> Result<()> {
		self.inner.write().await.rollback_to_savepoint()?;
		Ok(())
	}

	/// Release the last save point.
	async fn release_last_save_point(&self) -> Result<()> {
		Ok(())
	}
}
