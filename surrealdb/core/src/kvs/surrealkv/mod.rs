#![cfg(feature = "kv-surrealkv")]

mod cnf;

use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};

use surrealkv::{Durability, Mode, Transaction as Tx, Tree, TreeBuilder};
use tokio::sync::RwLock;

use super::api::ScanLimit;
use super::err::{Error, Result};
use crate::key::debug::Sprintable;
use crate::kvs::api::Transactable;
use crate::kvs::{Key, Val};

const TARGET: &str = "surrealdb::core::kvs::surrealkv";

pub struct Datastore {
	db: Tree,
	enable_versions: bool,
}

pub struct Transaction {
	/// Is the transaction complete?
	done: AtomicBool,
	/// Is the transaction writeable?
	write: bool,
	/// Is versioning enabled?
	enable_versions: bool,
	/// The underlying datastore transaction
	inner: RwLock<Tx>,
}

impl Datastore {
	/// Open a new database
	pub(crate) async fn new(path: &str, enable_versions: bool) -> Result<Datastore> {
		// Configure custom options
		let builder = TreeBuilder::new();
		// Enable separated keys and values
		info!(target: TARGET, "Enabling value log separation: {}", *cnf::SURREALKV_ENABLE_VLOG);
		let builder = builder.with_enable_vlog(*cnf::SURREALKV_ENABLE_VLOG);
		// Configure the maximum value log file size
		info!(target: TARGET, "Setting value log max file size: {}", *cnf::SURREALKV_VLOG_MAX_FILE_SIZE);
		let builder = builder.with_vlog_max_file_size(*cnf::SURREALKV_VLOG_MAX_FILE_SIZE);
		// Enable the block cache capacity
		info!(target: TARGET, "Setting block cache capacity: {}", *cnf::SURREALKV_BLOCK_CACHE_CAPACITY);
		let builder = builder.with_block_cache_capacity(*cnf::SURREALKV_BLOCK_CACHE_CAPACITY);
		// Configure versioned queries
		info!(target: TARGET, "Versioning enabled: {} with unlimited retention period", enable_versions);
		let builder = builder.with_versioning(enable_versions, 0);
		// Set the block size
		info!(target: TARGET, "Setting block size: {}", *cnf::SURREALKV_BLOCK_SIZE);
		let builder = builder.with_block_size(*cnf::SURREALKV_BLOCK_SIZE);
		// Log if writes should be synced
		info!(target: TARGET, "Wait for disk sync acknowledgement: {}", *cnf::SYNC_DATA);
		// Set the data storage directory
		let builder = builder.with_path(path.to_string().into());
		// Create a new datastore
		match builder.build() {
			Ok(db) => Ok(Datastore {
				db,
				enable_versions,
			}),
			Err(e) => Err(Error::Datastore(e.to_string())),
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
	pub(crate) async fn transaction(&self, write: bool, _: bool) -> Result<Box<dyn Transactable>> {
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
			enable_versions: self.enable_versions,
			inner: RwLock::new(txn),
		}))
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl Transactable for Transaction {
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
		// Atomically mark transaction as done and check if it was already closed
		if self.done.swap(true, Ordering::AcqRel) {
			return Err(Error::TransactionFinished);
		}
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
		// Atomically mark transaction as done and check if it was already closed
		if self.done.swap(true, Ordering::AcqRel) {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
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
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Get the key
		let res = match version {
			Some(ts) => inner.get_at(&key, ts)?.is_some(),
			None => inner.get(&key)?.is_some(),
		};
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get(&self, key: Key, version: Option<u64>) -> Result<Option<Val>> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Get the key
		let res = match version {
			Some(ts) => inner.get_at(&key, ts)?,
			None => inner.get(&key)?,
		};
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
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
		// Set the key
		match version {
			Some(ts) => inner.set_at(&key, &val, ts)?,
			None => inner.set(&key, &val)?,
		}
		// Return result
		Ok(())
	}

	/// Insert or replace a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn replace(&self, key: Key, val: Val) -> Result<()> {
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
		// Replace the key
		inner.replace(&key, &val)?;
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
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
		// Set the key if empty
		if let Some(ts) = version {
			inner.set_at(&key, &val, ts)?;
		} else {
			match inner.get(&key)? {
				None => inner.set(&key, &val)?,
				_ => return Err(Error::TransactionKeyAlreadyExists),
			}
		}
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition.
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
		// Set the key if valid
		match (inner.get(&key)?, chk) {
			(Some(v), Some(w)) if v == w => inner.set(&key, &val)?,
			(None, None) => inner.set(&key, &val)?,
			_ => return Err(Error::TransactionConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Delete a key from the database.
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
		// Delete the key
		if self.enable_versions {
			inner.soft_delete(&key)?;
		} else {
			inner.delete(&key)?;
		}
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition.
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
		// Delete the key if valid
		if self.enable_versions {
			match (inner.get(&key)?, chk) {
				(Some(v), Some(w)) if v == w => inner.soft_delete(&key)?,
				(None, None) => inner.soft_delete(&key)?,
				_ => return Err(Error::TransactionConditionNotMet),
			};
		} else {
			match (inner.get(&key)?, chk) {
				(Some(v), Some(w)) if v == w => inner.delete(&key)?,
				(None, None) => inner.delete(&key)?,
				_ => return Err(Error::TransactionConditionNotMet),
			};
		}
		// Return result
		Ok(())
	}

	/// Deletes all versions of a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clr(&self, key: Key) -> Result<()> {
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
		// Delete the key
		inner.delete(&key)?;
		// Return result
		Ok(())
	}

	/// Delete all versions of a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clrc(&self, key: Key, chk: Option<Val>) -> Result<()> {
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
		// Delete the key if valid
		match (inner.get(&key)?, chk) {
			(Some(v), Some(w)) if v == w => inner.delete(&key)?,
			(None, None) => inner.delete(&key)?,
			_ => return Err(Error::TransactionConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Count the total number of keys within a range.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn count(&self, rng: Range<Key>) -> Result<usize> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Count items using range iterator
		let mut iter = inner.range(&beg, &end)?;
		let mut count = 0;
		iter.seek_first()?;
		while iter.valid() {
			count += 1;
			iter.next()?;
		}
		// Return result
		Ok(count)
	}

	/// Retrieve a range of keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Extract the limit count (surrealkv backend only supports count-based limits for keys)
		let limit = match limit {
			ScanLimit::Count(n) => n,
			ScanLimit::Bytes(n) => n, // Treat bytes as count for keys-only scan
			ScanLimit::BytesOrCount(_bytes, count) => count, // Use count for keys-only scan
		};
		// Retrieve the scan range
		let res = match version {
			Some(ts) => {
				let mut iter = inner.history(&beg, &end)?;
				let mut res = Vec::new();
				iter.seek_first()?;
				while iter.valid() && res.len() < limit as usize {
					if iter.timestamp() <= ts {
						res.push(iter.key());
					}
					iter.next()?;
				}
				res
			}
			None => {
				let mut iter = inner.range(&beg, &end)?;
				let mut res = Vec::new();
				iter.seek_first()?;
				while iter.valid() && res.len() < limit as usize {
					res.push(iter.key());
					iter.next()?;
				}
				res
			}
		};
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keysr(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Extract the limit count (surrealkv backend only supports count-based limits for keys)
		let limit = match limit {
			ScanLimit::Count(n) => n,
			ScanLimit::Bytes(n) => n, // Treat bytes as count for keys-only scan
			ScanLimit::BytesOrCount(_bytes, count) => count, // Use count for keys-only scan
		};
		// Retrieve the scan range
		let res = match version {
			Some(ts) => {
				let mut iter = inner.history(&beg, &end)?;
				let mut res = Vec::new();
				iter.seek_last()?;
				while iter.valid() && res.len() < limit as usize {
					if iter.timestamp() <= ts {
						res.push(iter.key());
					}
					iter.prev()?;
				}
				res
			}
			None => {
				let mut iter = inner.range(&beg, &end)?;
				let mut res = Vec::new();
				iter.seek_last()?;
				while iter.valid() && res.len() < limit as usize {
					res.push(iter.key());
					iter.prev()?;
				}
				res
			}
		};
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Retrieve the scan range based on limit type
		let res = match limit {
			ScanLimit::Count(c) => match version {
				Some(ts) => {
					let mut iter = inner.history(&beg, &end)?;
					let mut res = Vec::new();
					iter.seek_first()?;
					while iter.valid() && res.len() < c as usize {
						if iter.timestamp() <= ts {
							let value = iter.value()?;
							res.push((iter.key(), value));
						}
						iter.next()?;
					}
					res
				}
				None => {
					let mut iter = inner.range(&beg, &end)?;
					let mut res = Vec::new();
					iter.seek_first()?;
					while iter.valid() && res.len() < c as usize {
						let key = iter.key();
						let value = iter.value()?.unwrap_or_default();
						res.push((key, value));
						iter.next()?;
					}
					res
				}
			},
			ScanLimit::Bytes(b) => {
				let mut res = Vec::new();
				let mut bytes_fetched = 0usize;
				match version {
					Some(ts) => {
						let mut iter = inner.history(&beg, &end)?;
						iter.seek_first()?;
						while iter.valid() {
							if iter.timestamp() <= ts {
								let value = iter.value()?;
								let key = iter.key();
								bytes_fetched += value.len();
								res.push((key, value));
								// Stop if we've exceeded byte limit AND have at least one entry
								if bytes_fetched >= b as usize && !res.is_empty() {
									break;
								}
							}
							iter.next()?;
						}
					}
					None => {
						let mut iter = inner.range(&beg, &end)?;
						iter.seek_first()?;
						while iter.valid() {
							let key = iter.key();
							let value = iter.value()?.unwrap_or_default();
							bytes_fetched += value.len();
							res.push((key, value));
							// Stop if we've exceeded byte limit AND have at least one entry
							if bytes_fetched >= b as usize && !res.is_empty() {
								break;
							}
							iter.next()?;
						}
					}
				}
				res
			}
			ScanLimit::BytesOrCount(bytes, count) => {
				let mut res = Vec::new();
				let mut bytes_fetched = 0usize;
				match version {
					Some(ts) => {
						let mut iter = inner.history(&beg, &end)?;
						iter.seek_first()?;
						while iter.valid() && res.len() < count as usize {
							if iter.timestamp() <= ts {
								let value = iter.value()?;
								let key = iter.key();
								bytes_fetched += value.len();
								res.push((key, value));
								// Stop if we've exceeded byte limit AND have at least one entry
								if bytes_fetched >= bytes as usize && !res.is_empty() {
									break;
								}
							}
							iter.next()?;
						}
					}
					None => {
						let mut iter = inner.range(&beg, &end)?;
						iter.seek_first()?;
						while iter.valid() && res.len() < count as usize {
							let key = iter.key();
							let value = iter.value()?.unwrap_or_default();
							bytes_fetched += value.len();
							res.push((key, value));
							// Stop if we've exceeded byte limit AND have at least one entry
							if bytes_fetched >= bytes as usize && !res.is_empty() {
								break;
							}
							iter.next()?;
						}
					}
				}
				res
			}
		};
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scanr(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Retrieve the scan range
		let res = match limit {
			ScanLimit::Count(n) => match version {
				Some(ts) => {
					let mut iter = inner.history(&beg, &end)?;
					let mut res = Vec::new();
					iter.seek_last()?;
					while iter.valid() && res.len() < n as usize {
						if iter.timestamp() <= ts {
							let value = iter.value()?;
							res.push((iter.key(), value));
						}
						iter.prev()?;
					}
					res
				}
				None => {
					let mut iter = inner.range(&beg, &end)?;
					let mut res = Vec::new();
					iter.seek_last()?;
					while iter.valid() && res.len() < n as usize {
						let key = iter.key();
						let value = iter.value()?.unwrap_or_default();
						res.push((key, value));
						iter.prev()?;
					}
					res
				}
			},
			ScanLimit::Bytes(n) => {
				let mut res = Vec::new();
				let mut bytes_fetched = 0usize;
				match version {
					Some(ts) => {
						let mut iter = inner.history(&beg, &end)?;
						iter.seek_last()?;
						while iter.valid() {
							if iter.timestamp() <= ts {
								let value = iter.value()?;
								let key = iter.key();
								bytes_fetched += value.len();
								res.push((key, value));
								// Stop if we've exceeded byte limit AND have at least one entry
								if bytes_fetched >= n as usize && !res.is_empty() {
									break;
								}
							}
							iter.prev()?;
						}
					}
					None => {
						let mut iter = inner.range(&beg, &end)?;
						iter.seek_last()?;
						while iter.valid() {
							let key = iter.key();
							let value = iter.value()?.unwrap_or_default();
							bytes_fetched += value.len();
							res.push((key, value));
							// Stop if we've exceeded byte limit AND have at least one entry
							if bytes_fetched >= n as usize && !res.is_empty() {
								break;
							}
							iter.prev()?;
						}
					}
				}
				res
			}
			ScanLimit::BytesOrCount(bytes, count) => {
				let mut res = Vec::new();
				let mut bytes_fetched = 0usize;
				match version {
					Some(ts) => {
						let mut iter = inner.history(&beg, &end)?;
						iter.seek_last()?;
						while iter.valid() && res.len() < count as usize {
							if iter.timestamp() <= ts {
								let value = iter.value()?;
								let key = iter.key();
								bytes_fetched += value.len();
								res.push((key, value));
								// Stop if we've exceeded byte limit AND have at least one entry
								if bytes_fetched >= bytes as usize && !res.is_empty() {
									break;
								}
							}
							iter.prev()?;
						}
					}
					None => {
						let mut iter = inner.range(&beg, &end)?;
						iter.seek_last()?;
						while iter.valid() && res.len() < count as usize {
							let key = iter.key();
							let value = iter.value()?.unwrap_or_default();
							bytes_fetched += value.len();
							res.push((key, value));
							// Stop if we've exceeded byte limit AND have at least one entry
							if bytes_fetched >= bytes as usize && !res.is_empty() {
								break;
							}
							iter.prev()?;
						}
					}
				}
				res
			}
		};
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
