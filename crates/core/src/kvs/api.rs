//! This module defines the API for a transaction in a key-value store.
#![warn(clippy::missing_docs_in_private_items)]

use std::ops::Range;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use super::tr::Check;
use super::util;
use crate::cnf::{COUNT_BATCH_SIZE, NORMAL_FETCH_SIZE};
use crate::err::Error;
use crate::key::database::vs::VsKey;
use crate::key::debug::Sprintable;
use crate::kvs::batch::Batch;
use crate::kvs::savepoint::{SaveOperation, SavePoints, SavePrepare, SavedValue};
use crate::kvs::{KVKey, KVValue, Key, Val, Version};
use crate::vs::VersionStamp;

mod requirements {
	//! This module defines the trait requirements for a transaction.
	//!
	//! The reason this exists is to allow for swapping out the `Send`
	//! requirement for WASM targets, where we don't want to require `Send` for
	//! transactions. But for non-WASM targets, we do want to require `Send`
	//! for transactions.
	//!
	//! There is no `cfg` / `cfg_attr` support for trait requirements, so we use
	//! this dependent trait to conditionally require `Send` based on the
	//! target family.
	//!
	//! Without this, we would have had to duplicate the entire `Transaction`
	//! trait for WASM and non-WASM targets, which would have been a pain to
	//! maintain.

	/// This trait defines WASM requirements for a transaction.
	#[cfg(target_family = "wasm")]
	pub trait TransactionRequirements {}

	/// Implements the `TransactionRequirements` trait for all types.
	#[cfg(target_family = "wasm")]
	impl<T> TransactionRequirements for T {}

	/// This trait defines non-WASM requirements for a transaction.
	#[cfg(not(target_family = "wasm"))]
	pub trait TransactionRequirements: Send {}

	/// Implements the `TransactionRequirements` trait for all types that are
	/// `Send`.
	#[cfg(not(target_family = "wasm"))]
	impl<T: Send> TransactionRequirements for T {}
}

/// This trait defines the API for a transaction in a key-value store.
///
/// All keys and values are represented as byte arrays, encoding is handled
/// by [`super::tr::Transactor`].
#[allow(dead_code, reason = "Not used when none of the storage backends are enabled.")]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait Transaction: requirements::TransactionRequirements {
	/// Returns if the transaction supports scanning in reverse.
	fn supports_reverse_scan(&self) -> bool;

	/// Get the name of the transaction type.
	fn kind(&self) -> &'static str;

	/// Specify how we should handle unclosed transactions.
	///
	/// If a transaction is not cancelled or rolled back then
	/// this can cause issues on some storage engine
	/// implementations. In tests we can ignore unhandled
	/// transactions, whilst in development we should panic
	/// so that any unintended behaviour is detected, and in
	/// production we should only log a warning.
	fn check_level(&mut self, check: Check);

	/// Check if transaction is finished.
	///
	/// If the transaction has been cancelled or committed,
	/// then this function will return [`true`], and any further
	/// calls to functions on this transaction will result
	/// in an [`Error::TxFinished`] error.
	fn closed(&self) -> bool;

	/// Check if transaction is writeable.
	///
	/// If the transaction has been marked as a writeable
	/// transaction, then this function will return [`true`].
	/// This fuction can be used to check whether a transaction
	/// allows data to be modified, and if not then the function
	/// will return an [`Error::TxReadonly`] error.
	fn writeable(&self) -> bool;

	/// Cancel a transaction.
	///
	/// This reverses all changes made within the transaction.
	async fn cancel(&mut self) -> Result<()>;

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	async fn commit(&mut self) -> Result<()>;

	/// Check if a key exists in the datastore.
	async fn exists(&mut self, key: Key, version: Option<u64>) -> Result<bool>;

	/// Fetch a key from the datastore.
	async fn get(&mut self, key: Key, version: Option<u64>) -> Result<Option<Val>>;

	/// Insert or update a key in the datastore.
	async fn set(&mut self, key: Key, val: Val, version: Option<u64>) -> Result<()>;

	/// Insert a key if it doesn't exist in the datastore.
	async fn put(&mut self, key: Key, val: Val, version: Option<u64>) -> Result<()>;

	/// Update a key in the datastore if the current value matches a condition.
	async fn putc(&mut self, key: Key, val: Val, chk: Option<Val>) -> Result<()>;

	/// Delete a key from the datastore.
	async fn del(&mut self, key: Key) -> Result<()>;

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<()>;

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys without values, in a single
	/// request to the underlying datastore.
	async fn keys(&mut self, rng: Range<Key>, limit: u32, version: Option<u64>)
	-> Result<Vec<Key>>;

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys without values, in a single
	/// request to the underlying datastore.
	async fn keysr(
		&mut self,
		_rng: Range<Key>,
		_limit: u32,
		_version: Option<u64>,
	) -> Result<Vec<Key>> {
		Err(anyhow::Error::new(Error::UnsupportedReversedScans))
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore.
	async fn scan(
		&mut self,
		_rng: Range<Key>,
		_limit: u32,
		_version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		Err(anyhow::Error::new(Error::UnsupportedVersionedQueries))
	}

	/// Retrieve a specific range of keys from the datastore in reverse order.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore.
	async fn scanr(
		&mut self,
		_rng: Range<Key>,
		_limit: u32,
		_version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		Err(anyhow::Error::new(Error::UnsupportedReversedScans))
	}

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn replace(&mut self, key: Key, val: Val) -> Result<()> {
		self.set(key, val, None).await
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clr(&mut self, key: Key) -> Result<()> {
		self.del(key).await
	}

	/// Delete all versions of a key from the datastore if the current value
	/// matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clrc(&mut self, key: Key, chk: Option<Val>) -> Result<()> {
		self.delc(key, chk).await
	}

	/// Fetch many keys from the datastore.
	///
	/// This function fetches all matching keys pairs from the underlying
	/// datastore concurrently.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(keys = keys.sprint()))]
	async fn getm(&mut self, keys: Vec<Key>) -> Result<Vec<Option<Val>>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Continue with function logic
		let mut out = Vec::with_capacity(keys.len());
		for key in keys.into_iter() {
			if let Some(val) = self.get(key, None).await? {
				out.push(Some(val));
			} else {
				out.push(None);
			}
		}
		Ok(out)
	}

	/// Retrieve a range of prefixed keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn getp(&mut self, key: Key) -> Result<Vec<(Key, Val)>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Continue with function logic
		let range = util::to_prefix_range(key)?;
		self.getr(range, None).await
	}

	/// Retrieve a range of keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn getr(&mut self, rng: Range<Key>, version: Option<u64>) -> Result<Vec<(Key, Val)>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Continue with function logic
		let mut out = vec![];
		let mut next = Some(rng);
		while let Some(rng) = next {
			let res = self.batch_keys_vals(rng, *NORMAL_FETCH_SIZE, version).await?;
			next = res.next;
			for v in res.result.into_iter() {
				out.push(v);
			}
		}
		Ok(out)
	}

	/// Delete a range of prefixed keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delp(&mut self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Continue with function logic
		let range = util::to_prefix_range(key)?;
		self.delr(range).await
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn delr(&mut self, rng: Range<Key>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Continue with function logic
		let mut next = Some(rng);
		while let Some(rng) = next {
			let res = self.batch_keys(rng, *NORMAL_FETCH_SIZE, None).await?;
			next = res.next;
			for k in res.result.into_iter() {
				self.del(k).await?;
			}
		}
		Ok(())
	}

	/// Delete all versions of a range of prefixed keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clrp(&mut self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);

		let range = util::to_prefix_range(key)?;
		self.clrr(range).await
	}

	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn clrr(&mut self, rng: Range<Key>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Continue with function logic
		let mut next = Some(rng);
		while let Some(rng) = next {
			let res = self.batch_keys(rng, *NORMAL_FETCH_SIZE, None).await?;
			next = res.next;
			for k in res.result {
				self.clr(k).await?;
			}
		}
		Ok(())
	}

	/// Count the total number of keys within a range in the datastore.
	///
	/// This function fetches the total key count from the underlying datastore
	/// in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn count(&mut self, rng: Range<Key>) -> Result<usize> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Continue with function logic
		let mut len = 0;
		let mut next = Some(rng);
		while let Some(rng) = next {
			let res = self.batch_keys(rng, *COUNT_BATCH_SIZE, None).await?;
			next = res.next;
			len += res.result.len();
		}
		Ok(len)
	}

	/// Retrieve all the versions for a specific range of keys from the
	/// datastore.
	///
	/// This function fetches all the versions for the full range of key-value
	/// pairs, in a single request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = _rng.sprint()))]
	async fn scan_all_versions(
		&mut self,
		_rng: Range<Key>,
		_limit: u32,
	) -> Result<Vec<(Key, Val, Version, bool)>> {
		Err(anyhow::Error::new(Error::UnsupportedVersionedQueries))
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches keys, in batches, with multiple requests to the
	/// underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn batch_keys(
		&mut self,
		rng: Range<Key>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<Key>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Continue with function logic
		let end = rng.end.clone();
		// Scan for the next batch
		let res = self.keys(rng, batch, version).await?;
		// Check if range is consumed
		if res.len() < batch as usize && batch > 0 {
			Ok(Batch::<Key>::new(None, res))
		} else {
			match res.last() {
				Some(k) => {
					let mut k = k.clone();
					util::advance_key(&mut k);
					Ok(Batch::<Key>::new(
						Some(Range {
							start: k,
							end,
						}),
						res,
					))
				}
				// We have checked the length above, so
				// there should be a last item in the
				// vector, so we shouldn't arrive here
				None => Ok(Batch::<Key>::new(None, res)),
			}
		}
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches key-value pairs, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn batch_keys_vals(
		&mut self,
		rng: Range<Key>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<(Key, Val)>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Continue with function logic
		let end = rng.end.clone();
		// Scan for the next batch
		let res = self.scan(rng, batch, version).await?;
		// Check if range is consumed
		if res.len() < batch as usize && batch > 0 {
			Ok(Batch::<(Key, Val)>::new(None, res))
		} else {
			match res.last() {
				Some((k, _)) => {
					let mut k = k.clone();
					util::advance_key(&mut k);

					Ok(Batch::<(Key, Val)>::new(
						Some(Range {
							start: k,
							end,
						}),
						res,
					))
				}
				// We have checked the length above, so
				// there should be a last item in the
				// vector, so we shouldn't arrive here
				None => Ok(Batch::<(Key, Val)>::new(None, res)),
			}
		}
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches key-value-version pairs, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn batch_keys_vals_versions(
		&mut self,
		rng: Range<Key>,
		batch: u32,
	) -> Result<Batch<(Key, Val, Version, bool)>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Continue with function logic
		let end = rng.end.clone();
		// Scan for the next batch
		let res = self.scan_all_versions(rng, batch).await?;
		// Check if range is consumed
		if res.len() < batch as usize && batch > 0 {
			Ok(Batch::<(Key, Val, Version, bool)>::new(None, res))
		} else {
			match res.last() {
				Some((k, _, _, _)) => {
					let mut k = k.clone();
					util::advance_key(&mut k);
					Ok(Batch::<(Key, Val, Version, bool)>::new(
						Some(Range {
							start: k,
							end,
						}),
						res,
					))
				}
				// We have checked the length above, so
				// there should be a last item in the
				// vector, so we shouldn't arrive here
				None => Ok(Batch::<(Key, Val, Version, bool)>::new(None, res)),
			}
		}
	}

	/// Obtain a new change timestamp for a key
	/// which is replaced with the current timestamp when the transaction is
	/// committed. NOTE: This should be called when composing the change feed
	/// entries for this transaction, which should be done immediately before
	/// the transaction commit. That is to keep other transactions commit
	/// delay(pessimistic) or conflict(optimistic) as less as possible.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn get_timestamp(&mut self, key: VsKey) -> Result<VersionStamp> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);

		let key_encoded = key.encode_key()?;
		// Calculate the version number
		let ver = match self.get(key_encoded.clone(), None).await? {
			Some(prev) => <VsKey as KVKey>::ValueType::kv_decode_value(prev)?
				.next()
				.context("exhausted all possible timestamps")?,
			None => VersionStamp::from_u64(1),
		};
		// Store the timestamp to prevent other transactions from committing
		self.set(key_encoded, ver.kv_encode_value()?, None).await?;
		// Return the uint64 representation of the timestamp as the result
		Ok(ver)
	}

	/// Insert the versionstamped key into the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn set_versionstamp(
		&mut self,
		ts_key: VsKey,
		prefix: Key,
		suffix: Key,
		val: Val,
	) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Continue with function logic
		let ts = self.get_timestamp(ts_key).await?;
		let mut k: Vec<u8> = prefix;
		k.extend_from_slice(&ts.as_bytes());
		k.extend_from_slice(&suffix);

		self.set(k, val, None).await
	}

	/// Get the save points for this transaction.
	fn get_save_points(&mut self) -> &mut SavePoints;

	/// Set a new current save point for this transaction.
	fn new_save_point(&mut self) {
		self.get_save_points().new_save_point()
	}

	/// Rollback to the last save point.
	async fn rollback_to_save_point(&mut self) -> Result<()> {
		let sp = self.get_save_points().pop()?;

		for (key, saved_value) in sp {
			match saved_value.last_operation {
				SaveOperation::Set | SaveOperation::Put => {
					if let Some(initial_value) = saved_value.saved_val {
						// If the last operation was a SET or PUT
						// then we just have set back the key to its initial value
						self.set(key, initial_value, saved_value.saved_version).await?;
					} else {
						// If the last operation on this key was not a DEL operation,
						// then we have to delete the key
						self.del(key).await?;
					}
				}
				SaveOperation::Del => {
					if let Some(initial_value) = saved_value.saved_val {
						// If the last operation was a DEL,
						// then we have to put back the initial value
						self.put(key, initial_value, saved_value.saved_version).await?;
					}
				}
			}
		}
		Ok(())
	}

	/// Release the last save point.
	fn release_last_save_point(&mut self) -> Result<()> {
		self.get_save_points().pop()?;
		Ok(())
	}

	/// Prepare a save point for a key.
	async fn save_point_prepare(
		&mut self,
		key: &Key,
		version: Option<u64>,
		op: SaveOperation,
	) -> Result<Option<SavePrepare>> {
		let is_saved_key = self.get_save_points().is_saved_key(key);
		let r = match is_saved_key {
			None => None,
			Some(true) => Some(SavePrepare::AlreadyPresent(key.clone(), op)),
			Some(false) => {
				let val = self.get(key.clone(), version).await?;
				Some(SavePrepare::NewKey(key.clone(), SavedValue::new(val, version, op)))
			}
		};
		Ok(r)
	}
}
