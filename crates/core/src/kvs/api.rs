use super::tr::Check;
use super::util;
use crate::cnf::COUNT_BATCH_SIZE;
use crate::cnf::NORMAL_FETCH_SIZE;
use crate::err::Error;
use crate::key::debug::Sprintable;
use crate::kvs::{batch::Batch, Key, KeyEncode, Val, Version};
use crate::vs::VersionStamp;
use std::fmt::Debug;
use std::ops::Range;

#[allow(dead_code)] // not used when non of the storage backends are enabled.
pub trait Transaction {
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
	async fn cancel(&mut self) -> Result<(), Error>;

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	async fn commit(&mut self) -> Result<(), Error>;

	/// Check if a key exists in the datastore.
	async fn exists<K>(&mut self, key: K, version: Option<u64>) -> Result<bool, Error>
	where
		K: KeyEncode + Sprintable + Debug;

	/// Fetch a key from the datastore.
	async fn get<K>(&mut self, key: K, version: Option<u64>) -> Result<Option<Val>, Error>
	where
		K: KeyEncode + Sprintable + Debug;

	/// Insert or update a key in the datastore.
	async fn set<K, V>(&mut self, key: K, val: V, version: Option<u64>) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
		V: Into<Val> + Debug;

	/// Insert a key if it doesn't exist in the datastore.
	async fn put<K, V>(&mut self, key: K, val: V, version: Option<u64>) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
		V: Into<Val> + Debug;

	/// Update a key in the datastore if the current value matches a condition.
	async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
		V: Into<Val> + Debug;

	/// Delete a key from the datastore.
	async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug;

	/// Delete a key from the datastore if the current value matches a condition.
	async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
		V: Into<Val> + Debug;

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys without values, in a single request to the underlying datastore.
	async fn keys<K>(
		&mut self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>, Error>
	where
		K: KeyEncode + Sprintable + Debug;

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single request to the underlying datastore.
	async fn scan<K>(
		&mut self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>, Error>
	where
		K: KeyEncode + Sprintable + Debug;

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn replace<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
		V: Into<Val> + Debug,
	{
		self.set(key, val, None).await
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clr<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		self.del(key).await
	}

	/// Delete all versions of a key from the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clrc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
		V: Into<Val> + Debug,
	{
		self.delc(key, chk).await
	}

	/// Fetch many keys from the datastore.
	///
	/// This function fetches all matching keys pairs from the underlying datastore concurrently.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(keys = keys.sprint()))]
	async fn getm<K>(&mut self, keys: Vec<K>) -> Result<Vec<Option<Val>>, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
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
	/// This function fetches all matching key-value pairs from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn getp<K>(&mut self, key: K) -> Result<Vec<(Key, Val)>, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Continue with function logic
		let range = util::to_prefix_range(key)?;
		self.getr(range, None).await
	}

	/// Retrieve a range of keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn getr<K>(
		&mut self,
		rng: Range<K>,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Continue with function logic
		let mut out = vec![];
		let beg: Key = rng.start.encode()?;
		let end: Key = rng.end.encode()?;
		let mut next = Some(beg..end);
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
	/// This function deletes all matching key-value pairs from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delp<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TxReadonly);
		}
		// Continue with function logic
		let range = util::to_prefix_range(key)?;
		self.delr(range).await
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn delr<K>(&mut self, rng: Range<K>) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TxReadonly);
		}
		// Continue with function logic
		let beg: Key = rng.start.encode()?;
		let end: Key = rng.end.encode()?;
		let mut next = Some(beg..end);
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
	/// This function deletes all matching key-value pairs from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clrp<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TxReadonly);
		}

		let range = util::to_prefix_range(key)?;
		self.clrr(range).await
	}

	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn clrr<K>(&mut self, rng: Range<K>) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TxReadonly);
		}
		// Continue with function logic
		let beg: Key = rng.start.encode()?;
		let end: Key = rng.end.encode()?;
		let mut next = Some(beg..end);
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
	/// This function fetches the total key count from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn count<K>(&mut self, rng: Range<K>) -> Result<usize, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Continue with function logic
		let mut len = 0;
		let beg: Key = rng.start.encode()?;
		let end: Key = rng.end.encode()?;
		let mut next = Some(beg..end);
		while let Some(rng) = next {
			let res = self.batch_keys(rng, *COUNT_BATCH_SIZE, None).await?;
			next = res.next;
			len += res.result.len();
		}
		Ok(len)
	}

	/// Retrieve all the versions for a specific range of keys from the datastore.
	///
	/// This function fetches all the versions for the full range of key-value pairs, in a single request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan_all_versions<K>(
		&mut self,
		rng: Range<K>,
		limit: u32,
	) -> Result<Vec<(Key, Val, Version, bool)>, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		Err(Error::UnsupportedVersionedQueries)
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches keys, in batches, with multiple requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn batch_keys<K>(
		&mut self,
		rng: Range<K>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<Key>, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Continue with function logic
		let beg: Key = rng.start.encode()?;
		let end: Key = rng.end.encode()?;
		// Scan for the next batch
		let res = self.keys(beg..end.clone(), batch, version).await?;
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
	/// This function fetches key-value pairs, in batches, with multiple requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn batch_keys_vals<K>(
		&mut self,
		rng: Range<K>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<(Key, Val)>, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Continue with function logic
		let beg: Key = rng.start.encode()?;
		let end: Key = rng.end.encode()?;
		// Scan for the next batch
		let res = self.scan(beg..end.clone(), batch, version).await?;
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
	/// This function fetches key-value-version pairs, in batches, with multiple requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn batch_keys_vals_versions<K>(
		&mut self,
		rng: Range<K>,
		batch: u32,
	) -> Result<Batch<(Key, Val, Version, bool)>, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Continue with function logic
		let beg: Key = rng.start.encode()?;
		let end: Key = rng.end.encode()?;
		// Scan for the next batch
		let res = self.scan_all_versions(beg..end.clone(), batch).await?;
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
	/// which is replaced with the current timestamp when the transaction is committed.
	/// NOTE: This should be called when composing the change feed entries for this transaction,
	/// which should be done immediately before the transaction commit.
	/// That is to keep other transactions commit delay(pessimistic) or conflict(optimistic) as less as possible.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get_timestamp<K>(&mut self, key: K) -> Result<VersionStamp, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Calculate the version key
		let key = key.encode()?;
		// Calculate the version number
		let ver = match self.get(key.clone(), None).await? {
			Some(prev) => VersionStamp::from_slice(prev.as_slice())?
				.next()
				.expect("exhausted all possible timestamps"),
			None => VersionStamp::from_u64(1),
		};
		// Store the timestamp to prevent other transactions from committing
		self.set(key, &ver.as_bytes(), None).await?;
		// Return the uint64 representation of the timestamp as the result
		Ok(ver)
	}

	/// Insert the versionstamped key into the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(ts_key = ts_key.sprint()))]
	async fn set_versionstamp<K, V>(
		&mut self,
		ts_key: K,
		prefix: K,
		suffix: K,
		val: V,
	) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
		V: Into<Val> + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TxReadonly);
		}
		// Continue with function logic
		let ts = self.get_timestamp(ts_key).await?;
		let mut k: Vec<u8> = prefix.encode()?;
		k.extend_from_slice(&ts.as_bytes());
		suffix.encode_into(&mut k)?;
		self.set(k, val, None).await
	}
}
