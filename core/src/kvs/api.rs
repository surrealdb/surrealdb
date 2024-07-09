use super::kv::Add;
use crate::cnf::NORMAL_FETCH_SIZE;
use crate::err::Error;
use crate::kvs::batch::Batch;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::Versionstamp;
use std::fmt::Debug;
use std::ops::Range;

pub trait Transaction {
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
	async fn exists<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key> + Debug;

	/// Fetch a key from the datastore.
	async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<Key> + Debug;

	/// Insert or update a key in the datastore.
	async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug;

	/// Insert a key if it doesn't exist in the datastore.
	async fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug;

	/// Update a key in the datastore if the current value matches a condition.
	async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug;

	/// Delete a key from the datastore.
	async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key> + Debug;

	/// Delete a key from the datastore if the current value matches a condition.
	async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug;

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys without values, in a single request to the underlying datastore.
	async fn keys<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<Key>, Error>
	where
		K: Into<Key> + Debug;

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single request to the underlying datastore.
	async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Debug;

	/// Fetch many keys from the datastore.
	///
	/// This function fetches all matching keys pairs from the underlying datastore concurrently.
	async fn getm<K>(&mut self, keys: Vec<K>) -> Result<Vec<Val>, Error>
	where
		K: Into<Key> + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Continue with function logic
		let mut out = Vec::with_capacity(keys.len());
		for key in keys.into_iter() {
			if let Some(val) = self.get(key).await? {
				out.push(val);
			} else {
				out.push(vec![]);
			}
		}
		Ok(out)
	}

	/// Retrieve a range of prefixed keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying datastore in grouped batches.
	async fn getp<K>(&mut self, key: K) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Continue with function logic
		let beg: Key = key.into();
		let end: Key = beg.clone().add(0xff);
		self.getr(beg..end).await
	}

	/// Retrieve a range of keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying datastore in grouped batches.
	async fn getr<K>(&mut self, rng: Range<K>) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Continue with function logic
		let mut out = vec![];
		let beg: Key = rng.start.into();
		let end: Key = rng.end.into();
		let mut next = Some(beg..end);
		while let Some(rng) = next {
			let res = self.batch(rng, *NORMAL_FETCH_SIZE, true).await?;
			next = res.next;
			for v in res.values.into_iter() {
				out.push(v);
			}
		}
		Ok(out)
	}

	/// Delete a range of prefixed keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying datastore in grouped batches.
	async fn delp<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
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
		let beg: Key = key.into();
		let end: Key = beg.clone().add(0xff);
		self.delr(beg..end).await
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying datastore in grouped batches.
	async fn delr<K>(&mut self, rng: Range<K>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
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
		let beg: Key = rng.start.into();
		let end: Key = rng.end.into();
		let mut next = Some(beg..end);
		while let Some(rng) = next {
			let res = self.batch(rng, *NORMAL_FETCH_SIZE, false).await?;
			next = res.next;
			for (k, _) in res.values.into_iter() {
				self.del(k).await?;
			}
		}
		Ok(())
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches keys or key-value pairs, in batches, with multiple requests to the underlying datastore.
	async fn batch<K>(&mut self, rng: Range<K>, batch: u32, values: bool) -> Result<Batch, Error>
	where
		K: Into<Key> + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Continue with function logic
		let beg: Key = rng.start.into();
		let end: Key = rng.end.into();
		// Scan for the next batch
		let res = if values {
			self.scan(beg..end.clone(), batch).await?
		} else {
			self.keys(beg..end.clone(), batch)
				.await?
				.into_iter()
				.map(|k| (k, vec![]))
				.collect::<Vec<(Key, Val)>>()
		};
		// Check if range is consumed
		if res.len() < batch as usize && batch > 0 {
			Ok(Batch {
				next: None,
				values: res,
			})
		} else {
			match res.last() {
				Some((k, _)) => Ok(Batch {
					next: Some(Range {
						start: k.clone().add(0x00),
						end,
					}),
					values: res,
				}),
				// We have checked the length above,
				// so there is guaranteed to always
				// be a last item in the vector.
				// This is therefore unreachable.
				None => unreachable!(),
			}
		}
	}

	/// Obtain a new change timestamp for a key
	/// which is replaced with the current timestamp when the transaction is committed.
	/// NOTE: This should be called when composing the change feed entries for this transaction,
	/// which should be done immediately before the transaction commit.
	/// That is to keep other transactions commit delay(pessimistic) or conflict(optimistic) as less as possible.
	async fn get_timestamp<K>(&mut self, key: K) -> Result<Versionstamp, Error>
	where
		K: Into<Key> + Debug,
	{
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TxFinished);
		}
		// Calculate the version key
		let key = key.into();
		// Calculate the version number
		let ver = match self.get(key.as_slice()).await? {
			Some(prev) => {
				let res: Result<[u8; 10], Error> = match prev.as_slice().try_into() {
					Ok(ba) => Ok(ba),
					Err(e) => Err(Error::Tx(e.to_string())),
				};
				crate::vs::try_to_u64_be(res?)? + 1
			}
			None => 1,
		};
		// Convert the timestamp to a versionstamp
		let verbytes = crate::vs::u64_to_versionstamp(ver);
		// Store the timestamp to prevent other transactions from committing
		self.set(key.as_slice(), verbytes.to_vec()).await?;
		// Return the uint64 representation of the timestamp as the result
		Ok(verbytes)
	}

	/// Insert the versionstamped key into the datastore.
	async fn set_versionstamp<K, V>(
		&mut self,
		ts_key: K,
		prefix: K,
		suffix: K,
		val: V,
	) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
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
		let mut k: Vec<u8> = prefix.into();
		k.append(&mut ts.to_vec());
		k.append(&mut suffix.into());
		self.set(k, val).await
	}
}
