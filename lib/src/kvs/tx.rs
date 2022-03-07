use super::Transaction;
use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use std::ops::Range;

trait Add<T> {
	fn add(self, v: T) -> Self;
}

impl Add<u8> for Vec<u8> {
	fn add(mut self, v: u8) -> Self {
		self.push(v);
		self
	}
}

impl Transaction {
	// Check if closed
	pub async fn closed(&self) -> bool {
		match self {
			Transaction::Mock => unreachable!(),
			#[cfg(feature = "kv-echodb")]
			Transaction::Mem(v) => v.closed(),
			#[cfg(feature = "kv-indxdb")]
			Transaction::IxDB(v) => v.closed(),
			#[cfg(feature = "kv-yokudb")]
			Transaction::File(v) => v.closed(),
			#[cfg(feature = "kv-tikv")]
			Transaction::TiKV(v) => v.closed().await,
		}
	}
	// Cancel a transaction
	pub async fn cancel(&mut self) -> Result<(), Error> {
		match self {
			Transaction::Mock => unreachable!(),
			#[cfg(feature = "kv-echodb")]
			Transaction::Mem(v) => v.cancel(),
			#[cfg(feature = "kv-indxdb")]
			Transaction::IxDB(v) => v.cancel().await,
			#[cfg(feature = "kv-yokudb")]
			Transaction::File(v) => v.cancel(),
			#[cfg(feature = "kv-tikv")]
			Transaction::TiKV(v) => v.cancel().await,
		}
	}
	// Commit a transaction
	pub async fn commit(&mut self) -> Result<(), Error> {
		match self {
			Transaction::Mock => unreachable!(),
			#[cfg(feature = "kv-echodb")]
			Transaction::Mem(v) => v.commit(),
			#[cfg(feature = "kv-indxdb")]
			Transaction::IxDB(v) => v.commit().await,
			#[cfg(feature = "kv-yokudb")]
			Transaction::File(v) => v.commit(),
			#[cfg(feature = "kv-tikv")]
			Transaction::TiKV(v) => v.commit().await,
		}
	}
	// Delete a key
	pub async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		match self {
			Transaction::Mock => unreachable!(),
			#[cfg(feature = "kv-echodb")]
			Transaction::Mem(v) => v.del(key),
			#[cfg(feature = "kv-indxdb")]
			Transaction::IxDB(v) => v.del(key).await,
			#[cfg(feature = "kv-yokudb")]
			Transaction::File(v) => v.del(key),
			#[cfg(feature = "kv-tikv")]
			Transaction::TiKV(v) => v.del(key).await,
		}
	}
	// Check if a key exists
	pub async fn exi<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key>,
	{
		match self {
			Transaction::Mock => unreachable!(),
			#[cfg(feature = "kv-echodb")]
			Transaction::Mem(v) => v.exi(key),
			#[cfg(feature = "kv-indxdb")]
			Transaction::IxDB(v) => v.exi(key).await,
			#[cfg(feature = "kv-yokudb")]
			Transaction::File(v) => v.exi(key),
			#[cfg(feature = "kv-tikv")]
			Transaction::TiKV(v) => v.exi(key).await,
		}
	}
	// Fetch a key from the database
	pub async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<Key>,
	{
		match self {
			Transaction::Mock => unreachable!(),
			#[cfg(feature = "kv-echodb")]
			Transaction::Mem(v) => v.get(key),
			#[cfg(feature = "kv-indxdb")]
			Transaction::IxDB(v) => v.get(key).await,
			#[cfg(feature = "kv-yokudb")]
			Transaction::File(v) => v.get(key),
			#[cfg(feature = "kv-tikv")]
			Transaction::TiKV(v) => v.get(key).await,
		}
	}
	// Insert or update a key in the database
	pub async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Key>,
	{
		match self {
			Transaction::Mock => unreachable!(),
			#[cfg(feature = "kv-echodb")]
			Transaction::Mem(v) => v.set(key, val),
			#[cfg(feature = "kv-indxdb")]
			Transaction::IxDB(v) => v.set(key, val).await,
			#[cfg(feature = "kv-yokudb")]
			Transaction::File(v) => v.set(key, val),
			#[cfg(feature = "kv-tikv")]
			Transaction::TiKV(v) => v.set(key, val).await,
		}
	}
	// Insert a key if it doesn't exist in the database
	pub async fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Key>,
	{
		match self {
			Transaction::Mock => unreachable!(),
			#[cfg(feature = "kv-echodb")]
			Transaction::Mem(v) => v.put(key, val),
			#[cfg(feature = "kv-indxdb")]
			Transaction::IxDB(v) => v.put(key, val).await,
			#[cfg(feature = "kv-yokudb")]
			Transaction::File(v) => v.put(key, val),
			#[cfg(feature = "kv-tikv")]
			Transaction::TiKV(v) => v.put(key, val).await,
		}
	}
	// Retrieve a range of keys from the databases
	pub async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		match self {
			Transaction::Mock => unreachable!(),
			#[cfg(feature = "kv-echodb")]
			Transaction::Mem(v) => v.scan(rng, limit),
			#[cfg(feature = "kv-indxdb")]
			Transaction::IxDB(v) => v.scan(rng, limit).await,
			#[cfg(feature = "kv-yokudb")]
			Transaction::File(v) => v.scan(rng, limit),
			#[cfg(feature = "kv-tikv")]
			Transaction::TiKV(v) => v.scan(rng, limit).await,
		}
	}
	// Retrieve a range of keys from the databases
	pub async fn getr<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		let beg: Key = rng.start.into();
		let end: Key = rng.end.into();
		let mut nxt: Option<Key> = None;
		let mut num = limit;
		let mut out: Vec<(Key, Val)> = vec![];
		// Start processing
		while num > 0 {
			// Get records batch
			let res = match nxt {
				None => {
					let min = beg.clone();
					let max = end.clone();
					let num = std::cmp::min(1000, num);
					self.scan(min..max, num).await?
				}
				Some(ref mut beg) => {
					beg.push(0);
					let min = beg.clone();
					let max = end.clone();
					let num = std::cmp::min(1000, num);
					self.scan(min..max, num).await?
				}
			};
			// Get total results
			let n = res.len() - 1;
			// Loop over results
			for (i, (k, v)) in res.into_iter().enumerate() {
				// Ready the next
				if i == n {
					nxt = Some(k.clone());
				}
				// Delete
				out.push((k, v));
				// Count
				num -= 1;
			}
		}
		Ok(out)
	}
	// Delete a range of keys from the databases
	pub async fn delr<K>(&mut self, rng: Range<K>, limit: u32) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		let beg: Key = rng.start.into();
		let end: Key = rng.end.into();
		let mut nxt: Option<Key> = None;
		let mut num = limit;
		// Start processing
		while num > 0 {
			// Get records batch
			let res = match nxt {
				None => {
					let min = beg.clone();
					let max = end.clone();
					let num = std::cmp::min(1000, num);
					self.scan(min..max, num).await?
				}
				Some(ref mut beg) => {
					beg.push(0);
					let min = beg.clone();
					let max = end.clone();
					let num = std::cmp::min(1000, num);
					self.scan(min..max, num).await?
				}
			};
			// Get total results
			let n = res.len() - 1;
			// Loop over results
			for (i, (k, _)) in res.into_iter().enumerate() {
				// Ready the next
				if i == n {
					nxt = Some(k.clone());
				}
				// Delete
				self.del(k).await?;
				// Count
				num -= 1;
			}
		}
		Ok(())
	}
	// Retrieve a prefix of keys from the databases
	pub async fn getp<K>(&mut self, key: K, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		let beg: Key = key.into();
		let end: Key = beg.clone().add(255);
		let mut nxt: Option<Key> = None;
		let mut num = limit;
		let mut out: Vec<(Key, Val)> = vec![];
		// Start processing
		while num > 0 {
			// Get records batch
			let res = match nxt {
				None => {
					let min = beg.clone();
					let max = end.clone();
					let num = std::cmp::min(1000, num);
					self.scan(min..max, num).await?
				}
				Some(ref mut beg) => {
					beg.push(0);
					let min = beg.clone();
					let max = end.clone();
					let num = std::cmp::min(1000, num);
					self.scan(min..max, num).await?
				}
			};
			// Get total results
			let n = res.len() - 1;
			// Loop over results
			for (i, (k, v)) in res.into_iter().enumerate() {
				// Ready the next
				if i == n {
					nxt = Some(k.clone());
				}
				// Delete
				out.push((k, v));
				// Count
				num -= 1;
			}
		}
		Ok(out)
	}
	// Delete a prefix of keys from the databases
	pub async fn delp<K>(&mut self, key: K, limit: u32) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		let beg: Key = key.into();
		let end: Key = beg.clone().add(255);
		let mut nxt: Option<Key> = None;
		let mut num = limit;
		// Start processing
		while num > 0 {
			// Get records batch
			let res = match nxt {
				None => {
					let min = beg.clone();
					let max = end.clone();
					let num = std::cmp::min(1000, num);
					self.scan(min..max, num).await?
				}
				Some(ref mut beg) => {
					beg.push(0);
					let min = beg.clone();
					let max = end.clone();
					let num = std::cmp::min(1000, num);
					self.scan(min..max, num).await?
				}
			};
			// Get total results
			let n = res.len() - 1;
			// Loop over results
			for (i, (k, _)) in res.into_iter().enumerate() {
				// Ready the next
				if i == n {
					nxt = Some(k.clone());
				}
				// Delete
				self.del(k).await?;
				// Count
				num -= 1;
			}
		}
		Ok(())
	}
}
