use super::Transaction;
use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use std::ops::Range;

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
}
