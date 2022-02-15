use super::Transaction;
use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use std::ops::Range;

impl<'a> Transaction<'a> {
	// Check if closed
	pub async fn closed(&self) -> bool {
		match self {
			Transaction::Mock => unreachable!(),
			Transaction::Mem(v) => v.closed(),
			Transaction::File(v) => v.closed(),
			Transaction::TiKV(v) => v.closed().await,
		}
	}
	// Cancel a transaction
	pub async fn cancel(&mut self) -> Result<(), Error> {
		match self {
			Transaction::Mock => unreachable!(),
			Transaction::Mem(v) => v.cancel(),
			Transaction::File(v) => v.cancel(),
			Transaction::TiKV(v) => v.cancel().await,
		}
	}
	// Commit a transaction
	pub async fn commit(&mut self) -> Result<(), Error> {
		match self {
			Transaction::Mock => unreachable!(),
			Transaction::Mem(v) => v.commit(),
			Transaction::File(v) => v.commit(),
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
			Transaction::Mem(v) => v.del(key),
			Transaction::File(v) => v.del(key),
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
			Transaction::Mem(v) => v.exi(key),
			Transaction::File(v) => v.exi(key),
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
			Transaction::Mem(v) => v.get(key),
			Transaction::File(v) => v.get(key),
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
			Transaction::Mem(v) => v.set(key, val),
			Transaction::File(v) => v.set(key, val),
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
			Transaction::Mem(v) => v.put(key, val),
			Transaction::File(v) => v.put(key, val),
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
			Transaction::Mem(v) => v.scan(rng, limit),
			Transaction::File(v) => v.scan(rng, limit),
			Transaction::TiKV(v) => v.scan(rng, limit).await,
		}
	}
}
