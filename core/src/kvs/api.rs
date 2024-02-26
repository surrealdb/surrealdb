use std::future::Future;
use std::ops::Range;

use crate::err::Error;
use crate::key::error::KeyCategory;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::Versionstamp;
pub trait Transaction {
	/// Check if closed
	fn closed(&self) -> bool;
	/// Cancel a transaction
	fn cancel(&mut self) -> impl Future<Output = Result<(), Error>>;
	/// Commit a transaction
	fn commit(&mut self) -> impl Future<Output = Result<(), Error>>;
	/// Check if a key exists
	fn exi<K>(&mut self, key: K) -> impl Future<Output = Result<bool, Error>>
	where
		K: Into<Key>;
	/// Fetch a key from the database
	fn get<K>(&mut self, key: K) -> impl Future<Output = Result<Option<Val>, Error>>
	where
		K: Into<Key>;
	/// Insert or update a key in the database
	fn set<K, V>(&mut self, key: K, val: V) -> impl Future<Output = Result<(), Error>>
	where
		K: Into<Key>,
		V: Into<Val>;
	/// Insert a key if it doesn't exist in the database
	fn put<K, V>(
		&mut self,
		category: KeyCategory,
		key: K,
		val: V,
	) -> impl Future<Output = Result<(), Error>>
	where
		K: Into<Key>,
		V: Into<Val>;
	/// Insert a key if it doesn't exist in the database
	fn putc<K, V>(
		&mut self,
		key: K,
		val: V,
		chk: Option<V>,
	) -> impl Future<Output = Result<(), Error>>
	where
		K: Into<Key>,
		V: Into<Val>;
	/// Delete a key
	fn del<K>(&mut self, key: K) -> impl Future<Output = Result<(), Error>>
	where
		K: Into<Key>;
	/// Delete a key
	fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> impl Future<Output = Result<(), Error>>
	where
		K: Into<Key>,
		V: Into<Val>;

	#[allow(unused_variables)]
	fn delr<K>(&mut self, rng: Range<K>, limit: u32) -> impl Future<Output = Result<(), Error>>
	where
		K: Into<Key>,
	{
		async { Err(Error::Unimplemented(String::new())) }
	}
	/// Retrieve a range of keys from the databases
	fn scan<K>(
		&mut self,
		rng: Range<K>,
		limit: u32,
	) -> impl Future<Output = Result<Vec<(Key, Val)>, Error>>
	where
		K: Into<Key>;
	fn get_timestamp<K>(
		&mut self,
		key: K,
		lock: bool,
	) -> impl Future<Output = Result<Versionstamp, Error>>
	where
		K: Into<Key>;
	fn set_versionstamped_key<K, V>(
		&mut self,
		ts_key: K,
		prefix: K,
		suffix: K,
		val: V,
	) -> impl Future<Output = Result<(), Error>>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		async {
			let k = self.get_versionstamped_key(ts_key, prefix, suffix).await?;
			self.set(k, val).await
		}
	}
	#[allow(unused_variables)]
	fn get_versionstamped_key<K>(
		&mut self,
		ts_key: K,
		prefix: K,
		suffix: K,
	) -> impl Future<Output = Result<Key, Error>>
	where
		K: Into<Key>,
	{
		async { Err(Error::Unimplemented(String::new())) }
	}
}
