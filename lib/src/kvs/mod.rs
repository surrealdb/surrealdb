mod cache;
mod ds;
mod kv;
mod tx;

#[cfg(test)]
mod tests;

pub use self::ds::*;
pub use self::kv::*;
pub use self::tx::*;
use crate::err::Error;
use async_trait_fn::async_trait;
use std::ops::Range;

pub(crate) const LOG: &str = "surrealdb::kvs";

#[async_trait]
pub trait DatastoreMetadata {
	async fn new(&self, path: &str) -> Result<Box<dyn DatastoreFacade + Send + Sync>, Error>;
	fn name(&self) -> &'static str;
	fn scheme(&self) -> &'static [&'static str];
	fn connection_string_match_prefix(&self, url: &str) -> bool {
		self.scheme().iter().any(|&x| url.starts_with(&format!("{x}:")))
	}
	fn trim_connection_string(&self, url: &str) -> String;
}

#[async_trait]
pub trait DatastoreFacade {
	/// Start a new transaction
	async fn transaction(
		&self,
		write: bool,
		lock: bool,
	) -> Result<Box<dyn TransactionFacade + Send + Sync>, Error>;
}

#[async_trait]
pub trait TransactionFacade {
	/// Check if closed
	fn closed(&self) -> bool;
	/// Cancel a transaction
	async fn cancel(&mut self) -> Result<(), Error>;
	/// Commit a transaction
	async fn commit(&mut self) -> Result<(), Error>;
	/// Check if a key exists
	async fn exi(&mut self, key: Key) -> Result<bool, Error>;
	/// Fetch a key from the database
	async fn get(&mut self, key: Key) -> Result<Option<Val>, Error>;
	/// Insert or update a key in the database
	async fn set(&mut self, key: Key, val: Val) -> Result<(), Error>;
	/// Insert a key if it doesn't exist in the database
	async fn put(&mut self, key: Key, val: Val) -> Result<(), Error>;
	/// Insert a key if it doesn't exist in the database
	async fn putc(&mut self, key: Key, val: Val, chk: Option<Val>) -> Result<(), Error>;
	/// Delete a key
	async fn del(&mut self, key: Key) -> Result<(), Error>;
	/// Delete a key
	async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<(), Error>;
	/// Retrieve a range of keys from the databases
	async fn scan(&mut self, rng: Range<Key>, limit: u32) -> Result<Vec<(Key, Val)>, Error>;
}

#[cfg(feature = "kv-fdb")]
mod fdb;
#[cfg(feature = "kv-indxdb")]
mod indxdb;
#[cfg(feature = "kv-mem")]
mod mem;
#[cfg(feature = "kv-rocksdb")]
mod rocksdb;
#[cfg(feature = "kv-tikv")]
mod tikv;

pub static AVAILABLE_DATASTORE_METADATA: &'static [&(dyn DatastoreMetadata + Send + Sync)] = &[
	#[cfg(feature = "kv-mem")]
	&mem::MemoryDatastoreMetadata,
	#[cfg(feature = "kv-rocksdb")]
	&rocksdb::RocksDbDatastoreMetadata,
	#[cfg(feature = "kv-indxdb")]
	&indxdb::IndexDbDatastoreMetadata,
	#[cfg(feature = "kv-tikv")]
	&tikv::TikvDatastoreMetadata,
	#[cfg(feature = "kv-fdb")]
	&fdb::FoundationDbDatastoreMetadata,
];
