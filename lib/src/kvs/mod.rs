mod ex;
mod file;
mod ixdb;
mod kv;
mod mem;
mod tikv;
mod tx;

pub use self::kv::*;
pub use self::tx::*;

use crate::err::Error;
use std::sync::Arc;

pub type Store = Arc<Datastore>;

pub enum Datastore {
	Mock,
	#[cfg(feature = "kv-echodb")]
	Mem(mem::Datastore),
	#[cfg(feature = "kv-indxdb")]
	IxDB(ixdb::Datastore),
	#[cfg(feature = "kv-yokudb")]
	File(file::Datastore),
	#[cfg(feature = "kv-tikv")]
	TiKV(tikv::Datastore),
}

pub enum Transaction {
	Mock,
	#[cfg(feature = "kv-echodb")]
	Mem(mem::Transaction),
	#[cfg(feature = "kv-indxdb")]
	IxDB(ixdb::Transaction),
	#[cfg(feature = "kv-yokudb")]
	File(file::Transaction),
	#[cfg(feature = "kv-tikv")]
	TiKV(tikv::Transaction),
}

impl Datastore {
	// Create a new datastore
	pub async fn new(path: &str) -> Result<Self, Error> {
		match path {
			#[cfg(feature = "kv-echodb")]
			"memory" => {
				info!("Starting kvs store in {}", path);
				mem::Datastore::new().await.map(Datastore::Mem)
			}
			// Parse and initiate an IxDB database
			#[cfg(feature = "kv-indxdb")]
			s if s.starts_with("ixdb:") => {
				info!("Starting kvs store at {}", path);
				let s = s.trim_start_matches("ixdb://");
				ixdb::Datastore::new(s).await.map(Datastore::IxDB)
			}
			// Parse and initiate an File database
			#[cfg(feature = "kv-yokudb")]
			s if s.starts_with("file:") => {
				info!("Starting kvs store at {}", path);
				let s = s.trim_start_matches("file://");
				file::Datastore::new(s).await.map(Datastore::File)
			}
			// Parse and initiate an TiKV database
			#[cfg(feature = "kv-tikv")]
			s if s.starts_with("tikv:") => {
				info!("Starting kvs store at {}", path);
				let s = s.trim_start_matches("tikv://");
				tikv::Datastore::new(s).await.map(Datastore::TiKV)
			}
			// The datastore path is not valid
			_ => unreachable!(),
		}
	}
	// Create a new transaction
	pub async fn transaction(&self, write: bool, lock: bool) -> Result<Transaction, Error> {
		match self {
			Datastore::Mock => {
				let tx = Transaction::Mock;
				Ok(tx)
			}
			#[cfg(feature = "kv-echodb")]
			Datastore::Mem(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction::Mem(tx))
			}
			#[cfg(feature = "kv-indxdb")]
			Datastore::IxDB(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction::IxDB(tx))
			}
			#[cfg(feature = "kv-yokudb")]
			Datastore::File(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction::File(tx))
			}
			#[cfg(feature = "kv-tikv")]
			Datastore::TiKV(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction::TiKV(tx))
			}
		}
	}
}
