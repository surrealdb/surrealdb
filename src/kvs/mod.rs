mod file;
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
	Mem(mem::Datastore),
	File(file::Datastore),
	TiKV(tikv::Datastore),
}

pub enum Transaction {
	Mock,
	Mem(mem::Transaction),
	File(file::Transaction),
	TiKV(tikv::Transaction),
}

impl Datastore {
	// Create a new datastore
	pub async fn new(path: &str) -> Result<Self, Error> {
		match path {
			"memory" => {
				info!("Starting kvs store in {}", path);
				mem::Datastore::new().await.map(Datastore::Mem)
			}
			// Parse and initiate an File database
			#[cfg(not(target_arch = "wasm32"))]
			s if s.starts_with("file:") => {
				info!("Starting kvs store at {}", path);
				let s = s.trim_start_matches("tikv://");
				file::Datastore::new(s).await.map(Datastore::File)
			}
			// Parse and initiate an TiKV database
			#[cfg(not(target_arch = "wasm32"))]
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
			Datastore::Mem(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction::Mem(tx))
			}
			#[cfg(not(target_arch = "wasm32"))]
			Datastore::File(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction::File(tx))
			}
			#[cfg(not(target_arch = "wasm32"))]
			Datastore::TiKV(v) => {
				let tx = v.transaction(write, lock).await?;
				Ok(Transaction::TiKV(tx))
			}
		}
	}
}
