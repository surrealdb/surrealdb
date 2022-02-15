mod file;
mod kv;
mod mem;
mod tikv;
mod tx;

pub use self::kv::*;
pub use self::tx::*;

use crate::err::Error;
use once_cell::sync::OnceCell;

pub enum Datastore {
	Mock,
	Mem(mem::Datastore),
	File(file::Datastore),
	TiKV(tikv::Datastore),
}

pub enum Transaction<'a> {
	Mock,
	Mem(mem::Transaction<'a>),
	File(file::Transaction<'a>),
	TiKV(tikv::Transaction),
}

static DB: OnceCell<Datastore> = OnceCell::new();

pub fn init(path: &str) -> Result<(), Error> {
	// Instantiate the database endpoint
	match path {
		"memory" => {
			info!("Starting kvs store in {}", path);
			let ds = mem::Datastore::new()?;
			let _ = DB.set(Datastore::Mem(ds));
			Ok(())
		}
		s if s.starts_with("file:") => {
			info!("Starting kvs store at {}", path);
			let s = s.trim_start_matches("file://");
			let ds = file::Datastore::new(s)?;
			let _ = DB.set(Datastore::File(ds));
			Ok(())
		}
		s if s.starts_with("tikv:") => {
			info!("Starting kvs store at {}", path);
			let s = s.trim_start_matches("tikv://");
			let ds = tikv::Datastore::new(s)?;
			let _ = DB.set(Datastore::TiKV(ds));
			Ok(())
		}
		_ => unreachable!(),
	}
}

pub async fn transaction<'a>(write: bool, lock: bool) -> Result<Transaction<'a>, Error> {
	match DB.get().unwrap() {
		Datastore::Mock => {
			let tx = Transaction::Mock;
			Ok(tx)
		}
		Datastore::Mem(v) => {
			let tx = v.transaction(write, lock)?;
			Ok(Transaction::Mem(tx))
		}
		Datastore::File(v) => {
			let tx = v.transaction(write, lock)?;
			Ok(Transaction::File(tx))
		}
		Datastore::TiKV(v) => {
			let tx = v.transaction(write, lock).await?;
			Ok(Transaction::TiKV(tx))
		}
	}
}
