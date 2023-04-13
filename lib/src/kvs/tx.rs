use super::kv::Add;
use super::kv::Convert;
use super::Key;
use super::Val;
use crate::err::Error;
use crate::key::thing;
use crate::kvs::cache::Cache;
use crate::kvs::cache::Entry;
use crate::sql;
use crate::sql::paths::EDGE;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::thing::Thing;
use crate::sql::Value;
use channel::Sender;
use sql::permission::Permissions;
use sql::statements::DefineDatabaseStatement;
use sql::statements::DefineEventStatement;
use sql::statements::DefineFieldStatement;
use sql::statements::DefineFunctionStatement;
use sql::statements::DefineIndexStatement;
use sql::statements::DefineLoginStatement;
use sql::statements::DefineNamespaceStatement;
use sql::statements::DefineParamStatement;
use sql::statements::DefineScopeStatement;
use sql::statements::DefineTableStatement;
use sql::statements::DefineTokenStatement;
use sql::statements::LiveStatement;
use std::fmt;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

#[cfg(debug_assertions)]
const LOG: &str = "surrealdb::txn";

/// A set of undoable updates and requests against a dataset.
#[allow(dead_code)]
pub struct Transaction {
	pub(super) inner: Inner,
	pub(super) cache: Cache,
}

#[allow(clippy::large_enum_variant)]
pub(super) enum Inner {
	#[cfg(feature = "kv-mem")]
	Mem(super::mem::Transaction),
	#[cfg(feature = "kv-rocksdb")]
	RocksDB(super::rocksdb::Transaction),
	#[cfg(feature = "kv-indxdb")]
	IndxDB(super::indxdb::Transaction),
	#[cfg(feature = "kv-tikv")]
	TiKV(super::tikv::Transaction),
	#[cfg(feature = "kv-fdb")]
	FDB(super::fdb::Transaction),
}

impl fmt::Display for Transaction {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		#![allow(unused_variables)]
		match &self.inner {
			#[cfg(feature = "kv-mem")]
			Inner::Mem(_) => write!(f, "memory"),
			#[cfg(feature = "kv-rocksdb")]
			Inner::RocksDB(_) => write!(f, "rocksdb"),
			#[cfg(feature = "kv-indxdb")]
			Inner::IndxDB(_) => write!(f, "indexdb"),
			#[cfg(feature = "kv-tikv")]
			Inner::TiKV(_) => write!(f, "tikv"),
			#[cfg(feature = "kv-fdb")]
			Inner::FDB(_) => write!(f, "fdb"),
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}
}

impl Transaction {
	// --------------------------------------------------
	// Integral methods
	// --------------------------------------------------

	/// Check if transactions is finished.
	///
	/// If the transaction has been cancelled or committed,
	/// then this function will return [`true`], and any further
	/// calls to functions on this transaction will result
	/// in a [`Error::TxFinished`] error.
	pub async fn closed(&self) -> bool {
		#[cfg(debug_assertions)]
		trace!(target: LOG, "Closed");
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => v.closed(),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => v.closed(),
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.closed(),
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.closed(),
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FDB(v),
				..
			} => v.closed(),
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Cancel a transaction.
	///
	/// This reverses all changes made within the transaction.
	pub async fn cancel(&mut self) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		trace!(target: LOG, "Cancel");
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => v.cancel(),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => v.cancel().await,
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.cancel().await,
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.cancel().await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FDB(v),
				..
			} => v.cancel().await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	pub async fn commit(&mut self) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		trace!(target: LOG, "Commit");
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => v.commit(),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => v.commit().await,
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.commit().await,
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.commit().await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FDB(v),
				..
			} => v.commit().await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Delete a key from the datastore.
	#[allow(unused_variables)]
	pub async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		#[cfg(debug_assertions)]
		trace!(target: LOG, "Del {:?}", key);
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => v.del(key),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => v.del(key).await,
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.del(key).await,
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.del(key).await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FDB(v),
				..
			} => v.del(key).await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Check if a key exists in the datastore.
	#[allow(unused_variables)]
	pub async fn exi<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key> + Debug,
	{
		#[cfg(debug_assertions)]
		trace!(target: LOG, "Exi {:?}", key);
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => v.exi(key),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => v.exi(key).await,
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.exi(key).await,
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.exi(key).await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FDB(v),
				..
			} => v.exi(key).await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Fetch a key from the datastore.
	#[allow(unused_variables)]
	pub async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<Key> + Debug,
	{
		#[cfg(debug_assertions)]
		trace!(target: LOG, "Get {:?}", key);
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => v.get(key),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => v.get(key).await,
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.get(key).await,
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.get(key).await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FDB(v),
				..
			} => v.get(key).await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Insert or update a key in the datastore.
	#[allow(unused_variables)]
	pub async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		#[cfg(debug_assertions)]
		trace!(target: LOG, "Set {:?} => {:?}", key, val);
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => v.set(key, val),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => v.set(key, val).await,
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.set(key, val).await,
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.set(key, val).await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FDB(v),
				..
			} => v.set(key, val).await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[allow(unused_variables)]
	pub async fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		#[cfg(debug_assertions)]
		trace!(target: LOG, "Put {:?} => {:?}", key, val);
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => v.put(key, val),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => v.put(key, val).await,
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.put(key, val).await,
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.put(key, val).await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FDB(v),
				..
			} => v.put(key, val).await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single request to the underlying datastore.
	#[allow(unused_variables)]
	pub async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Debug,
	{
		#[cfg(debug_assertions)]
		trace!(target: LOG, "Scan {:?} - {:?}", rng.start, rng.end);
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => v.scan(rng, limit),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => v.scan(rng, limit).await,
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.scan(rng, limit).await,
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.scan(rng, limit).await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FDB(v),
				..
			} => v.scan(rng, limit).await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Update a key in the datastore if the current value matches a condition.
	#[allow(unused_variables)]
	pub async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		#[cfg(debug_assertions)]
		trace!(target: LOG, "Putc {:?} if {:?} => {:?}", key, chk, val);
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => v.putc(key, val, chk),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => v.putc(key, val, chk).await,
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.putc(key, val, chk).await,
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.putc(key, val, chk).await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FDB(v),
				..
			} => v.putc(key, val, chk).await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Delete a key from the datastore if the current value matches a condition.
	#[allow(unused_variables)]
	pub async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		#[cfg(debug_assertions)]
		trace!(target: LOG, "Delc {:?} if {:?}", key, chk);
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => v.delc(key, chk),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => v.delc(key, chk).await,
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.delc(key, chk).await,
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.delc(key, chk).await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FDB(v),
				..
			} => v.delc(key, chk).await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	// --------------------------------------------------
	// Superjacent methods
	// --------------------------------------------------

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in batches of 1000.
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
					beg.push(0x00);
					let min = beg.clone();
					let max = end.clone();
					let num = std::cmp::min(1000, num);
					self.scan(min..max, num).await?
				}
			};
			// Get total results
			let n = res.len();
			// Exit when settled
			if n == 0 {
				break;
			}
			// Loop over results
			for (i, (k, v)) in res.into_iter().enumerate() {
				// Ready the next
				if n == i + 1 {
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
	/// Delete a range of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in batches of 1000.
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
					beg.push(0x00);
					let min = beg.clone();
					let max = end.clone();
					let num = std::cmp::min(1000, num);
					self.scan(min..max, num).await?
				}
			};
			// Get total results
			let n = res.len();
			// Exit when settled
			if n == 0 {
				break;
			}
			// Loop over results
			for (i, (k, _)) in res.into_iter().enumerate() {
				// Ready the next
				if n == i + 1 {
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
	/// Retrieve a specific prefix of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in batches of 1000.
	pub async fn getp<K>(&mut self, key: K, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		let beg: Key = key.into();
		let end: Key = beg.clone().add(0xff);
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
			let n = res.len();
			// Exit when settled
			if n == 0 {
				break;
			}
			// Loop over results
			for (i, (k, v)) in res.into_iter().enumerate() {
				// Ready the next
				if n == i + 1 {
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
	/// Delete a prefix of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in batches of 1000.
	pub async fn delp<K>(&mut self, key: K, limit: u32) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		let beg: Key = key.into();
		let end: Key = beg.clone().add(0xff);
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
			let n = res.len();
			// Exit when settled
			if n == 0 {
				break;
			}
			// Loop over results
			for (i, (k, _)) in res.into_iter().enumerate() {
				// Ready the next
				if n == i + 1 {
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

	// --------------------------------------------------
	// Superimposed methods
	// --------------------------------------------------

	/// Clear any cache entry for the specified key.
	pub async fn clr<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		let key: Key = key.into();
		self.cache.del(&key);
		Ok(())
	}

	/// Retrieve all namespace definitions in a datastore.
	pub async fn all_ns(&mut self) -> Result<Arc<[DefineNamespaceStatement]>, Error> {
		let key = crate::key::ns::prefix();
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Nss(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::ns::prefix();
			let end = crate::key::ns::suffix();
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Nss(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all namespace login definitions for a specific namespace.
	pub async fn all_nl(&mut self, ns: &str) -> Result<Arc<[DefineLoginStatement]>, Error> {
		let key = crate::key::nl::prefix(ns);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Nls(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::nl::prefix(ns);
			let end = crate::key::nl::suffix(ns);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Nls(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all namespace token definitions for a specific namespace.
	pub async fn all_nt(&mut self, ns: &str) -> Result<Arc<[DefineTokenStatement]>, Error> {
		let key = crate::key::nt::prefix(ns);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Nts(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::nt::prefix(ns);
			let end = crate::key::nt::suffix(ns);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Nts(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all database definitions for a specific namespace.
	pub async fn all_db(&mut self, ns: &str) -> Result<Arc<[DefineDatabaseStatement]>, Error> {
		let key = crate::key::db::prefix(ns);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Dbs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::db::prefix(ns);
			let end = crate::key::db::suffix(ns);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Dbs(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all database login definitions for a specific database.
	pub async fn all_dl(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineLoginStatement]>, Error> {
		let key = crate::key::dl::prefix(ns, db);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Dls(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::dl::prefix(ns, db);
			let end = crate::key::dl::suffix(ns, db);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Dls(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all database token definitions for a specific database.
	pub async fn all_dt(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineTokenStatement]>, Error> {
		let key = crate::key::dt::prefix(ns, db);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Dts(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::dt::prefix(ns, db);
			let end = crate::key::dt::suffix(ns, db);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Dts(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all function definitions for a specific database.
	pub async fn all_fc(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineFunctionStatement]>, Error> {
		let key = crate::key::fc::prefix(ns, db);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Fcs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::fc::prefix(ns, db);
			let end = crate::key::fc::suffix(ns, db);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Fcs(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all scope definitions for a specific database.
	pub async fn all_sc(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineScopeStatement]>, Error> {
		let key = crate::key::sc::prefix(ns, db);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Scs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::sc::prefix(ns, db);
			let end = crate::key::sc::suffix(ns, db);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Scs(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all scope token definitions for a scope.
	pub async fn all_st(
		&mut self,
		ns: &str,
		db: &str,
		sc: &str,
	) -> Result<Arc<[DefineTokenStatement]>, Error> {
		let key = crate::key::st::prefix(ns, db, sc);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Sts(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::st::prefix(ns, db, sc);
			let end = crate::key::st::suffix(ns, db, sc);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Sts(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all scope definitions for a specific database.
	pub async fn all_pa(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineParamStatement]>, Error> {
		let key = crate::key::pa::prefix(ns, db);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Pas(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::pa::prefix(ns, db);
			let end = crate::key::pa::suffix(ns, db);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Pas(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all table definitions for a specific database.
	pub async fn all_tb(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineTableStatement]>, Error> {
		let key = crate::key::tb::prefix(ns, db);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Tbs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::tb::prefix(ns, db);
			let end = crate::key::tb::suffix(ns, db);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Tbs(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all event definitions for a specific table.
	pub async fn all_ev(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineEventStatement]>, Error> {
		let key = crate::key::ev::prefix(ns, db, tb);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Evs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::ev::prefix(ns, db, tb);
			let end = crate::key::ev::suffix(ns, db, tb);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Evs(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all field definitions for a specific table.
	pub async fn all_fd(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineFieldStatement]>, Error> {
		let key = crate::key::fd::prefix(ns, db, tb);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Fds(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::fd::prefix(ns, db, tb);
			let end = crate::key::fd::suffix(ns, db, tb);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Fds(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all index definitions for a specific table.
	pub async fn all_ix(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineIndexStatement]>, Error> {
		let key = crate::key::ix::prefix(ns, db, tb);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Ixs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::ix::prefix(ns, db, tb);
			let end = crate::key::ix::suffix(ns, db, tb);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Ixs(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all view definitions for a specific table.
	pub async fn all_ft(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineTableStatement]>, Error> {
		let key = crate::key::ft::prefix(ns, db, tb);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Fts(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::ft::prefix(ns, db, tb);
			let end = crate::key::ft::suffix(ns, db, tb);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Fts(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all live definitions for a specific table.
	pub async fn all_lv(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[LiveStatement]>, Error> {
		let key = crate::key::lv::prefix(ns, db, tb);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Lvs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::lv::prefix(ns, db, tb);
			let end = crate::key::lv::suffix(ns, db, tb);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Lvs(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve a specific namespace definition.
	pub async fn get_ns(&mut self, ns: &str) -> Result<DefineNamespaceStatement, Error> {
		let key = crate::key::ns::new(ns);
		let val = self.get(key).await?.ok_or(Error::NsNotFound {
			value: ns.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific namespace login definition.
	pub async fn get_nl(&mut self, ns: &str, nl: &str) -> Result<DefineLoginStatement, Error> {
		let key = crate::key::nl::new(ns, nl);
		let val = self.get(key).await?.ok_or(Error::NlNotFound {
			value: nl.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific namespace token definition.
	pub async fn get_nt(&mut self, ns: &str, nt: &str) -> Result<DefineTokenStatement, Error> {
		let key = crate::key::nt::new(ns, nt);
		let val = self.get(key).await?.ok_or(Error::NtNotFound {
			value: nt.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific database definition.
	pub async fn get_db(&mut self, ns: &str, db: &str) -> Result<DefineDatabaseStatement, Error> {
		let key = crate::key::db::new(ns, db);
		let val = self.get(key).await?.ok_or(Error::DbNotFound {
			value: db.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific database login definition.
	pub async fn get_dl(
		&mut self,
		ns: &str,
		db: &str,
		dl: &str,
	) -> Result<DefineLoginStatement, Error> {
		let key = crate::key::dl::new(ns, db, dl);
		let val = self.get(key).await?.ok_or(Error::DlNotFound {
			value: dl.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific database token definition.
	pub async fn get_dt(
		&mut self,
		ns: &str,
		db: &str,
		dt: &str,
	) -> Result<DefineTokenStatement, Error> {
		let key = crate::key::dt::new(ns, db, dt);
		let val = self.get(key).await?.ok_or(Error::DtNotFound {
			value: dt.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific scope definition.
	pub async fn get_sc(
		&mut self,
		ns: &str,
		db: &str,
		sc: &str,
	) -> Result<DefineScopeStatement, Error> {
		let key = crate::key::sc::new(ns, db, sc);
		let val = self.get(key).await?.ok_or(Error::ScNotFound {
			value: sc.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific scope token definition.
	pub async fn get_st(
		&mut self,
		ns: &str,
		db: &str,
		sc: &str,
		st: &str,
	) -> Result<DefineTokenStatement, Error> {
		let key = crate::key::st::new(ns, db, sc, st);
		let val = self.get(key).await?.ok_or(Error::StNotFound {
			value: st.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific function definition.
	pub async fn get_fc(
		&mut self,
		ns: &str,
		db: &str,
		fc: &str,
	) -> Result<DefineFunctionStatement, Error> {
		let key = crate::key::fc::new(ns, db, fc);
		let val = self.get(key).await?.ok_or(Error::FcNotFound {
			value: fc.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific param definition.
	pub async fn get_pa(
		&mut self,
		ns: &str,
		db: &str,
		pa: &str,
	) -> Result<DefineParamStatement, Error> {
		let key = crate::key::pa::new(ns, db, pa);
		let val = self.get(key).await?.ok_or(Error::PaNotFound {
			value: pa.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific table definition.
	pub async fn get_tb(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<DefineTableStatement, Error> {
		let key = crate::key::tb::new(ns, db, tb);
		let val = self.get(key).await?.ok_or(Error::TbNotFound {
			value: tb.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Add a namespace with a default configuration, only if we are in dynamic mode.
	pub async fn add_ns(
		&mut self,
		ns: &str,
		strict: bool,
	) -> Result<DefineNamespaceStatement, Error> {
		match self.get_ns(ns).await {
			Err(Error::NsNotFound {
				value,
			}) => match strict {
				false => {
					let key = crate::key::ns::new(ns);
					let val = DefineNamespaceStatement {
						name: ns.to_owned().into(),
					};
					self.put(key, &val).await?;
					Ok(val)
				}
				true => Err(Error::NsNotFound {
					value,
				}),
			},
			Err(e) => Err(e),
			Ok(v) => Ok(v),
		}
	}

	/// Add a database with a default configuration, only if we are in dynamic mode.
	pub async fn add_db(
		&mut self,
		ns: &str,
		db: &str,
		strict: bool,
	) -> Result<DefineDatabaseStatement, Error> {
		match self.get_db(ns, db).await {
			Err(Error::DbNotFound {
				value,
			}) => match strict {
				false => {
					let key = crate::key::db::new(ns, db);
					let val = DefineDatabaseStatement {
						name: db.to_owned().into(),
					};
					self.put(key, &val).await?;
					Ok(val)
				}
				true => Err(Error::DbNotFound {
					value,
				}),
			},
			Err(e) => Err(e),
			Ok(v) => Ok(v),
		}
	}

	/// Add a scope with a default configuration, only if we are in dynamic mode.
	pub async fn add_sc(
		&mut self,
		ns: &str,
		db: &str,
		sc: &str,
		strict: bool,
	) -> Result<DefineScopeStatement, Error> {
		match self.get_sc(ns, db, sc).await {
			Err(Error::ScNotFound {
				value,
			}) => match strict {
				false => {
					let key = crate::key::sc::new(ns, db, sc);
					let val = DefineScopeStatement {
						name: sc.to_owned().into(),
						..DefineScopeStatement::default()
					};
					self.put(key, &val).await?;
					Ok(val)
				}
				true => Err(Error::ScNotFound {
					value,
				}),
			},
			Err(e) => Err(e),
			Ok(v) => Ok(v),
		}
	}

	/// Add a table with a default configuration, only if we are in dynamic mode.
	pub async fn add_tb(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<DefineTableStatement, Error> {
		match self.get_tb(ns, db, tb).await {
			Err(Error::TbNotFound {
				value,
			}) => match strict {
				false => {
					let key = crate::key::tb::new(ns, db, tb);
					let val = DefineTableStatement {
						name: tb.to_owned().into(),
						permissions: Permissions::none(),
						..DefineTableStatement::default()
					};
					self.put(key, &val).await?;
					Ok(val)
				}
				true => Err(Error::TbNotFound {
					value,
				}),
			},
			Err(e) => Err(e),
			Ok(v) => Ok(v),
		}
	}

	/// Retrieve and cache a specific namespace definition.
	pub async fn get_and_cache_ns(
		&mut self,
		ns: &str,
	) -> Result<Arc<DefineNamespaceStatement>, Error> {
		let key = crate::key::ns::new(ns).encode()?;
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Ns(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let val = self.get(key.clone()).await?.ok_or(Error::NsNotFound {
				value: ns.to_owned(),
			})?;
			let val: Arc<DefineNamespaceStatement> = Arc::new(val.into());
			self.cache.set(key, Entry::Ns(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve and cache a specific database definition.
	pub async fn get_and_cache_db(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<DefineDatabaseStatement>, Error> {
		let key = crate::key::db::new(ns, db).encode()?;
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Db(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let val = self.get(key.clone()).await?.ok_or(Error::DbNotFound {
				value: db.to_owned(),
			})?;
			let val: Arc<DefineDatabaseStatement> = Arc::new(val.into());
			self.cache.set(key, Entry::Db(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve and cache a specific table definition.
	pub async fn get_and_cache_tb(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<DefineTableStatement>, Error> {
		let key = crate::key::tb::new(ns, db, tb).encode()?;
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Tb(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let val = self.get(key.clone()).await?.ok_or(Error::TbNotFound {
				value: tb.to_owned(),
			})?;
			let val: Arc<DefineTableStatement> = Arc::new(val.into());
			self.cache.set(key, Entry::Tb(Arc::clone(&val)));
			val
		})
	}

	/// Add a namespace with a default configuration, only if we are in dynamic mode.
	pub async fn add_and_cache_ns(
		&mut self,
		ns: &str,
		strict: bool,
	) -> Result<Arc<DefineNamespaceStatement>, Error> {
		match self.get_and_cache_ns(ns).await {
			Err(Error::NsNotFound {
				value,
			}) => match strict {
				false => {
					let key = crate::key::ns::new(ns);
					let val = DefineNamespaceStatement {
						name: ns.to_owned().into(),
					};
					self.put(key, &val).await?;
					Ok(Arc::new(val))
				}
				true => Err(Error::NsNotFound {
					value,
				}),
			},
			Err(e) => Err(e),
			Ok(v) => Ok(v),
		}
	}

	/// Add a database with a default configuration, only if we are in dynamic mode.
	pub async fn add_and_cache_db(
		&mut self,
		ns: &str,
		db: &str,
		strict: bool,
	) -> Result<Arc<DefineDatabaseStatement>, Error> {
		match self.get_and_cache_db(ns, db).await {
			Err(Error::DbNotFound {
				value,
			}) => match strict {
				false => {
					let key = crate::key::db::new(ns, db);
					let val = DefineDatabaseStatement {
						name: db.to_owned().into(),
					};
					self.put(key, &val).await?;
					Ok(Arc::new(val))
				}
				true => Err(Error::DbNotFound {
					value,
				}),
			},
			Err(e) => Err(e),
			Ok(v) => Ok(v),
		}
	}

	/// Add a table with a default configuration, only if we are in dynamic mode.
	pub async fn add_and_cache_tb(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<Arc<DefineTableStatement>, Error> {
		match self.get_and_cache_tb(ns, db, tb).await {
			Err(Error::TbNotFound {
				value,
			}) => match strict {
				false => {
					let key = crate::key::tb::new(ns, db, tb);
					let val = DefineTableStatement {
						name: tb.to_owned().into(),
						permissions: Permissions::none(),
						..DefineTableStatement::default()
					};
					self.put(key, &val).await?;
					Ok(Arc::new(val))
				}
				true => Err(Error::TbNotFound {
					value,
				}),
			},
			Err(e) => Err(e),
			Ok(v) => Ok(v),
		}
	}

	/// Retrieve and cache a specific table definition.
	pub async fn check_ns_db_tb(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<(), Error> {
		match strict {
			// Strict mode is disabled
			false => Ok(()),
			// Strict mode is enabled
			true => {
				self.get_and_cache_ns(ns).await?;
				self.get_and_cache_db(ns, db).await?;
				self.get_and_cache_tb(ns, db, tb).await?;
				Ok(())
			}
		}
	}

	// --------------------------------------------------
	// Additional methods
	// --------------------------------------------------

	/// Writes the full database contents as binary SQL.
	pub async fn export(&mut self, ns: &str, db: &str, chn: Sender<Vec<u8>>) -> Result<(), Error> {
		// Output OPTIONS
		{
			chn.send(bytes!("-- ------------------------------")).await?;
			chn.send(bytes!("-- OPTION")).await?;
			chn.send(bytes!("-- ------------------------------")).await?;
			chn.send(bytes!("")).await?;
			chn.send(bytes!("OPTION IMPORT;")).await?;
			chn.send(bytes!("")).await?;
		}
		// Output FUNCTIONS
		{
			let fcs = self.all_fc(ns, db).await?;
			if !fcs.is_empty() {
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- FUNCTIONS")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				for fc in fcs.iter() {
					chn.send(bytes!(format!("{fc};"))).await?;
				}
				chn.send(bytes!("")).await?;
			}
		}
		// Output LOGINS
		{
			let dls = self.all_dl(ns, db).await?;
			if !dls.is_empty() {
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- LOGINS")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				for dl in dls.iter() {
					chn.send(bytes!(format!("{dl};"))).await?;
				}
				chn.send(bytes!("")).await?;
			}
		}
		// Output TOKENS
		{
			let dts = self.all_dt(ns, db).await?;
			if !dts.is_empty() {
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- TOKENS")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				for dt in dts.iter() {
					chn.send(bytes!(format!("{dt};"))).await?;
				}
				chn.send(bytes!("")).await?;
			}
		}
		// Output PARAMS
		{
			let pas = self.all_pa(ns, db).await?;
			if !pas.is_empty() {
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- PARAMS")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				for pa in pas.iter() {
					chn.send(bytes!(format!("{pa};"))).await?;
				}
				chn.send(bytes!("")).await?;
			}
		}
		// Output SCOPES
		{
			let scs = self.all_sc(ns, db).await?;
			if !scs.is_empty() {
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- SCOPES")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				for sc in scs.iter() {
					// Output SCOPE
					chn.send(bytes!(format!("{sc};"))).await?;
					// Output TOKENS
					{
						let sts = self.all_st(ns, db, &sc.name).await?;
						if !sts.is_empty() {
							for st in sts.iter() {
								chn.send(bytes!(format!("{st};"))).await?;
							}
							chn.send(bytes!("")).await?;
						}
					}
				}
				chn.send(bytes!("")).await?;
			}
		}
		// Output TABLES
		{
			let tbs = self.all_tb(ns, db).await?;
			if !tbs.is_empty() {
				for tb in tbs.iter() {
					// Output TABLE
					chn.send(bytes!("-- ------------------------------")).await?;
					chn.send(bytes!(format!("-- TABLE: {}", tb.name))).await?;
					chn.send(bytes!("-- ------------------------------")).await?;
					chn.send(bytes!("")).await?;
					chn.send(bytes!(format!("{tb};"))).await?;
					chn.send(bytes!("")).await?;
					// Output FIELDS
					{
						let fds = self.all_fd(ns, db, &tb.name).await?;
						if !fds.is_empty() {
							for fd in fds.iter() {
								chn.send(bytes!(format!("{fd};"))).await?;
							}
							chn.send(bytes!("")).await?;
						}
					}
					// Output INDEXES
					let ixs = self.all_ix(ns, db, &tb.name).await?;
					if !ixs.is_empty() {
						for ix in ixs.iter() {
							chn.send(bytes!(format!("{ix};"))).await?;
						}
						chn.send(bytes!("")).await?;
					}
					// Output EVENTS
					let evs = self.all_ev(ns, db, &tb.name).await?;
					if !evs.is_empty() {
						for ev in evs.iter() {
							chn.send(bytes!(format!("{ev};"))).await?;
						}
						chn.send(bytes!("")).await?;
					}
				}
				// Start transaction
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- TRANSACTION")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				chn.send(bytes!("BEGIN TRANSACTION;")).await?;
				chn.send(bytes!("")).await?;
				// Output TABLE data
				for tb in tbs.iter() {
					// Start records
					chn.send(bytes!("-- ------------------------------")).await?;
					chn.send(bytes!(format!("-- TABLE DATA: {}", tb.name))).await?;
					chn.send(bytes!("-- ------------------------------")).await?;
					chn.send(bytes!("")).await?;
					// Fetch records
					let beg = thing::prefix(ns, db, &tb.name);
					let end = thing::suffix(ns, db, &tb.name);
					let mut nxt: Option<Vec<u8>> = None;
					loop {
						let res = match nxt {
							None => {
								let min = beg.clone();
								let max = end.clone();
								self.scan(min..max, 1000).await?
							}
							Some(ref mut beg) => {
								beg.push(0x00);
								let min = beg.clone();
								let max = end.clone();
								self.scan(min..max, 1000).await?
							}
						};
						if !res.is_empty() {
							// Get total results
							let n = res.len();
							// Exit when settled
							if n == 0 {
								break;
							}
							// Loop over results
							for (i, (k, v)) in res.into_iter().enumerate() {
								// Ready the next
								if n == i + 1 {
									nxt = Some(k.clone());
								}
								// Parse the key and the value
								let k: crate::key::thing::Thing = (&k).into();
								let v: crate::sql::value::Value = (&v).into();
								let t = Thing::from((k.tb, k.id));
								// Check if this is a graph edge
								match (v.pick(&*EDGE), v.pick(&*IN), v.pick(&*OUT)) {
									// This is a graph edge record
									(Value::True, Value::Thing(l), Value::Thing(r)) => {
										let sql = format!("RELATE {l} -> {t} -> {r} CONTENT {v};",);
										chn.send(bytes!(sql)).await?;
									}
									// This is a normal record
									_ => {
										let sql = format!("UPDATE {t} CONTENT {v};");
										chn.send(bytes!(sql)).await?;
									}
								}
							}
							continue;
						}
						break;
					}
					chn.send(bytes!("")).await?;
				}
				// Commit transaction
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- TRANSACTION")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				chn.send(bytes!("COMMIT TRANSACTION;")).await?;
				chn.send(bytes!("")).await?;
			}
		}
		// Everything exported
		Ok(())
	}
}
