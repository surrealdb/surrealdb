use std::borrow::Cow;
use std::fmt;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use channel::{Receiver, Sender};
use futures::lock::Mutex;
use uuid::Uuid;

use sql::permission::Permissions;
use sql::statements::DefineAccessStatement;
use sql::statements::DefineAnalyzerStatement;
use sql::statements::DefineDatabaseStatement;
use sql::statements::DefineEventStatement;
use sql::statements::DefineFieldStatement;
use sql::statements::DefineFunctionStatement;
use sql::statements::DefineIndexStatement;
use sql::statements::DefineModelStatement;
use sql::statements::DefineNamespaceStatement;
use sql::statements::DefineParamStatement;
use sql::statements::DefineTableStatement;
use sql::statements::DefineUserStatement;
use sql::statements::LiveStatement;

use crate::cf;
use crate::dbs::node::ClusterMembership;
use crate::dbs::node::Timestamp;
use crate::err::Error;
use crate::idg::u32::U32;
#[cfg(debug_assertions)]
use crate::key::debug::sprint_key;
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use crate::kvs::cache::Cache;
use crate::kvs::cache::Entry;
use crate::kvs::clock::SizedClock;
use crate::kvs::lq_structs::{LqValue, TrackedResult};
use crate::kvs::Check;
use crate::options::EngineOptions;
use crate::sql;
use crate::sql::paths::EDGE;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::thing::Thing;
use crate::sql::Strand;
use crate::sql::Value;
use crate::vs::Oracle;
use crate::vs::Versionstamp;

use super::kv::Add;
use super::kv::Convert;
use super::Key;
use super::Val;

#[derive(Copy, Clone, Debug)]
#[non_exhaustive]
pub enum Limit {
	Unlimited,
	Limited(u32),
}

#[non_exhaustive]
pub struct ScanPage<K>
where
	K: Into<Key> + Debug,
{
	pub range: Range<K>,
	pub limit: Limit,
}

impl From<Range<Vec<u8>>> for ScanPage<Vec<u8>> {
	fn from(value: Range<Vec<u8>>) -> Self {
		ScanPage {
			range: value,
			limit: Limit::Unlimited,
		}
	}
}

#[non_exhaustive]
pub struct ScanResult<K>
where
	K: Into<Key> + Debug,
{
	pub next_page: Option<ScanPage<K>>,
	pub values: Vec<(Key, Val)>,
}

/// A set of undoable updates and requests against a dataset.
#[allow(dead_code)]
#[non_exhaustive]
pub struct Transaction {
	pub(super) inner: Inner,
	pub(super) cache: Cache,
	pub(super) cf: cf::Writer,
	pub(super) vso: Arc<Mutex<Oracle>>,
	pub(super) clock: Arc<SizedClock>,
	pub(super) prepared_async_events: (Arc<Sender<TrackedResult>>, Arc<Receiver<TrackedResult>>),
	pub(super) engine_options: EngineOptions,
}

#[allow(clippy::large_enum_variant)]
pub(super) enum Inner {
	#[cfg(feature = "kv-mem")]
	Mem(super::mem::Transaction),
	#[cfg(feature = "kv-rocksdb")]
	RocksDB(super::rocksdb::Transaction),
	#[cfg(feature = "kv-speedb")]
	SpeeDB(super::speedb::Transaction),
	#[cfg(feature = "kv-indxdb")]
	IndxDB(super::indxdb::Transaction),
	#[cfg(feature = "kv-tikv")]
	TiKV(super::tikv::Transaction),
	#[cfg(feature = "kv-fdb")]
	FoundationDB(super::fdb::Transaction),
	#[cfg(feature = "kv-surrealkv")]
	SurrealKV(super::surrealkv::Transaction),
}

#[derive(Copy, Clone)]
#[non_exhaustive]
pub enum TransactionType {
	Read,
	Write,
}

impl From<bool> for TransactionType {
	fn from(value: bool) -> Self {
		match value {
			true => TransactionType::Write,
			false => TransactionType::Read,
		}
	}
}

#[non_exhaustive]
pub enum LockType {
	Pessimistic,
	Optimistic,
}

impl fmt::Display for Transaction {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		#![allow(unused_variables)]
		match &self.inner {
			#[cfg(feature = "kv-mem")]
			Inner::Mem(_) => write!(f, "memory"),
			#[cfg(feature = "kv-rocksdb")]
			Inner::RocksDB(_) => write!(f, "rocksdb"),
			#[cfg(feature = "kv-speedb")]
			Inner::SpeeDB(_) => write!(f, "speedb"),
			#[cfg(feature = "kv-indxdb")]
			Inner::IndxDB(_) => write!(f, "indxdb"),
			#[cfg(feature = "kv-tikv")]
			Inner::TiKV(_) => write!(f, "tikv"),
			#[cfg(feature = "kv-fdb")]
			Inner::FoundationDB(_) => write!(f, "fdb"),
			#[cfg(feature = "kv-surrealkv")]
			Inner::SurrealKV(_) => write!(f, "surrealkv"),
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}
}

impl Transaction {
	// --------------------------------------------------
	// Configuration methods
	// --------------------------------------------------

	pub fn rollback_with_warning(mut self) -> Self {
		self.check_level(Check::Warn);
		self
	}

	pub fn rollback_with_panic(mut self) -> Self {
		self.check_level(Check::Panic);
		self
	}

	pub fn rollback_and_ignore(mut self) -> Self {
		self.check_level(Check::None);
		self
	}

	pub fn enclose(self) -> Arc<Mutex<Self>> {
		Arc::new(Mutex::new(self))
	}

	// --------------------------------------------------
	// Integral methods
	// --------------------------------------------------

	/// Check if transaction is finished.
	///
	/// If the transaction has been cancelled or committed,
	/// then this function will return [`true`], and any further
	/// calls to functions on this transaction will result
	/// in a [`Error::TxFinished`] error.
	pub async fn closed(&self) -> bool {
		#[cfg(debug_assertions)]
		trace!("Closed");
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
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
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
				inner: Inner::FoundationDB(v),
				..
			} => v.closed(),
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
				..
			} => v.is_closed(),
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Cancel a transaction.
	///
	/// This reverses all changes made within the transaction.
	pub async fn cancel(&mut self) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		trace!("Cancel");
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
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
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
				inner: Inner::FoundationDB(v),
				..
			} => v.cancel().await,
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
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
		trace!("Commit");
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
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
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
				inner: Inner::FoundationDB(v),
				..
			} => v.commit().await,
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
				..
			} => v.commit().await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// From the existing transaction, consume all the remaining live query registration events and return them synchronously
	/// This function does not check that a transaction was committed, but the intention is to consume from this
	/// only once the transaction is committed
	pub(crate) fn consume_pending_live_queries(&self) -> Vec<TrackedResult> {
		let mut tracked_results: Vec<TrackedResult> =
			Vec::with_capacity(self.engine_options.new_live_queries_per_transaction as usize);
		while let Ok(tracked_result) = self.prepared_async_events.1.try_recv() {
			tracked_results.push(tracked_result);
		}
		tracked_results
	}

	/// Sends an async operation, such as a new live query, to the transaction which is forwarded
	/// only once committed and removed once a transaction is aborted
	// allow(dead_code) because this is used in v2, but not v1
	#[allow(dead_code)]
	pub(crate) fn pre_commit_register_async_event(
		&mut self,
		lq_entry: TrackedResult,
	) -> Result<(), Error> {
		self.prepared_async_events.0.try_send(lq_entry).map_err(|_send_err| {
			Error::Internal("Prepared lq failed to add lq to channel".to_string())
		})
	}

	/// Delete a key from the datastore.
	#[allow(unused_variables)]
	pub async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		let key = key.into();
		#[cfg(debug_assertions)]
		trace!("Del {}", sprint_key(&key));
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
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
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
				inner: Inner::FoundationDB(v),
				..
			} => v.del(key).await,
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
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
		K: Into<Key> + Debug + AsRef<[u8]>,
	{
		#[cfg(debug_assertions)]
		trace!("Exi {}", sprint_key(&key));
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
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
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
				inner: Inner::FoundationDB(v),
				..
			} => v.exi(key).await,
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
				..
			} => v.exists(key).await,
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
		let key = key.into();
		#[cfg(debug_assertions)]
		trace!("Get {}", sprint_key(&key));
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
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
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
				inner: Inner::FoundationDB(v),
				..
			} => v.get(key).await,
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
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
		let key = key.into();
		#[cfg(debug_assertions)]
		trace!("Set {} => {:?}", sprint_key(&key), val);
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
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
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
				inner: Inner::FoundationDB(v),
				..
			} => v.set(key, val).await,
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
				..
			} => v.set(key, val).await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Obtain a new change timestamp for a key
	/// which is replaced with the current timestamp when the transaction is committed.
	/// NOTE: This should be called when composing the change feed entries for this transaction,
	/// which should be done immediately before the transaction commit.
	/// That is to keep other transactions commit delay(pessimistic) or conflict(optimistic) as less as possible.
	#[allow(unused)]
	pub async fn get_timestamp<K>(&mut self, key: K, lock: bool) -> Result<Versionstamp, Error>
	where
		K: Into<Key> + Debug,
	{
		// We convert to byte slice as its easier at this level
		let key = key.into();
		#[cfg(debug_assertions)]
		trace!("Get Timestamp {}", sprint_key(&key));
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => v.get_timestamp(key),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => v.get_timestamp(key).await,
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.get_timestamp(key).await,
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.get_timestamp(key, lock).await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FoundationDB(v),
				..
			} => v.get_timestamp().await,
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
				..
			} => v.get_timestamp(key).await,
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
				..
			} => v.get_timestamp(key).await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	#[allow(unused)]
	async fn get_non_monotonic_versionstamp(&mut self) -> Result<Versionstamp, Error> {
		Ok(self.vso.lock().await.now())
	}

	#[allow(unused)]
	async fn get_non_monotonic_versionstamped_key<K>(
		&mut self,
		prefix: K,
		suffix: K,
	) -> Result<Vec<u8>, Error>
	where
		K: Into<Key>,
	{
		let prefix: Key = prefix.into();
		let suffix: Key = suffix.into();
		let ts = self.get_non_monotonic_versionstamp().await?;
		let mut k: Vec<u8> = prefix.clone();
		k.append(&mut ts.to_vec());
		k.append(&mut suffix.clone());
		Ok(k)
	}

	/// Insert or update a key in the datastore.
	#[allow(unused_variables)]
	pub async fn set_versionstamped_key<K, V>(
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
		let ts_key = ts_key.into();
		let prefix = prefix.into();
		let suffix = suffix.into();
		#[cfg(debug_assertions)]
		trace!(
			"Set Versionstamped Key ts={} prefix={} suffix={}",
			sprint_key(&prefix),
			sprint_key(&ts_key),
			sprint_key(&suffix)
		);
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => {
				let k = v.get_versionstamped_key(ts_key, prefix, suffix).await?;
				v.set(k, val)
			}
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => {
				let k = v.get_versionstamped_key(ts_key, prefix, suffix).await?;
				v.set(k, val).await
			}
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => {
				let k = v.get_versionstamped_key(ts_key, prefix, suffix).await?;
				v.set(k, val).await
			}
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => {
				let k = v.get_versionstamped_key(ts_key, prefix, suffix).await?;
				v.set(k, val).await
			}
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FoundationDB(v),
				..
			} => v.set_versionstamped_key(prefix, suffix, val).await,
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
				..
			} => {
				let k = v.get_versionstamped_key(ts_key, prefix, suffix).await?;
				v.set(k, val).await
			}
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
				..
			} => {
				let k = v.get_versionstamped_key(ts_key, prefix, suffix).await?;
				v.set(k, val).await
			}
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[allow(unused_variables)]
	pub async fn put<K, V>(&mut self, category: KeyCategory, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
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
			} => v.put(category, key, val).await,
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
				..
			} => v.put(category, key, val).await,
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.put(key, val).await,
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.put(category, key, val).await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FoundationDB(v),
				..
			} => v.put(category, key, val).await,
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
				..
			} => v.put(category, key, val).await,
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
		let rng = Range {
			start: rng.start.into(),
			end: rng.end.into(),
		};
		#[cfg(debug_assertions)]
		trace!("Scan {} - {}", sprint_key(&rng.start), sprint_key(&rng.end));
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
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
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
				inner: Inner::FoundationDB(v),
				..
			} => v.scan(rng, limit).await,
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
				..
			} => v.scan(rng, limit).await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single request to the underlying datastore.
	#[allow(unused_variables)]
	pub async fn scan_paged<K>(
		&mut self,
		page: ScanPage<K>,
		batch_limit: u32,
	) -> Result<ScanResult<K>, Error>
	where
		K: Into<Key> + From<Vec<u8>> + AsRef<[u8]> + Debug + Clone,
	{
		#[cfg(debug_assertions)]
		trace!("Scan paged {} - {}", sprint_key(&page.range.start), sprint_key(&page.range.end));
		let range = page.range.clone();
		let res = match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(v),
				..
			} => v.scan(range, batch_limit),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(v),
				..
			} => v.scan(range, batch_limit).await,
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
				..
			} => v.scan(range, batch_limit).await,
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(v),
				..
			} => v.scan(range, batch_limit).await,
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.scan(range, batch_limit).await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FoundationDB(v),
				..
			} => v.scan(range, batch_limit).await,
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
				..
			} => v.scan(range, batch_limit).await,
			#[allow(unreachable_patterns)]
			_ => Err(Error::MissingStorageEngine),
		};
		// Construct next page
		res.map(|tup_vec: Vec<(Key, Val)>| {
			if tup_vec.len() < batch_limit as usize {
				ScanResult {
					next_page: None,
					values: tup_vec,
				}
			} else {
				let (mut rng, limit) = (page.range, page.limit);
				rng.start = match tup_vec.last() {
					Some((k, _)) => K::from(k.clone().add(0)),
					None => rng.start,
				};
				ScanResult {
					next_page: Some(ScanPage {
						range: rng,
						limit,
					}),
					values: tup_vec,
				}
			}
		})
	}

	/// Update a key in the datastore if the current value matches a condition.
	#[allow(unused_variables)]
	pub async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		let key = key.into();
		#[cfg(debug_assertions)]
		trace!("Putc {} if {:?} => {:?}", sprint_key(&key), chk, val);
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
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
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
				inner: Inner::FoundationDB(v),
				..
			} => v.putc(key, val, chk).await,
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
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
		let key = key.into();
		#[cfg(debug_assertions)]
		trace!("Delc {} if {:?}", sprint_key(&key), chk);
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
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(v),
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
				inner: Inner::FoundationDB(v),
				..
			} => v.delc(key, chk).await,
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
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
		K: Into<Key> + Debug,
	{
		let beg: Key = rng.start.into();
		let end: Key = rng.end.into();
		#[cfg(debug_assertions)]
		trace!("Getr {}..{} (limit: {limit})", sprint_key(&beg), sprint_key(&end));
		let mut out: Vec<(Key, Val)> = vec![];
		let mut next_page = Some(ScanPage {
			range: beg..end,
			limit: Limit::Limited(limit),
		});
		// Start processing
		while let Some(page) = next_page {
			// Get records batch
			let res = self.scan_paged(page, 1000).await?;
			next_page = res.next_page;
			let res = res.values;
			// Exit when settled
			if res.is_empty() {
				break;
			}
			// Loop over results
			for (k, v) in res.into_iter() {
				// Delete
				out.push((k, v));
			}
		}
		Ok(out)
	}
	/// Delete a range of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in batches of 1000.
	pub async fn delr<K>(&mut self, rng: Range<K>, limit: u32) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		let rng = Range {
			start: rng.start.into(),
			end: rng.end.into(),
		};
		#[cfg(debug_assertions)]
		trace!("Delr {}..{} (limit: {limit})", sprint_key(&rng.start), sprint_key(&rng.end));
		match self {
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(v),
				..
			} => v.delr(rng, limit).await,
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FoundationDB(v),
				..
			} => v.delr(rng).await,
			#[allow(unreachable_patterns)]
			_ => self._delr(rng, limit).await,
		}
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in batches of 1000.
	async fn _delr<K>(&mut self, rng: Range<K>, limit: u32) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		let beg: Key = rng.start.into();
		let end: Key = rng.end.into();
		// Start processing
		let mut next_page = Some(ScanPage {
			range: beg..end,
			limit: Limit::Limited(limit),
		});
		while let Some(page) = next_page {
			// Get records batch
			let res = self.scan_paged(page, limit).await?;
			next_page = res.next_page;
			let res = res.values;
			// Exit when settled
			if res.is_empty() {
				#[cfg(debug_assertions)]
				trace!("Delr page was empty");
				break;
			}
			// Loop over results
			for (k, _) in res.into_iter() {
				// Delete
				#[cfg(debug_assertions)]
				trace!("Delr key {}", sprint_key(&k));
				self.del(k).await?;
			}
		}
		Ok(())
	}
	/// Retrieve a specific prefix of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in batches of 1000.
	pub async fn getp<K>(&mut self, key: K, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Debug,
	{
		let beg: Key = key.into();
		let end: Key = beg.clone().add(0xff);
		#[cfg(debug_assertions)]
		trace!("Getp {}-{} (limit: {limit})", sprint_key(&beg), sprint_key(&end));
		let mut out: Vec<(Key, Val)> = vec![];
		// Start processing
		let mut next_page = Some(ScanPage {
			range: beg..end,
			limit: Limit::Limited(limit),
		});
		while let Some(page) = next_page {
			let res = self.scan_paged(page, 1000).await?;
			next_page = res.next_page;
			// Get records batch
			let res = res.values;
			// Exit when settled
			if res.is_empty() {
				break;
			};
			// Loop over results
			for (k, v) in res.into_iter() {
				// Delete
				out.push((k, v));
			}
		}
		Ok(out)
	}
	/// Delete a prefix of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in batches of 1000.
	pub async fn delp<K>(&mut self, key: K, limit: u32) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		let beg: Key = key.into();
		let end: Key = beg.clone().add(0xff);
		#[cfg(debug_assertions)]
		trace!("Delp {}-{} (limit: {limit})", sprint_key(&beg), sprint_key(&end));
		let min = beg.clone();
		let max = end.clone();
		self.delr(min..max, limit).await?;
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

	// Register cluster membership
	// NOTE: Setting cluster membership sets the heartbeat
	// Remember to set the heartbeat as well
	pub async fn set_nd(&mut self, id: Uuid) -> Result<(), Error> {
		let key = crate::key::root::nd::Nd::new(id);
		match self.get_nd(id).await? {
			Some(_) => Err(Error::ClAlreadyExists {
				value: id.to_string(),
			}),
			None => {
				let value = ClusterMembership {
					name: id.to_string(),
					heartbeat: self.clock().await,
				};
				self.put(key.key_category(), key, value).await?;
				Ok(())
			}
		}
	}

	// Retrieve cluster information
	pub async fn get_nd(&mut self, id: Uuid) -> Result<Option<ClusterMembership>, Error> {
		let key = crate::key::root::nd::Nd::new(id);
		let val = self.get(key).await?;
		match val {
			Some(v) => Ok(Some::<ClusterMembership>(v.into())),
			None => Ok(None),
		}
	}

	/// Clock retrieves the current timestamp, without guaranteeing
	/// monotonicity in all implementations.
	///
	/// It is used for unreliable ordering of events as well as
	/// handling of timeouts. Operations that are not guaranteed to be correct.
	/// But also allows for lexicographical ordering.
	///
	/// Public for tests, but not required for usage from a user perspective.
	pub async fn clock(&self) -> Timestamp {
		// Use a timestamp oracle if available
		// Match, because we cannot have sized traits or async traits
		self.clock.now().await
	}

	// Set heartbeat
	pub async fn set_hb(&mut self, timestamp: Timestamp, id: Uuid) -> Result<(), Error> {
		let key = crate::key::root::hb::Hb::new(timestamp, id);
		// We do not need to do a read, we always want to overwrite
		let key_enc = key.encode()?;
		self.put(
			key.key_category(),
			key_enc,
			ClusterMembership {
				name: id.to_string(),
				heartbeat: timestamp,
			},
		)
		.await?;
		Ok(())
	}

	pub async fn del_hb(&mut self, timestamp: Timestamp, id: Uuid) -> Result<(), Error> {
		let key = crate::key::root::hb::Hb::new(timestamp, id);
		self.del(key).await?;
		Ok(())
	}

	// Delete a cluster registration entry
	pub async fn del_nd(&mut self, node: Uuid) -> Result<(), Error> {
		let key = crate::key::root::nd::Nd::new(node);
		let key_enc = key.encode()?;
		self.del(key_enc).await
	}

	// Delete the live query notification registry on the table
	pub async fn del_ndlq(&mut self, nd: Uuid, lq: Uuid, ns: &str, db: &str) -> Result<(), Error> {
		let key = crate::key::node::lq::Lq::new(nd, lq, ns, db);
		let key_enc = key.encode()?;
		self.del(key_enc).await
	}

	// Scans up until the heartbeat timestamp and returns the discovered nodes
	pub async fn scan_hb(
		&mut self,
		time_to: &Timestamp,
		batch_size: u32,
	) -> Result<Vec<crate::key::root::hb::Hb>, Error> {
		let beg = crate::key::root::hb::Hb::prefix();
		let end = crate::key::root::hb::Hb::suffix(time_to);
		let mut out: Vec<crate::key::root::hb::Hb> = vec![];
		// Start processing
		let mut next_page = Some(ScanPage::from(beg..end));
		while let Some(page) = next_page {
			let res = self.scan_paged(page, batch_size).await?;
			next_page = res.next_page;
			for (k, _) in res.values.into_iter() {
				out.push(crate::key::root::hb::Hb::decode(k.as_slice())?);
			}
		}
		Ok(out)
	}

	/// scan_nd will scan all the cluster membership registers
	/// setting limit to 0 will result in scanning all entries
	pub async fn scan_nd(&mut self, batch_size: u32) -> Result<Vec<ClusterMembership>, Error> {
		let beg = crate::key::root::nd::Nd::prefix();
		let end = crate::key::root::nd::Nd::suffix();
		let mut out: Vec<ClusterMembership> = vec![];
		// Start processing
		let mut next_page = Some(ScanPage::from(beg..end));
		while let Some(page) = next_page {
			let res = self.scan_paged(page, batch_size).await?;
			next_page = res.next_page;
			for (_, v) in res.values.into_iter() {
				out.push(v.into());
			}
		}
		Ok(out)
	}

	pub async fn delr_hb(
		&mut self,
		ts: Vec<crate::key::root::hb::Hb>,
		limit: u32,
	) -> Result<(), Error> {
		trace!("delr_hb: ts={:?} limit={:?}", ts, limit);
		for hb in ts.into_iter() {
			self.del(hb).await?;
		}
		Ok(())
	}

	pub async fn del_tblq(&mut self, ns: &str, db: &str, tb: &str, lv: Uuid) -> Result<(), Error> {
		trace!("del_lv: ns={:?} db={:?} tb={:?} lv={:?}", ns, db, tb, lv);
		let key = crate::key::table::lq::new(ns, db, tb, lv);
		self.cache.del(&key.clone().into());
		self.del(key).await
	}

	pub async fn scan_ndlq<'a>(
		&mut self,
		node: &Uuid,
		batch_size: u32,
	) -> Result<Vec<LqValue>, Error> {
		let beg = crate::key::node::lq::prefix_nd(node);
		let end = crate::key::node::lq::suffix_nd(node);
		let mut out: Vec<LqValue> = vec![];
		let mut next_page = Some(ScanPage::from(beg..end));
		while let Some(page) = next_page {
			let res = self.scan_paged(page, batch_size).await?;
			next_page = res.next_page;
			for (key, value) in res.values.into_iter() {
				let lv = crate::key::node::lq::Lq::decode(key.as_slice())?;
				let tb: String = String::from_utf8(value).unwrap();
				out.push(LqValue {
					nd: lv.nd.into(),
					ns: lv.ns.to_string(),
					db: lv.db.to_string(),
					tb,
					lq: lv.lq.into(),
				});
			}
		}
		Ok(out)
	}

	pub async fn scan_tblq<'a>(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		batch_size: u32,
	) -> Result<Vec<LqValue>, Error> {
		let beg = crate::key::table::lq::prefix(ns, db, tb);
		let end = crate::key::table::lq::suffix(ns, db, tb);
		let mut out: Vec<LqValue> = vec![];
		let mut next_page = Some(ScanPage::from(beg..end));
		while let Some(page) = next_page {
			let res = self.scan_paged(page, batch_size).await?;
			next_page = res.next_page;
			for (key, value) in res.values.into_iter() {
				let lv = crate::key::table::lq::Lq::decode(key.as_slice())?;
				let val: LiveStatement = value.into();
				out.push(LqValue {
					nd: val.node,
					ns: lv.ns.to_string(),
					db: lv.db.to_string(),
					tb: lv.tb.to_string(),
					lq: val.id,
				});
			}
		}
		Ok(out)
	}

	/// Add live query to table
	pub async fn putc_tblq(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		live_stm: LiveStatement,
		expected: Option<LiveStatement>,
	) -> Result<(), Error> {
		let key = crate::key::table::lq::new(ns, db, tb, live_stm.id.0);
		let key_enc = crate::key::table::lq::Lq::encode(&key)?;
		#[cfg(debug_assertions)]
		trace!("putc_tblq ({:?}): key={:?}", &live_stm.id, sprint_key(&key_enc));
		self.putc(key_enc, live_stm, expected).await
	}

	pub async fn putc_ndlq(
		&mut self,
		nd: Uuid,
		lq: Uuid,
		ns: &str,
		db: &str,
		tb: &str,
		chk: Option<&str>,
	) -> Result<(), Error> {
		let key = crate::key::node::lq::new(nd, lq, ns, db);
		self.putc(key, tb, chk).await
	}

	/// Retrieve all ROOT users.
	pub async fn all_root_users(&mut self) -> Result<Arc<[DefineUserStatement]>, Error> {
		let beg = crate::key::root::us::prefix();
		let end = crate::key::root::us::suffix();
		let val = self.getr(beg..end, u32::MAX).await?;
		let val = val.convert().into();
		Ok(val)
	}

	/// Retrieve all namespace definitions in a datastore.
	pub async fn all_ns(&mut self) -> Result<Arc<[DefineNamespaceStatement]>, Error> {
		let key = crate::key::root::ns::prefix();
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Nss(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::root::ns::prefix();
			let end = crate::key::root::ns::suffix();
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Nss(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all namespace user definitions for a specific namespace.
	pub async fn all_ns_users(&mut self, ns: &str) -> Result<Arc<[DefineUserStatement]>, Error> {
		let key = crate::key::namespace::us::prefix(ns);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Nus(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::namespace::us::prefix(ns);
			let end = crate::key::namespace::us::suffix(ns);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Nus(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all namespace access method definitions.
	pub async fn all_ns_accesses(
		&mut self,
		ns: &str,
	) -> Result<Arc<[DefineAccessStatement]>, Error> {
		let key = crate::key::namespace::ac::prefix(ns);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Nas(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::namespace::ac::prefix(ns);
			let end = crate::key::namespace::ac::suffix(ns);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Nas(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all namespace access method definitions in redacted form.
	pub async fn all_ns_accesses_redacted(
		&mut self,
		ns: &str,
	) -> Result<Arc<[DefineAccessStatement]>, Error> {
		let accesses = self.all_ns_accesses(ns).await?;
		let redacted: Vec<_> = accesses.iter().map(|statement| statement.redacted()).collect();
		Ok(Arc::from(redacted))
	}

	/// Retrieve all database definitions for a specific namespace.
	pub async fn all_db(&mut self, ns: &str) -> Result<Arc<[DefineDatabaseStatement]>, Error> {
		let key = crate::key::namespace::db::prefix(ns);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Dbs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::namespace::db::prefix(ns);
			let end = crate::key::namespace::db::suffix(ns);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Dbs(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all database user definitions for a specific database.
	pub async fn all_db_users(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineUserStatement]>, Error> {
		let key = crate::key::database::us::prefix(ns, db);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Dus(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::database::us::prefix(ns, db);
			let end = crate::key::database::us::suffix(ns, db);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Dus(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all database access method definitions.
	pub async fn all_db_accesses(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineAccessStatement]>, Error> {
		let key = crate::key::database::ac::prefix(ns, db);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Das(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::database::ac::prefix(ns, db);
			let end = crate::key::database::ac::suffix(ns, db);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Das(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all database access method definitions in redacted form.
	pub async fn all_db_accesses_redacted(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineAccessStatement]>, Error> {
		let accesses = self.all_db_accesses(ns, db).await?;
		let redacted: Vec<_> = accesses.iter().map(|statement| statement.redacted()).collect();
		Ok(Arc::from(redacted))
	}

	/// Retrieve all analyzer definitions for a specific database.
	pub async fn all_db_analyzers(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineAnalyzerStatement]>, Error> {
		let key = crate::key::database::az::prefix(ns, db);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Azs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::database::az::prefix(ns, db);
			let end = crate::key::database::az::suffix(ns, db);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Azs(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all function definitions for a specific database.
	pub async fn all_db_functions(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineFunctionStatement]>, Error> {
		let key = crate::key::database::fc::prefix(ns, db);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Fcs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::database::fc::prefix(ns, db);
			let end = crate::key::database::fc::suffix(ns, db);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Fcs(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all param definitions for a specific database.
	pub async fn all_db_params(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineParamStatement]>, Error> {
		let key = crate::key::database::pa::prefix(ns, db);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Pas(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::database::pa::prefix(ns, db);
			let end = crate::key::database::pa::suffix(ns, db);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Pas(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all model definitions for a specific database.
	pub async fn all_db_models(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineModelStatement]>, Error> {
		let key = crate::key::database::ml::prefix(ns, db);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Mls(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::database::ml::prefix(ns, db);
			let end = crate::key::database::ml::suffix(ns, db);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Mls(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all table definitions for a specific database.
	pub async fn all_tb(
		&mut self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineTableStatement]>, Error> {
		let key = crate::key::database::tb::prefix(ns, db);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Tbs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::database::tb::prefix(ns, db);
			let end = crate::key::database::tb::suffix(ns, db);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Tbs(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all event definitions for a specific table.
	pub async fn all_tb_events(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineEventStatement]>, Error> {
		let key = crate::key::table::ev::prefix(ns, db, tb);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Evs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::table::ev::prefix(ns, db, tb);
			let end = crate::key::table::ev::suffix(ns, db, tb);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Evs(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all field definitions for a specific table.
	pub async fn all_tb_fields(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineFieldStatement]>, Error> {
		let key = crate::key::table::fd::prefix(ns, db, tb);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Fds(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::table::fd::prefix(ns, db, tb);
			let end = crate::key::table::fd::suffix(ns, db, tb);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Fds(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all index definitions for a specific table.
	pub async fn all_tb_indexes(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineIndexStatement]>, Error> {
		let key = crate::key::table::ix::prefix(ns, db, tb);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Ixs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::table::ix::prefix(ns, db, tb);
			let end = crate::key::table::ix::suffix(ns, db, tb);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Ixs(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all view definitions for a specific table.
	pub async fn all_tb_views(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineTableStatement]>, Error> {
		let key = crate::key::table::ft::prefix(ns, db, tb);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Fts(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::table::ft::prefix(ns, db, tb);
			let end = crate::key::table::ft::suffix(ns, db, tb);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Fts(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve all live definitions for a specific table.
	pub async fn all_tb_lives(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[LiveStatement]>, Error> {
		let key = crate::key::table::lq::prefix(ns, db, tb);
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Lvs(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let beg = crate::key::table::lq::prefix(ns, db, tb);
			let end = crate::key::table::lq::suffix(ns, db, tb);
			let val = self.getr(beg..end, u32::MAX).await?;
			let val = val.convert().into();
			self.cache.set(key, Entry::Lvs(Arc::clone(&val)));
			val
		})
	}

	pub async fn all_lq(&mut self, nd: &uuid::Uuid) -> Result<Vec<LqValue>, Error> {
		let beg = crate::key::node::lq::prefix_nd(nd);
		let end = crate::key::node::lq::suffix_nd(nd);
		let lq_pairs = self.getr(beg..end, u32::MAX).await?;
		let mut lqs = vec![];
		for (key, value) in lq_pairs {
			let lq_key = crate::key::node::lq::Lq::decode(key.as_slice())?;
			trace!("Value is {:?}", &value);
			let lq_value = String::from_utf8(value).map_err(|e| {
				Error::Internal(format!("Failed to decode a value while reading LQ: {}", e))
			})?;
			let lqv = LqValue {
				nd: (*nd).into(),
				ns: lq_key.ns.to_string(),
				db: lq_key.db.to_string(),
				tb: lq_value,
				lq: lq_key.lq.into(),
			};
			lqs.push(lqv);
		}
		Ok(lqs)
	}

	/// Retrieve a specific user definition from ROOT.
	pub async fn get_root_user(&mut self, user: &str) -> Result<DefineUserStatement, Error> {
		let key = crate::key::root::us::new(user);
		let val = self.get(key).await?.ok_or(Error::UserRootNotFound {
			value: user.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific namespace definition.
	pub async fn get_ns(&mut self, ns: &str) -> Result<DefineNamespaceStatement, Error> {
		let key = crate::key::root::ns::new(ns);
		let val = self.get(key).await?.ok_or(Error::NsNotFound {
			value: ns.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific user definition from a namespace.
	pub async fn get_ns_user(
		&mut self,
		ns: &str,
		user: &str,
	) -> Result<DefineUserStatement, Error> {
		let key = crate::key::namespace::us::new(ns, user);
		let val = self.get(key).await?.ok_or(Error::UserNsNotFound {
			value: user.to_owned(),
			ns: ns.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific namespace access method definition.
	pub async fn get_ns_access(
		&mut self,
		ns: &str,
		ac: &str,
	) -> Result<DefineAccessStatement, Error> {
		let key = crate::key::namespace::ac::new(ns, ac);
		let val = self.get(key).await?.ok_or(Error::NaNotFound {
			value: ac.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific database definition.
	pub async fn get_db(&mut self, ns: &str, db: &str) -> Result<DefineDatabaseStatement, Error> {
		let key = crate::key::namespace::db::new(ns, db);
		let val = self.get(key).await?.ok_or(Error::DbNotFound {
			value: db.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific user definition from a database.
	pub async fn get_db_user(
		&mut self,
		ns: &str,
		db: &str,
		user: &str,
	) -> Result<DefineUserStatement, Error> {
		let key = crate::key::database::us::new(ns, db, user);
		let val = self.get(key).await?.ok_or(Error::UserDbNotFound {
			value: user.to_owned(),
			ns: ns.to_owned(),
			db: db.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific model definition from a database.
	pub async fn get_db_model(
		&mut self,
		ns: &str,
		db: &str,
		ml: &str,
		vn: &str,
	) -> Result<DefineModelStatement, Error> {
		let key = crate::key::database::ml::new(ns, db, ml, vn);
		let val = self.get(key).await?.ok_or(Error::MlNotFound {
			value: format!("{ml}<{vn}>"),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific database access method definition.
	pub async fn get_db_access(
		&mut self,
		ns: &str,
		db: &str,
		ac: &str,
	) -> Result<DefineAccessStatement, Error> {
		let key = crate::key::database::ac::new(ns, db, ac);
		let val = self.get(key).await?.ok_or(Error::DaNotFound {
			value: ac.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific analyzer definition.
	pub async fn get_db_analyzer(
		&mut self,
		ns: &str,
		db: &str,
		az: &str,
	) -> Result<DefineAnalyzerStatement, Error> {
		let key = crate::key::database::az::new(ns, db, az);
		let val = self.get(key).await?.ok_or(Error::AzNotFound {
			value: az.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific function definition from a database.
	pub async fn get_db_function(
		&mut self,
		ns: &str,
		db: &str,
		fc: &str,
	) -> Result<DefineFunctionStatement, Error> {
		let key = crate::key::database::fc::new(ns, db, fc);
		let val = self.get(key).await?.ok_or(Error::FcNotFound {
			value: fc.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a specific function definition from a database.
	pub async fn get_db_param(
		&mut self,
		ns: &str,
		db: &str,
		pa: &str,
	) -> Result<DefineParamStatement, Error> {
		let key = crate::key::database::pa::new(ns, db, pa);
		let val = self.get(key).await?.ok_or(Error::PaNotFound {
			value: pa.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Return the table stored at the lq address
	pub async fn get_lq(
		&mut self,
		nd: Uuid,
		ns: &str,
		db: &str,
		lq: Uuid,
	) -> Result<Strand, Error> {
		let key = crate::key::node::lq::new(nd, lq, ns, db);
		let val = self.get(key).await?.ok_or(Error::LqNotFound {
			value: lq.to_string(),
		})?;
		Value::from(val).convert_to_strand()
	}

	/// Retrieve a specific table definition.
	pub async fn get_tb(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<DefineTableStatement, Error> {
		let key = crate::key::database::tb::new(ns, db, tb);
		let val = self.get(key).await?.ok_or(Error::TbNotFound {
			value: tb.to_owned(),
		})?;
		Ok(val.into())
	}

	/// Retrieve a live query for a table.
	pub async fn get_tb_live(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		lv: &Uuid,
	) -> Result<LiveStatement, Error> {
		let key = crate::key::table::lq::new(ns, db, tb, *lv);
		let key_enc = crate::key::table::lq::Lq::encode(&key)?;
		#[cfg(debug_assertions)]
		trace!("Getting lv ({:?}) {}", lv, sprint_key(&key_enc));
		let val = self.get(key_enc).await?.ok_or(Error::LvNotFound {
			value: lv.to_string(),
		})?;
		Ok(val.into())
	}

	/// Retrieve an event for a table.
	pub async fn get_tb_event(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		ev: &str,
	) -> Result<DefineEventStatement, Error> {
		let key = crate::key::table::ev::new(ns, db, tb, ev);
		let key_enc = crate::key::table::ev::Ev::encode(&key)?;
		#[cfg(debug_assertions)]
		trace!("Getting ev ({:?}) {}", ev, sprint_key(&key_enc));
		let val = self.get(key_enc).await?.ok_or(Error::EvNotFound {
			value: ev.to_string(),
		})?;
		Ok(val.into())
	}

	/// Retrieve an event for a table.
	pub async fn get_tb_field(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		fd: &str,
	) -> Result<DefineFieldStatement, Error> {
		let key = crate::key::table::fd::new(ns, db, tb, fd);
		let key_enc = crate::key::table::fd::Fd::encode(&key)?;
		#[cfg(debug_assertions)]
		trace!("Getting fd ({:?}) {}", fd, sprint_key(&key_enc));
		let val = self.get(key_enc).await?.ok_or(Error::FdNotFound {
			value: fd.to_string(),
		})?;
		Ok(val.into())
	}

	/// Retrieve an event for a table.
	pub async fn get_tb_index(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		ix: &str,
	) -> Result<DefineIndexStatement, Error> {
		let key = crate::key::table::ix::new(ns, db, tb, ix);
		let key_enc = crate::key::table::ix::Ix::encode(&key)?;
		#[cfg(debug_assertions)]
		trace!("Getting ix ({:?}) {}", ix, sprint_key(&key_enc));
		let val = self.get(key_enc).await?.ok_or(Error::IxNotFound {
			value: ix.to_string(),
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
					let key = crate::key::root::ns::new(ns);
					let val = DefineNamespaceStatement {
						name: ns.to_owned().into(),
						..Default::default()
					};
					self.put(key.key_category(), key, &val).await?;
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
					let key = crate::key::namespace::db::new(ns, db);
					let val = DefineDatabaseStatement {
						name: db.to_owned().into(),
						..Default::default()
					};
					self.put(key.key_category(), key, &val).await?;
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
					let key = crate::key::database::tb::new(ns, db, tb);
					let val = DefineTableStatement {
						name: tb.to_owned().into(),
						permissions: Permissions::none(),
						..Default::default()
					};
					self.put(key.key_category(), key, &val).await?;
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
		let key = crate::key::root::ns::new(ns).encode()?;
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
		let key = crate::key::namespace::db::new(ns, db).encode()?;
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
		let key = crate::key::database::tb::new(ns, db, tb).encode()?;
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

	/// Retrieve a specific function definition.
	pub async fn get_and_cache_db_function(
		&mut self,
		ns: &str,
		db: &str,
		fc: &str,
	) -> Result<Arc<DefineFunctionStatement>, Error> {
		let key = crate::key::database::fc::new(ns, db, fc).encode()?;
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Fc(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let val = self.get(key.clone()).await?.ok_or(Error::FcNotFound {
				value: fc.to_owned(),
			})?;
			let val: Arc<DefineFunctionStatement> = Arc::new(val.into());
			self.cache.set(key, Entry::Fc(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve a specific param definition.
	pub async fn get_and_cache_db_param(
		&mut self,
		ns: &str,
		db: &str,
		pa: &str,
	) -> Result<Arc<DefineParamStatement>, Error> {
		let key = crate::key::database::pa::new(ns, db, pa).encode()?;
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Pa(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let val = self.get(key.clone()).await?.ok_or(Error::PaNotFound {
				value: pa.to_owned(),
			})?;
			let val: Arc<DefineParamStatement> = Arc::new(val.into());
			self.cache.set(key, Entry::Pa(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve a specific model definition.
	pub async fn get_and_cache_db_model(
		&mut self,
		ns: &str,
		db: &str,
		ml: &str,
		vn: &str,
	) -> Result<Arc<DefineModelStatement>, Error> {
		let key = crate::key::database::ml::new(ns, db, ml, vn).encode()?;
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Ml(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let val = self.get(key.clone()).await?.ok_or(Error::MlNotFound {
				value: format!("{ml}<{vn}>"),
			})?;
			let val: Arc<DefineModelStatement> = Arc::new(val.into());
			self.cache.set(key, Entry::Ml(Arc::clone(&val)));
			val
		})
	}

	/// Retrieve a specific table index definition.
	pub async fn get_and_cache_tb_index(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		ix: &str,
	) -> Result<Arc<DefineIndexStatement>, Error> {
		let key = crate::key::table::ix::new(ns, db, tb, ix).encode()?;
		Ok(if let Some(e) = self.cache.get(&key) {
			if let Entry::Ix(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let val = self.get(key.clone()).await?.ok_or(Error::IxNotFound {
				value: ix.to_owned(),
			})?;
			let val: Arc<DefineIndexStatement> = Arc::new(val.into());
			self.cache.set(key, Entry::Ix(Arc::clone(&val)));
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
					let key = crate::key::root::ns::new(ns);
					let val = DefineNamespaceStatement {
						name: ns.to_owned().into(),
						..Default::default()
					};
					self.put(key.key_category(), key, &val).await?;
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
					let key = crate::key::namespace::db::new(ns, db);
					let val = DefineDatabaseStatement {
						name: db.to_owned().into(),
						..Default::default()
					};
					self.put(key.key_category(), key, &val).await?;
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
					let key = crate::key::database::tb::new(ns, db, tb);
					let val = DefineTableStatement {
						name: tb.to_owned().into(),
						permissions: Permissions::none(),
						..Default::default()
					};
					self.put(key.key_category(), key, &val).await?;
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
		// Output USERS
		{
			let dus = self.all_db_users(ns, db).await?;
			if !dus.is_empty() {
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- USERS")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				for us in dus.iter() {
					chn.send(bytes!(format!("{us};"))).await?;
				}
				chn.send(bytes!("")).await?;
			}
		}
		// Output ACCESSES
		{
			let dts = self.all_db_accesses(ns, db).await?;
			if !dts.is_empty() {
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- ACCESSES")).await?;
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
			let pas = self.all_db_params(ns, db).await?;
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
		// Output FUNCTIONS
		{
			let fcs = self.all_db_functions(ns, db).await?;
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
		// Output ANALYZERS
		{
			let azs = self.all_db_analyzers(ns, db).await?;
			if !azs.is_empty() {
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("-- ANALYZERS")).await?;
				chn.send(bytes!("-- ------------------------------")).await?;
				chn.send(bytes!("")).await?;
				for az in azs.iter() {
					chn.send(bytes!(format!("{az};"))).await?;
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
					let fds = self.all_tb_fields(ns, db, &tb.name).await?;
					if !fds.is_empty() {
						for fd in fds.iter() {
							chn.send(bytes!(format!("{fd};"))).await?;
						}
						chn.send(bytes!("")).await?;
					}
					// Output INDEXES
					let ixs = self.all_tb_indexes(ns, db, &tb.name).await?;
					if !ixs.is_empty() {
						for ix in ixs.iter() {
							chn.send(bytes!(format!("{ix};"))).await?;
						}
						chn.send(bytes!("")).await?;
					}
					// Output EVENTS
					let evs = self.all_tb_events(ns, db, &tb.name).await?;
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
					let beg = crate::key::thing::prefix(ns, db, &tb.name);
					let end = crate::key::thing::suffix(ns, db, &tb.name);
					let mut nxt: Option<ScanPage<Vec<u8>>> = Some(ScanPage::from(beg..end));
					while nxt.is_some() {
						let res = self.scan_paged(nxt.unwrap(), 1000).await?;
						nxt = res.next_page;
						let res = res.values;
						if res.is_empty() {
							break;
						}
						// Loop over results
						for (k, v) in res.into_iter() {
							// Parse the key and the value
							let k: crate::key::thing::Thing = (&k).into();
							let v: Value = (&v).into();
							let t = Thing::from((k.tb, k.id));
							// Check if this is a graph edge
							match (v.pick(&*EDGE), v.pick(&*IN), v.pick(&*OUT)) {
								// This is a graph edge record
								(Value::Bool(true), Value::Thing(l), Value::Thing(r)) => {
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

	// change will record the change in the changefeed if enabled.
	// To actually persist the record changes into the underlying kvs,
	// you must call the `complete_changes` function and then commit the transaction.
	pub(crate) fn clear_cache(&mut self) {
		self.cache.clear()
	}

	// change will record the change in the changefeed if enabled.
	// To actually persist the record changes into the underlying kvs,
	// you must call the `complete_changes` function and then commit the transaction.
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn record_change(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		id: &Thing,
		previous: Cow<'_, Value>,
		current: Cow<'_, Value>,
		store_difference: bool,
	) {
		self.cf.record_cf_change(ns, db, tb, id.clone(), previous, current, store_difference)
	}

	// Records the table (re)definition in the changefeed if enabled.
	pub(crate) fn record_table_change(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		dt: &DefineTableStatement,
	) {
		self.cf.define_table(ns, db, tb, dt)
	}

	pub(crate) async fn get_idg(&mut self, key: Key) -> Result<U32, Error> {
		let seq = if let Some(e) = self.cache.get(&key) {
			if let Entry::Seq(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let val = self.get(key.clone()).await?;
			if let Some(val) = val {
				U32::new(key.clone(), Some(val)).await?
			} else {
				U32::new(key.clone(), None).await?
			}
		};

		Ok(seq)
	}

	// get_next_db_id will get the next db id for the given namespace.
	pub(crate) async fn get_next_db_id(&mut self, ns: u32) -> Result<u32, Error> {
		let key = crate::key::namespace::di::new(ns).encode().unwrap();
		let mut seq = if let Some(e) = self.cache.get(&key) {
			if let Entry::Seq(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let val = self.get(key.clone()).await?;
			if let Some(val) = val {
				U32::new(key.clone(), Some(val)).await?
			} else {
				U32::new(key.clone(), None).await?
			}
		};

		let id = seq.get_next_id();

		self.cache.set(key.clone(), Entry::Seq(seq.clone()));
		let (k, v) = seq.finish().unwrap();
		self.set(k, v).await?;

		Ok(id)
	}

	// remove_db_id removes the given db id from the sequence.
	#[allow(unused)]
	pub(crate) async fn remove_db_id(&mut self, ns: u32, db: u32) -> Result<(), Error> {
		let key = crate::key::namespace::di::new(ns).encode().unwrap();
		let mut seq = self.get_idg(key.clone()).await?;

		seq.remove_id(db);

		self.cache.set(key.clone(), Entry::Seq(seq.clone()));
		let (k, v) = seq.finish().unwrap();
		self.set(k, v).await?;

		Ok(())
	}

	// get_next_db_id will get the next tb id for the given namespace and database.
	pub(crate) async fn get_next_tb_id(&mut self, ns: u32, db: u32) -> Result<u32, Error> {
		let key = crate::key::database::ti::new(ns, db).encode().unwrap();
		let mut seq = self.get_idg(key.clone()).await?;

		let id = seq.get_next_id();

		self.cache.set(key.clone(), Entry::Seq(seq.clone()));
		let (k, v) = seq.finish().unwrap();
		self.set(k, v).await?;

		Ok(id)
	}

	// remove_tb_id removes the given tb id from the sequence.
	#[allow(unused)]
	pub(crate) async fn remove_tb_id(&mut self, ns: u32, db: u32, tb: u32) -> Result<(), Error> {
		let key = crate::key::database::ti::new(ns, db).encode().unwrap();
		let mut seq = self.get_idg(key.clone()).await?;

		seq.remove_id(tb);

		self.cache.set(key.clone(), Entry::Seq(seq.clone()));
		let (k, v) = seq.finish().unwrap();
		self.set(k, v).await?;

		Ok(())
	}

	// get_next_ns_id will get the next ns id.
	pub(crate) async fn get_next_ns_id(&mut self) -> Result<u32, Error> {
		let key = crate::key::root::ni::Ni::default().encode().unwrap();
		let mut seq = if let Some(e) = self.cache.get(&key) {
			if let Entry::Seq(v) = e {
				v
			} else {
				unreachable!();
			}
		} else {
			let val = self.get(key.clone()).await?;
			if let Some(val) = val {
				U32::new(key.clone(), Some(val)).await?
			} else {
				U32::new(key.clone(), None).await?
			}
		};

		let id = seq.get_next_id();

		self.cache.set(key.clone(), Entry::Seq(seq.clone()));
		let (k, v) = seq.finish().unwrap();
		self.set(k, v).await?;

		Ok(id)
	}

	// remove_ns_id removes the given ns id from the sequence.
	#[allow(unused)]
	pub(crate) async fn remove_ns_id(&mut self, ns: u32) -> Result<(), Error> {
		let key = crate::key::root::ni::Ni::default().encode().unwrap();
		let mut seq = self.get_idg(key.clone()).await?;

		seq.remove_id(ns);

		self.cache.set(key.clone(), Entry::Seq(seq.clone()));
		let (k, v) = seq.finish().unwrap();
		self.set(k, v).await?;

		Ok(())
	}

	// complete_changes will complete the changefeed recording for the given namespace and database.
	//
	// Under the hood, this function calls the transaction's `set_versionstamped_key` for each change.
	// Every change must be recorded by calling this struct's `record_change` function beforehand.
	// If there were no preceding `record_change` function calls for this transaction, this function will do nothing.
	//
	// This function should be called only after all the changes have been made to the transaction.
	// Otherwise, changes are missed in the change feed.
	//
	// This function should be called immediately before calling the commit function to guarantee that
	// the lock, if needed by lock=true, is held only for the duration of the commit, not the entire transaction.
	//
	// This function is here because it needs access to mutably borrow the transaction.
	//
	// Lastly, you should set lock=true if you want the changefeed to be correctly ordered for
	// non-FDB backends.
	pub(crate) async fn complete_changes(&mut self, _lock: bool) -> Result<(), Error> {
		let changes = self.cf.get();
		for (tskey, prefix, suffix, v) in changes {
			self.set_versionstamped_key(tskey, prefix, suffix, v).await?
		}
		Ok(())
	}

	// set_timestamp_for_versionstamp correlates the given timestamp with the current versionstamp.
	// This allows get_versionstamp_from_timestamp to obtain the versionstamp from the timestamp later.
	pub(crate) async fn set_timestamp_for_versionstamp(
		&mut self,
		ts: u64,
		ns: &str,
		db: &str,
		lock: bool,
	) -> Result<Versionstamp, Error> {
		// This also works as an advisory lock on the ts keys so that there is
		// on other concurrent transactions that can write to the ts_key or the keys after it.
		let vs = self.get_timestamp(crate::key::database::vs::new(ns, db), lock).await?;
		#[cfg(debug_assertions)]
		trace!(
			"Setting timestamp {} for versionstamp {:?} in ns: {}, db: {}",
			ts,
			crate::vs::conv::versionstamp_to_u64(&vs),
			ns,
			db
		);

		// Ensure there are no keys after the ts_key
		// Otherwise we can go back in time!
		let ts_key = crate::key::database::ts::new(ns, db, ts);
		let begin = ts_key.encode()?;
		let end = crate::key::database::ts::suffix(ns, db);
		let ts_pairs: Vec<(Vec<u8>, Vec<u8>)> = self.getr(begin..end, u32::MAX).await?;
		let latest_ts_pair = ts_pairs.last();
		if let Some((k, _)) = latest_ts_pair {
			#[cfg(debug_assertions)]
			trace!(
				"There already was a greater committed timestamp {} in ns: {}, db: {} found: {}",
				ts,
				ns,
				db,
				sprint_key(k)
			);
			let k = crate::key::database::ts::Ts::decode(k)?;
			let latest_ts = k.ts;
			if latest_ts >= ts {
				return Err(Error::Internal(
					"ts is less than or equal to the latest ts".to_string(),
				));
			}
		}
		self.set(ts_key, vs).await?;
		Ok(vs)
	}

	pub(crate) async fn get_versionstamp_from_timestamp(
		&mut self,
		ts: u64,
		ns: &str,
		db: &str,
		_lock: bool,
	) -> Result<Option<Versionstamp>, Error> {
		let start = crate::key::database::ts::prefix(ns, db);
		let ts_key = crate::key::database::ts::new(ns, db, ts + 1);
		let end = ts_key.encode()?;
		let ts_pairs = self.getr(start..end, u32::MAX).await?;
		let latest_ts_pair = ts_pairs.last();
		if let Some((_, v)) = latest_ts_pair {
			if v.len() == 10 {
				let mut sl = [0u8; 10];
				sl.copy_from_slice(v);
				return Ok(Some(sl));
			} else {
				return Err(Error::Internal("versionstamp is not 10 bytes".to_string()));
			}
		}
		Ok(None)
	}

	// --------------------------------------------------
	// Private methods
	// --------------------------------------------------

	#[allow(unused_variables)]
	fn check_level(&mut self, check: Check) {
		#![allow(unused_variables)]
		match self {
			#[cfg(feature = "kv-mem")]
			Transaction {
				inner: Inner::Mem(ref mut v),
				..
			} => v.check_level(check),
			#[cfg(feature = "kv-rocksdb")]
			Transaction {
				inner: Inner::RocksDB(ref mut v),
				..
			} => v.check_level(check),
			#[cfg(feature = "kv-speedb")]
			Transaction {
				inner: Inner::SpeeDB(ref mut v),
				..
			} => v.check_level(check),
			#[cfg(feature = "kv-indxdb")]
			Transaction {
				inner: Inner::IndxDB(ref mut v),
				..
			} => v.check_level(check),
			#[cfg(feature = "kv-tikv")]
			Transaction {
				inner: Inner::TiKV(ref mut v),
				..
			} => v.check_level(check),
			#[cfg(feature = "kv-fdb")]
			Transaction {
				inner: Inner::FoundationDB(ref mut v),
				..
			} => v.check_level(check),
			#[cfg(feature = "kv-surrealkv")]
			Transaction {
				inner: Inner::SurrealKV(v),
				..
			} => v.set_check_level(check),
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	#[cfg(debug_assertions)]
	#[allow(unused)]
	#[doc(hidden)]
	pub async fn print_all(&mut self) {
		let mut next_page =
			Some(ScanPage::from(crate::key::root::ns::prefix()..b"\xff\xff\xff".to_vec()));
		println!("Start print all");
		while next_page.is_some() {
			let res = self.scan_paged(next_page.unwrap(), 1000).await.unwrap();
			for (k, _) in res.values {
				println!("{}", sprint_key(&k));
			}
			next_page = res.next_page;
		}
		println!("End print all");
	}
}

#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests {
	use crate::key::database::all::All;
	use crate::key::database::tb::Tb;
	use crate::{
		kvs::{Datastore, LockType::*, TransactionType::*},
		sql::{statements::DefineUserStatement, Base},
	};

	#[tokio::test]
	async fn test_get_root_user() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();

		// Retrieve non-existent KV user
		let res = txn.get_root_user("nonexistent").await;
		assert_eq!(res.err().unwrap().to_string(), "The root user 'nonexistent' does not exist");

		// Create KV user and retrieve it
		let data = DefineUserStatement {
			name: "user".into(),
			base: Base::Root,
			..Default::default()
		};
		let key = crate::key::root::us::new("user");
		txn.set(key, data.to_owned()).await.unwrap();
		let res = txn.get_root_user("user").await.unwrap();
		assert_eq!(res, data);
		txn.commit().await.unwrap()
	}

	#[tokio::test]
	async fn test_get_ns_user() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();

		// Retrieve non-existent NS user
		let res = txn.get_ns_user("ns", "nonexistent").await;
		assert_eq!(
			res.err().unwrap().to_string(),
			"The user 'nonexistent' does not exist in the namespace 'ns'"
		);

		// Create NS user and retrieve it
		let data = DefineUserStatement {
			name: "user".into(),
			base: Base::Ns,
			..Default::default()
		};

		let key = crate::key::namespace::us::new("ns", "user");
		txn.set(key, data.to_owned()).await.unwrap();
		let res = txn.get_ns_user("ns", "user").await.unwrap();
		assert_eq!(res, data);
		txn.commit().await.unwrap();
	}

	#[tokio::test]
	async fn test_get_db_user() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();

		// Retrieve non-existent DB user
		let res = txn.get_db_user("ns", "db", "nonexistent").await;
		assert_eq!(
			res.err().unwrap().to_string(),
			"The user 'nonexistent' does not exist in the database 'db'"
		);

		// Create DB user and retrieve it
		let data = DefineUserStatement {
			name: "user".into(),
			base: Base::Db,
			..Default::default()
		};

		let key = crate::key::database::us::new("ns", "db", "user");
		txn.set(key, data.to_owned()).await.unwrap();
		let res = txn.get_db_user("ns", "db", "user").await.unwrap();
		assert_eq!(res, data);
		txn.commit().await.unwrap();
	}

	#[tokio::test]
	async fn test_all_root_users() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();

		// When there are no users
		let res = txn.all_root_users().await.unwrap();
		assert_eq!(res.len(), 0);

		// When there are users
		let data = DefineUserStatement {
			name: "user".into(),
			base: Base::Root,
			..Default::default()
		};

		let key1 = crate::key::root::us::new("user1");
		let key2 = crate::key::root::us::new("user2");
		txn.set(key1, data.to_owned()).await.unwrap();
		txn.set(key2, data.to_owned()).await.unwrap();
		let res = txn.all_root_users().await.unwrap();

		assert_eq!(res.len(), 2);
		assert_eq!(res[0], data);
		txn.commit().await.unwrap();
	}

	#[tokio::test]
	async fn test_all_ns_users() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();

		// When there are no users
		let res = txn.all_ns_users("ns").await.unwrap();
		assert_eq!(res.len(), 0);

		// When there are users
		let data = DefineUserStatement {
			name: "user".into(),
			base: Base::Ns,
			..Default::default()
		};

		let key1 = crate::key::namespace::us::new("ns", "user1");
		let key2 = crate::key::namespace::us::new("ns", "user2");
		txn.set(key1, data.to_owned()).await.unwrap();
		txn.set(key2, data.to_owned()).await.unwrap();

		txn.cache.clear();

		let res = txn.all_ns_users("ns").await.unwrap();

		assert_eq!(res.len(), 2);
		assert_eq!(res[0], data);
		txn.commit().await.unwrap();
	}

	#[tokio::test]
	async fn test_all_db_users() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();

		// When there are no users
		let res = txn.all_db_users("ns", "db").await.unwrap();
		assert_eq!(res.len(), 0);

		// When there are users
		let data = DefineUserStatement {
			name: "user".into(),
			base: Base::Db,
			..Default::default()
		};

		let key1 = crate::key::database::us::new("ns", "db", "user1");
		let key2 = crate::key::database::us::new("ns", "db", "user2");
		txn.set(key1, data.to_owned()).await.unwrap();
		txn.set(key2, data.to_owned()).await.unwrap();

		txn.cache.clear();

		let res = txn.all_db_users("ns", "db").await.unwrap();

		assert_eq!(res.len(), 2);
		assert_eq!(res[0], data);
		txn.commit().await.unwrap();
	}

	#[tokio::test]
	async fn test_seqs() {
		let ds = Datastore::new("memory").await.unwrap();

		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
		let nsid = txn.get_next_ns_id().await.unwrap();
		txn.complete_changes(false).await.unwrap();
		txn.commit().await.unwrap();
		assert_eq!(nsid, 0);

		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
		let dbid = txn.get_next_db_id(nsid).await.unwrap();
		txn.complete_changes(false).await.unwrap();
		txn.commit().await.unwrap();
		assert_eq!(dbid, 0);

		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
		let tbid1 = txn.get_next_tb_id(nsid, dbid).await.unwrap();
		txn.complete_changes(false).await.unwrap();
		txn.commit().await.unwrap();
		assert_eq!(tbid1, 0);

		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
		let tbid2 = txn.get_next_tb_id(nsid, dbid).await.unwrap();
		txn.complete_changes(false).await.unwrap();
		txn.commit().await.unwrap();
		assert_eq!(tbid2, 1);

		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
		txn.remove_tb_id(nsid, dbid, tbid1).await.unwrap();
		txn.complete_changes(false).await.unwrap();
		txn.commit().await.unwrap();

		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
		txn.remove_db_id(nsid, dbid).await.unwrap();
		txn.complete_changes(false).await.unwrap();
		txn.commit().await.unwrap();

		let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
		txn.remove_ns_id(nsid).await.unwrap();
		txn.complete_changes(false).await.unwrap();
		txn.commit().await.unwrap();
	}

	#[tokio::test]
	async fn test_delp() {
		let ds = Datastore::new("memory").await.unwrap();
		// Create entries
		{
			let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
			for i in 0..2500 {
				let t = format!("{i}");
				let tb = Tb::new("test", "test", &t);
				txn.set(tb, vec![]).await.unwrap();
			}
			txn.commit().await.unwrap();
		}

		let beg = crate::key::database::tb::prefix("test", "test");
		let end = crate::key::database::tb::suffix("test", "test");
		let rng = beg..end;

		// Check we have the table keys
		{
			let mut txn = ds.transaction(Read, Optimistic).await.unwrap();
			let res = txn.getr(rng.clone(), u32::MAX).await.unwrap();
			assert_eq!(res.len(), 2500);
		}

		// Delete using the prefix
		{
			let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
			let all = All::new("test", "test");
			txn.delp(all, u32::MAX).await.unwrap();
			txn.commit().await.unwrap();
		}

		// Check we don't have any table key anymore
		{
			let mut txn = ds.transaction(Read, Optimistic).await.unwrap();
			let res = txn.getr(rng, u32::MAX).await.unwrap();
			assert_eq!(res.len(), 0);
		}
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod tx_test {
	use crate::kvs::lq_structs::{LqEntry, TrackedResult};
	use crate::kvs::Datastore;
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::TransactionType::Write;
	use crate::sql;
	use crate::sql::statements::LiveStatement;
	use crate::sql::Value;

	#[tokio::test]
	pub async fn lqs_can_be_submitted_and_read() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();

		// Create live query data
		let node_id = uuid::uuid!("d2715187-9d1a-49a5-9b0a-b496035b6c21");
		let lq_entry = LqEntry {
			live_id: sql::Uuid::new_v4(),
			ns: "namespace".to_string(),
			db: "database".to_string(),
			stm: LiveStatement {
				id: sql::Uuid::new_v4(),
				node: sql::uuid::Uuid(node_id),
				expr: Default::default(),
				what: Default::default(),
				cond: None,
				fetch: None,
				archived: None,
				session: Some(Value::None),
				auth: None,
			},
		};
		tx.pre_commit_register_async_event(TrackedResult::LiveQuery(lq_entry.clone())).unwrap();

		tx.commit().await.unwrap();

		// Verify data
		let live_queries = tx.consume_pending_live_queries();
		assert_eq!(live_queries.len(), 1);
		assert_eq!(live_queries[0], TrackedResult::LiveQuery(lq_entry));
	}
}
