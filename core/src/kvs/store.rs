use super::Convert;
use super::Key;
use super::Val;
use crate::err::Error;
use crate::idg::u32::U32;
use crate::key::error::KeyCategory;
use crate::kvs::Transaction;
use crate::sql::statements::DefineAccessStatement;
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::statements::DefineDatabaseStatement;
use crate::sql::statements::DefineEventStatement;
use crate::sql::statements::DefineFieldStatement;
use crate::sql::statements::DefineFunctionStatement;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::statements::DefineModelStatement;
use crate::sql::statements::DefineNamespaceStatement;
use crate::sql::statements::DefineParamStatement;
use crate::sql::statements::DefineTableStatement;
use crate::sql::statements::DefineUserStatement;
use crate::sql::statements::LiveStatement;
use crate::sql::Id;
use crate::sql::Value;
use futures::lock::Mutex;
use futures::lock::MutexGuard;
use futures::stream::Stream;
use futures::Future;
use quick_cache::sync::Cache;
use quick_cache::Weighter;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone)]
struct EntryWeighter;

impl Weighter<Key, Entry> for EntryWeighter {
	fn weight(&self, _key: &Key, val: &Entry) -> u32 {
		match val {
			// Value entries all have the same weight,
			// and can be evicted whenever necessary.
			// We could improve this, by calculating
			// the precise weight of a Value (when
			// deserialising), and using this size to
			// determine the actual cache weight.
			Entry::Val(_) => 1,
			// We don't want to evict other entries
			// so we set the weight to 0 which will
			// prevent entries being evicted, unless
			// specifically removed from the cache.
			_ => 0,
		}
	}
}

pub enum Scanner<'a> {
	// The initial state of this scanner
	Begin {
		/// The store which started this range scan
		store: &'a Store,
		/// The number of keys to fetch at once
		batch: u32,
		// The key range for this range scan
		range: Range<Key>,
	},
	// The state when a future has completed
	Ready {
		/// The store which started this range scan
		store: &'a Store,
		/// The number of keys to fetch at once
		batch: u32,
		// The key range for this range scan
		range: Range<Key>,
		// The results from the last range scan
		results: VecDeque<(Key, Val)>,
	},
	// The state for when a future is being polled
	Pending {
		/// The store which started this range scan
		store: &'a Store,
		/// The number of keys to fetch at once
		batch: u32,
		// The key range for this range scan
		range: Range<Key>,
		// The currently awaiting range scan future
		future: Pin<Box<dyn Future<Output = Result<Vec<(Key, Val)>, Error>> + 'a>>,
	},
	// This scanner is complete
	Complete,
	// Used internally
	Internal,
}

impl<'a> Stream for Scanner<'a> {
	type Item = Result<(Key, Val), Error>;
	fn poll_next(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Option<Result<(Key, Val), Error>>> {
		// Take ownership of the pointed
		let this = std::mem::replace(&mut *self, Self::Internal);
		//
		match this {
			// The initial state of this scanner
			Self::Begin {
				store,
				batch,
				range,
			} => {
				// Set the max number of results to fetch
				let num = std::cmp::min(1000, batch);
				// Set the next state of the scanner
				self.set(Self::Pending {
					store,
					batch,
					range: range.clone(),
					future: Box::pin(store.scan(range, num)),
				});
				// Mark this async stream as pending
				Poll::Pending
			}
			// The future has finished and we have some results
			Self::Ready {
				store,
				batch,
				mut range,
				mut results,
			} => match results.pop_front() {
				// We still have results, so return a result
				Some(v) => {
					// Set the next state of the scanner
					self.set(Self::Ready {
						store,
						batch,
						range,
						results,
					});
					// Return the first result
					Poll::Ready(Some(Ok(v)))
				}
				// No more results so let's fetch some more
				None => {
					range.end.push(0x00);
					// Set the max number of results to fetch
					let num = std::cmp::min(1000, batch);
					// Set the next state of the scanner
					self.set(Self::Pending {
						store,
						batch,
						range: range.clone(),
						future: Box::pin(store.scan(range, num)),
					});
					// Mark this async stream as pending
					Poll::Pending
				}
			},
			// We are waiting for a future to resolve
			Self::Pending {
				store,
				batch,
				range,
				mut future,
			} => match future.as_mut().poll(cx) {
				// The future has not yet completed
				Poll::Pending => {
					// Set the next state of the scanner
					self.set(Self::Pending {
						store,
						batch,
						range,
						future,
					});
					// Mark this async stream as pending
					Poll::Pending
				}
				// The future has now completed fully
				Poll::Ready(v) => match v {
					// There was an error with the range fetch
					Err(e) => {
						// Mark this scanner as complete
						self.set(Self::Complete);
						// Return the received error
						Poll::Ready(Some(Err(e)))
					}
					// The range was fetched successfully
					Ok(v) => match v.is_empty() {
						// There are no more results to stream
						true => {
							// Mark this scanner as complete
							self.set(Self::Complete);
							// Mark this stream as complete
							Poll::Ready(None)
						}
						// There are results which need streaming
						false => {
							// Store the fetched range results
							let mut results = VecDeque::from(v);
							// Remove the first result to return
							let item = results.pop_front().unwrap();
							// Set the next state of the scanner
							self.set(Self::Ready {
								store,
								batch,
								range,
								results,
							});
							// Return the first result
							Poll::Ready(Some(Ok(item)))
						}
					},
				},
			},
			// This range scan is completed
			Self::Complete => {
				// Mark this scanner as complete
				self.set(Self::Complete);
				// Mark this stream as complete
				Poll::Ready(None)
			}
			// This state should never occur
			Self::Internal => unreachable!(),
		}
	}
}

#[derive(Clone)]
#[non_exhaustive]
pub enum Entry {
	// Single definitions
	Any(Arc<dyn Any + Send + Sync>),
	// Multi definitions
	Azs(Arc<[DefineAnalyzerStatement]>),
	Das(Arc<[DefineAccessStatement]>),
	Dbs(Arc<[DefineDatabaseStatement]>),
	Dus(Arc<[DefineUserStatement]>),
	Evs(Arc<[DefineEventStatement]>),
	Fcs(Arc<[DefineFunctionStatement]>),
	Fds(Arc<[DefineFieldStatement]>),
	Fts(Arc<[DefineTableStatement]>),
	Ixs(Arc<[DefineIndexStatement]>),
	Lvs(Arc<[LiveStatement]>),
	Mls(Arc<[DefineModelStatement]>),
	Nas(Arc<[DefineAccessStatement]>),
	Nss(Arc<[DefineNamespaceStatement]>),
	Nus(Arc<[DefineUserStatement]>),
	Pas(Arc<[DefineParamStatement]>),
	Rus(Arc<[DefineUserStatement]>),
	Tbs(Arc<[DefineTableStatement]>),
	// Any value
	Val(Arc<Value>),
	// Sequences
	Seq(U32),
}

impl Entry {
	/// Convert this entry into an Arc of a certain type
	fn into_type<T: Send + Sync + 'static>(self: Entry) -> Arc<T> {
		match self {
			Entry::Any(v) => v.downcast::<T>().unwrap(),
			_ => unreachable!(),
		}
	}
	fn into_rus(self) -> Arc<[DefineUserStatement]> {
		match self {
			Entry::Rus(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_nss(self) -> Arc<[DefineNamespaceStatement]> {
		match self {
			Entry::Nss(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_nas(self) -> Arc<[DefineAccessStatement]> {
		match self {
			Entry::Nas(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_nus(self) -> Arc<[DefineUserStatement]> {
		match self {
			Entry::Nus(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_dbs(self) -> Arc<[DefineDatabaseStatement]> {
		match self {
			Entry::Dbs(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_das(self) -> Arc<[DefineAccessStatement]> {
		match self {
			Entry::Das(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_dus(self) -> Arc<[DefineUserStatement]> {
		match self {
			Entry::Dus(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_azs(self) -> Arc<[DefineAnalyzerStatement]> {
		match self {
			Entry::Azs(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_fcs(self) -> Arc<[DefineFunctionStatement]> {
		match self {
			Entry::Fcs(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_pas(self) -> Arc<[DefineParamStatement]> {
		match self {
			Entry::Pas(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_mls(self) -> Arc<[DefineModelStatement]> {
		match self {
			Entry::Mls(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_tbs(self) -> Arc<[DefineTableStatement]> {
		match self {
			Entry::Tbs(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_evs(self) -> Arc<[DefineEventStatement]> {
		match self {
			Entry::Evs(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_fds(self) -> Arc<[DefineFieldStatement]> {
		match self {
			Entry::Fds(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_ixs(self) -> Arc<[DefineIndexStatement]> {
		match self {
			Entry::Ixs(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_fts(self) -> Arc<[DefineTableStatement]> {
		match self {
			Entry::Fts(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_lvs(self) -> Arc<[LiveStatement]> {
		match self {
			Entry::Lvs(v) => v,
			_ => unreachable!(),
		}
	}
	fn into_val(self) -> Arc<Value> {
		match self {
			Entry::Val(v) => v,
			_ => unreachable!(),
		}
	}
}

#[non_exhaustive]
pub struct Store {
	/// The query cache for this store
	qc: Cache<Key, Entry, EntryWeighter>,
	/// The underlying transactions for this store
	tx: Mutex<Transaction>,
}

impl Store {
	/// Create a new query store
	pub async fn new(tx: Transaction) -> Store {
		Store {
			tx: Mutex::new(tx),
			qc: Cache::with_weighter(10_000, 10_000, EntryWeighter),
		}
	}

	/// Retrieve the underlying transaction
	pub async fn tx(&self) -> MutexGuard<'_, Transaction> {
		self.tx.lock().await
	}

	/// Check if transaction is finished.
	///
	/// If the transaction has been cancelled or committed,
	/// then this function will return [`true`], and any further
	/// calls to functions on this transaction will result
	/// in a [`Error::TxFinished`] error.
	pub async fn closed(&self) -> bool {
		self.tx.lock().await.closed().await
	}

	/// Cancel a transaction.
	///
	/// This reverses all changes made within the transaction.
	pub async fn cancel(&self) -> Result<(), Error> {
		self.tx.lock().await.cancel().await
	}

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	pub async fn commit(&self) -> Result<(), Error> {
		self.tx.lock().await.commit().await
	}

	/// Check if a key exists in the datastore.
	pub async fn exi<K>(&self, key: K) -> Result<bool, Error>
	where
		K: Into<Key> + Debug,
	{
		self.tx.lock().await.exi(key).await
	}

	/// Fetch a key from the datastore.
	pub async fn get<K>(&self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<Key> + Debug,
	{
		self.tx.lock().await.get(key).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in batches of 1000.
	pub async fn getr<K>(&self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Debug,
	{
		self.tx.lock().await.getr(rng, limit).await
	}

	/// Retrieve a specific prefix of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in batches of 1000.
	pub async fn getp<K>(&self, key: K, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Debug,
	{
		self.tx.lock().await.getp(key, limit).await
	}

	/// Delete a key from the datastore.
	pub async fn del<K>(&self, key: K) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		self.tx.lock().await.del(key).await
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in batches of 1000.
	pub async fn delr<K>(&self, rng: Range<K>, limit: u32) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		self.tx.lock().await.delr(rng, limit).await
	}

	/// Delete a prefix of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in batches of 1000.
	pub async fn delp<K>(&self, key: K, limit: u32) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		self.tx.lock().await.delp(key, limit).await
	}

	/// Delete a key from the datastore if the current value matches a condition.
	///
	/// Beware that this method opens and drops a lock
	pub async fn delc<K, V>(&self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		self.tx.lock().await.delc(key, chk).await
	}

	/// Insert or update a key in the datastore.
	pub async fn set<K, V>(&self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		self.tx.lock().await.set(key, val).await
	}

	/// Insert a key if it doesn't exist in the datastore.
	pub async fn put<K, V>(&self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		self.tx.lock().await.put(KeyCategory::Unknown, key, val).await
	}

	/// Update a key in the datastore if the current value matches a condition.
	pub async fn putc<K, V>(&self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		self.tx.lock().await.putc(key, val, chk).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single request to the underlying datastore.
	pub async fn scan<K>(&self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Debug,
	{
		self.tx.lock().await.scan(rng, limit).await
	}

	/// Retrieve a stream over a specific range of keys in the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple requests to the underlying datastore.
	pub fn range<K>(&self, rng: Range<K>, batch: u32) -> impl Stream + '_
	where
		K: Into<Key> + Debug,
	{
		Scanner::Begin {
			store: self,
			range: Range {
				start: rng.start.into(),
				end: rng.end.into(),
			},
			batch,
		}
	}

	/// Retrieve all ROOT level users in a datastore.
	pub async fn all_root_users(&self) -> Result<Arc<[DefineUserStatement]>, Error> {
		let key = crate::key::root::us::prefix();
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::root::us::suffix();
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Rus(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_rus())
	}

	/// Retrieve all namespace definitions in a datastore.
	pub async fn all_ns(&self) -> Result<Arc<[DefineNamespaceStatement]>, Error> {
		let key = crate::key::root::ns::prefix();
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::root::ns::suffix();
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Nss(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_nss())
	}

	/// Retrieve all namespace user definitions for a specific namespace.
	pub async fn all_ns_users(&self, ns: &str) -> Result<Arc<[DefineUserStatement]>, Error> {
		let key = crate::key::namespace::us::prefix(ns);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::namespace::us::suffix(ns);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Nus(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_nus())
	}

	/// Retrieve all namespace token definitions for a specific namespace.
	pub async fn all_ns_accesses(&self, ns: &str) -> Result<Arc<[DefineAccessStatement]>, Error> {
		let key = crate::key::namespace::ac::prefix(ns);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::namespace::ac::suffix(ns);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Nas(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_nas())
	}

	/// Retrieve all database definitions for a specific namespace.
	pub async fn all_db(&self, ns: &str) -> Result<Arc<[DefineDatabaseStatement]>, Error> {
		let key = crate::key::namespace::db::prefix(ns);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::namespace::db::suffix(ns);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Dbs(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_dbs())
	}

	/// Retrieve all database user definitions for a specific database.
	pub async fn all_db_users(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineUserStatement]>, Error> {
		let key = crate::key::database::us::prefix(ns, db);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::database::us::suffix(ns, db);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Dus(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_dus())
	}

	/// Retrieve all database token definitions for a specific database.
	pub async fn all_db_accesses(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineAccessStatement]>, Error> {
		let key = crate::key::database::ac::prefix(ns, db);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::database::ac::suffix(ns, db);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Das(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_das())
	}

	/// Retrieve all analyzer definitions for a specific database.
	pub async fn all_db_analyzers(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineAnalyzerStatement]>, Error> {
		let key = crate::key::database::az::prefix(ns, db);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::database::az::suffix(ns, db);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Azs(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_azs())
	}

	/// Retrieve all function definitions for a specific database.
	pub async fn all_db_functions(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineFunctionStatement]>, Error> {
		let key = crate::key::database::fc::prefix(ns, db);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::database::fc::suffix(ns, db);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Fcs(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_fcs())
	}

	/// Retrieve all param definitions for a specific database.
	pub async fn all_db_params(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineParamStatement]>, Error> {
		let key = crate::key::database::pa::prefix(ns, db);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::database::pa::suffix(ns, db);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Pas(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_pas())
	}

	/// Retrieve all model definitions for a specific database.
	pub async fn all_db_models(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineModelStatement]>, Error> {
		let key = crate::key::database::ml::prefix(ns, db);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::database::ml::suffix(ns, db);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Mls(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_mls())
	}

	/// Retrieve all table definitions for a specific database.
	pub async fn all_tb(&self, ns: &str, db: &str) -> Result<Arc<[DefineTableStatement]>, Error> {
		let key = crate::key::database::tb::prefix(ns, db);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::database::tb::suffix(ns, db);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Tbs(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_tbs())
	}

	/// Retrieve all event definitions for a specific table.
	pub async fn all_tb_events(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineEventStatement]>, Error> {
		let key = crate::key::table::ev::prefix(ns, db, tb);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::table::ev::suffix(ns, db, tb);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Evs(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_evs())
	}

	/// Retrieve all field definitions for a specific table.
	pub async fn all_tb_fields(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineFieldStatement]>, Error> {
		let key = crate::key::table::fd::prefix(ns, db, tb);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::table::fd::suffix(ns, db, tb);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Fds(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_fds())
	}

	/// Retrieve all index definitions for a specific table.
	pub async fn all_tb_indexes(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineIndexStatement]>, Error> {
		let key = crate::key::table::ix::prefix(ns, db, tb);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::table::ix::suffix(ns, db, tb);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Ixs(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_ixs())
	}

	/// Retrieve all view definitions for a specific table.
	pub async fn all_tb_views(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineTableStatement]>, Error> {
		let key = crate::key::table::ft::prefix(ns, db, tb);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::table::ft::suffix(ns, db, tb);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Fts(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_fts())
	}

	/// Retrieve all live definitions for a specific table.
	pub async fn all_tb_lives(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[LiveStatement]>, Error> {
		let key = crate::key::table::lq::prefix(ns, db, tb);
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let end = crate::key::table::lq::suffix(ns, db, tb);
				let val = self.getr(key..end, u32::MAX).await?;
				let val = val.convert().into();
				let val = Entry::Lvs(Arc::clone(&val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_lvs())
	}

	/// Retrieve a specific namespace definition.
	pub async fn get_ns(&self, ns: &str) -> Result<Arc<DefineNamespaceStatement>, Error> {
		let key = crate::key::root::ns::new(ns).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::NsNotFound {
					value: ns.to_owned(),
				})?;
				let val: DefineNamespaceStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific namespace user definition.
	pub async fn get_ns_user(
		&self,
		ns: &str,
		user: &str,
	) -> Result<Arc<DefineUserStatement>, Error> {
		let key = crate::key::namespace::us::new(ns, user).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::UserNsNotFound {
					value: user.to_owned(),
					ns: ns.to_owned(),
				})?;
				let val: DefineUserStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific namespace token definition.
	pub async fn get_ns_access(
		&self,
		ns: &str,
		nt: &str,
	) -> Result<Arc<DefineAccessStatement>, Error> {
		let key = crate::key::namespace::ac::new(ns, nt).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::NaNotFound {
					value: nt.to_owned(),
				})?;
				let val: DefineAccessStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific database definition.
	pub async fn get_db(&self, ns: &str, db: &str) -> Result<Arc<DefineDatabaseStatement>, Error> {
		let key = crate::key::namespace::db::new(ns, db).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::DbNotFound {
					value: db.to_owned(),
				})?;
				let val: DefineDatabaseStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific user definition from a database.
	pub async fn get_db_user(
		&self,
		ns: &str,
		db: &str,
		user: &str,
	) -> Result<Arc<DefineUserStatement>, Error> {
		let key = crate::key::database::us::new(ns, db, user).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::UserDbNotFound {
					value: user.to_owned(),
					ns: ns.to_owned(),
					db: db.to_owned(),
				})?;
				let val: DefineUserStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific model definition from a database.
	pub async fn get_db_model(
		&self,
		ns: &str,
		db: &str,
		ml: &str,
		vn: &str,
	) -> Result<Arc<DefineModelStatement>, Error> {
		let key = crate::key::database::ml::new(ns, db, ml, vn).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::MlNotFound {
					value: format!("{ml}<{vn}>"),
				})?;
				let val: DefineModelStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific database token definition.
	pub async fn get_db_token(
		&self,
		ns: &str,
		db: &str,
		dt: &str,
	) -> Result<Arc<DefineAccessStatement>, Error> {
		let key = crate::key::database::ac::new(ns, db, dt).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::DaNotFound {
					value: dt.to_owned(),
				})?;
				let val: DefineAccessStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific analyzer definition.
	pub async fn get_db_analyzer(
		&self,
		ns: &str,
		db: &str,
		az: &str,
	) -> Result<Arc<DefineAnalyzerStatement>, Error> {
		let key = crate::key::database::az::new(ns, db, az).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::AzNotFound {
					value: az.to_owned(),
				})?;
				let val: DefineAnalyzerStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific function definition from a database.
	pub async fn get_db_function(
		&self,
		ns: &str,
		db: &str,
		fc: &str,
	) -> Result<Arc<DefineFunctionStatement>, Error> {
		let key = crate::key::database::fc::new(ns, db, fc).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::FcNotFound {
					value: fc.to_owned(),
				})?;
				let val: DefineFunctionStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific function definition from a database.
	pub async fn get_db_param(
		&self,
		ns: &str,
		db: &str,
		pa: &str,
	) -> Result<Arc<DefineParamStatement>, Error> {
		let key = crate::key::database::pa::new(ns, db, pa).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::PaNotFound {
					value: pa.to_owned(),
				})?;
				let val: DefineParamStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific table definition.
	pub async fn get_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<DefineTableStatement>, Error> {
		let key = crate::key::database::tb::new(ns, db, tb).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::TbNotFound {
					value: tb.to_owned(),
				})?;
				let val: DefineTableStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve an event for a table.
	pub async fn get_tb_event(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		ev: &str,
	) -> Result<Arc<DefineEventStatement>, Error> {
		let key = crate::key::table::ev::new(ns, db, tb, ev).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::EvNotFound {
					value: ev.to_owned(),
				})?;
				let val: DefineEventStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a field for a table.
	pub async fn get_tb_field(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		fd: &str,
	) -> Result<Arc<DefineFieldStatement>, Error> {
		let key = crate::key::table::fd::new(ns, db, tb, fd).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::FdNotFound {
					value: fd.to_owned(),
				})?;
				let val: DefineFieldStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve an index for a table.
	pub async fn get_tb_index(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		ix: &str,
	) -> Result<Arc<DefineIndexStatement>, Error> {
		let key = crate::key::table::ix::new(ns, db, tb, ix).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(qc) => {
				let val = self.get(key).await?.ok_or(Error::IxNotFound {
					value: ix.to_owned(),
				})?;
				let val: DefineIndexStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = qc.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Fetch a specific record value.
	pub async fn get_record(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		id: &Id,
	) -> Result<Arc<Value>, Error> {
		let key = crate::key::thing::new(ns, db, tb, id).encode()?;
		let res = self.qc.get_value_or_guard_async(&key).await;
		match res {
			// The entry is in the cache
			Ok(val) => Ok(val.into_val()),
			// The entry is not in the cache
			Err(qc) => match self.get(key).await? {
				// The value exists in the datastore
				Some(val) => {
					let val = Entry::Val(Arc::new(val.into()));
					let _ = qc.insert(val.clone());
					Ok(val.into_val())
				}
				// The value is not in the datastore
				None => Ok(Arc::new(Value::None)),
			},
		}
	}

	pub async fn set_record(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		id: &Id,
		val: Value,
	) -> Result<(), Error> {
		let key = crate::key::thing::new(ns, db, tb, id);
		let enc = crate::key::thing::new(ns, db, tb, id).encode()?;
		// Set the value in the datastore
		self.set(&key, &val).await?;
		// Set the value in the cache
		self.qc.insert(enc, Entry::Val(Arc::new(val)));
		// Return nothing
		Ok(())
	}

	/// Get or add a namespace with a default configuration, only if we are in dynamic mode.
	pub async fn get_or_add_ns(
		&self,
		ns: &str,
		strict: bool,
	) -> Result<Arc<DefineNamespaceStatement>, Error> {
		let key = crate::key::root::ns::new(ns);
		let enc = crate::key::root::ns::new(ns).encode()?;
		let res = self.qc.get_value_or_guard_async(&enc).await;
		Ok(match res {
			// The entry is in the cache
			Ok(val) => val,
			// The entry is not in the cache
			Err(qc) => {
				// Try to fetch the value from the datastore
				let res = self.get(&key).await?.ok_or(Error::NsNotFound {
					value: ns.to_owned(),
				});
				// Check whether the value exists in the datastore
				match res {
					// Store a new default value in the datastore
					Err(Error::NsNotFound {
						..
					}) if !strict => {
						let val = DefineNamespaceStatement {
							name: ns.to_owned().into(),
							..Default::default()
						};
						let val = {
							self.put(&key, &val).await?;
							Entry::Any(Arc::new(val))
						};
						let _ = qc.insert(val.clone());
						val
					}
					// Store the fetched value in the cache
					Ok(val) => {
						let val: DefineNamespaceStatement = val.into();
						let val = Entry::Any(Arc::new(val));
						let _ = qc.insert(val.clone());
						val
					}
					// Throw any received errors
					Err(err) => Err(err)?,
				}
			}
		}
		.into_type())
	}

	/// Get or add a database with a default configuration, only if we are in dynamic mode.
	pub async fn get_or_add_db(
		&self,
		ns: &str,
		db: &str,
		strict: bool,
	) -> Result<Arc<DefineDatabaseStatement>, Error> {
		let key = crate::key::namespace::db::new(ns, db);
		let enc = crate::key::namespace::db::new(ns, db).encode()?;
		let res = self.qc.get_value_or_guard_async(&enc).await;
		Ok(match res {
			// The entry is in the cache
			Ok(val) => val,
			// The entry is not in the cache
			Err(qc) => {
				// Try to fetch the value from the datastore
				let res = self.get(&key).await?.ok_or(Error::DbNotFound {
					value: db.to_owned(),
				});
				// Check whether the value exists in the datastore
				match res {
					// Store a new default value in the datastore
					Err(Error::DbNotFound {
						..
					}) if !strict => {
						let val = DefineDatabaseStatement {
							name: db.to_owned().into(),
							..Default::default()
						};
						let val = {
							self.put(&key, &val).await?;
							Entry::Any(Arc::new(val))
						};
						let _ = qc.insert(val.clone());
						val
					}
					// Store the fetched value in the cache
					Ok(val) => {
						let val: DefineDatabaseStatement = val.into();
						let val = Entry::Any(Arc::new(val));
						let _ = qc.insert(val.clone());
						val
					}
					// Throw any received errors
					Err(err) => Err(err)?,
				}
			}
		}
		.into_type())
	}

	/// Get or add a table with a default configuration, only if we are in dynamic mode.
	pub async fn get_or_add_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<Arc<DefineTableStatement>, Error> {
		let key = crate::key::database::tb::new(ns, db, tb);
		let enc = crate::key::database::tb::new(ns, db, tb).encode()?;
		let res = self.qc.get_value_or_guard_async(&enc).await;
		Ok(match res {
			// The entry is in the cache
			Ok(val) => val,
			// The entry is not in the cache
			Err(qc) => {
				// Try to fetch the value from the datastore
				let res = self.get(&key).await?.ok_or(Error::TbNotFound {
					value: tb.to_owned(),
				});
				// Check whether the value exists in the datastore
				match res {
					// Store a new default value in the datastore
					Err(Error::TbNotFound {
						..
					}) if !strict => {
						let val = DefineTableStatement {
							name: db.to_owned().into(),
							..Default::default()
						};
						let val = {
							self.put(&key, &val).await?;
							Entry::Any(Arc::new(val))
						};
						let _ = qc.insert(val.clone());
						val
					}
					// Store the fetched value in the cache
					Ok(val) => {
						let val: DefineTableStatement = val.into();
						let val = Entry::Any(Arc::new(val));
						let _ = qc.insert(val.clone());
						val
					}
					// Throw any received errors
					Err(err) => Err(err)?,
				}
			}
		}
		.into_type())
	}

	/// Ensure a specific table (and database, and namespace) exist.
	pub async fn check_ns_db_tb(
		&self,
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
				// Check the table exists
				self.get_tb(ns, db, tb).await?;
				// Everything is ok
				Ok(())
			}
		}
	}

	/// Clears all keys from the cache.
	pub fn clear<K>(&self) -> () {
		self.qc.clear()
	}
}
