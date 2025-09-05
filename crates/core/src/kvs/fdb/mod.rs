#![cfg(feature = "kv-fdb")]

mod cnf;

use std::ops::Range;
use std::sync::{Arc, LazyLock};

use anyhow::{Result, bail, ensure};
use foundationdb::options::{DatabaseOption, MutationType};
use foundationdb::{Database, RangeOption, Transaction as Tx};
use futures::StreamExt;

use crate::err::Error;
use crate::key::database::vs::VsKey;
use crate::key::debug::Sprintable;
use crate::kvs::savepoint::{SaveOperation, SavePoints, SavePrepare};
use crate::kvs::{Check, Key, Val};
use crate::vs::VersionStamp;

const TARGET: &str = "surrealdb::core::kvs::fdb";

const TIMESTAMP: [u8; 10] = [0x00; 10];

pub struct Datastore {
	db: Database,
	// The Database stored above, relies on the
	// foundationdb network being booted before
	// the client can be used. The return result
	// of the foundationdb::boot method is a
	// handle which must be dropped before the
	// program exits. This handle is stored on
	// the database so that it is held for the
	// duration of the programme. This pointer must
	// be declared last, so that it is dropped last.
	_fdbnet: Arc<foundationdb::api::NetworkAutoStop>,
}

pub struct Transaction {
	/// Is the transaction complete?
	done: bool,
	/// Should this transaction lock?
	lock: bool,
	/// Is the transaction writeable?
	write: bool,
	/// Should we check unhandled transactions?
	check: Check,
	/// The underlying datastore transaction
	inner: Option<Tx>,
	/// The save point implementation
	save_points: SavePoints,
}

impl Drop for Transaction {
	fn drop(&mut self) {
		if !self.done && self.write {
			match self.check {
				Check::None => {
					trace!("A transaction was dropped without being committed or cancelled");
				}
				Check::Warn => {
					warn!("A transaction was dropped without being committed or cancelled");
				}
				Check::Error => {
					error!("A transaction was dropped without being committed or cancelled");
				}
			}
		}
	}
}

impl Datastore {
	/// Open a new database
	///
	/// The `path` argument can be a local file path to a FoundationDB
	/// cluster file, or an empty string. If specified as an empty
	/// string, then the default cluster file placed at a system
	/// dependent location (defined by FoundationDB) will be used.
	/// See https://apple.github.io/foundationdb/administration.html
	/// for more information on cluster connection files.
	pub(crate) async fn new(path: &str) -> Result<Datastore> {
		// Initialize the FoundationDB Client API
		static NETWORK: LazyLock<Arc<foundationdb::api::NetworkAutoStop>> =
			LazyLock::new(|| Arc::new(unsafe { foundationdb::boot() }));
		// Store the network cancellation handle
		let _fdbnet = (*NETWORK).clone();
		// Configure and setup the database
		match foundationdb::Database::from_path(path) {
			Ok(db) => {
				// Set the transaction timeout
				info!(target: TARGET, "Setting transaction timeout: {}", *cnf::FOUNDATIONDB_TRANSACTION_TIMEOUT);
				db.set_option(DatabaseOption::TransactionTimeout(
					*cnf::FOUNDATIONDB_TRANSACTION_TIMEOUT,
				))
				.map_err(|e| Error::Ds(format!("Unable to set transaction timeout: {e}")))?;
				// Set the transaction retry liimt
				info!(target: TARGET, "Setting transaction retry limit: {}", *cnf::FOUNDATIONDB_TRANSACTION_RETRY_LIMIT);
				db.set_option(DatabaseOption::TransactionRetryLimit(
					*cnf::FOUNDATIONDB_TRANSACTION_RETRY_LIMIT,
				))
				.map_err(|e| Error::Ds(format!("Unable to set transaction retry limit: {e}")))?;
				// Set the transaction max retry delay
				info!(target: TARGET, "Setting maximum transaction retry delay: {}", *cnf::FOUNDATIONDB_TRANSACTION_MAX_RETRY_DELAY);
				db.set_option(DatabaseOption::TransactionMaxRetryDelay(
					*cnf::FOUNDATIONDB_TRANSACTION_MAX_RETRY_DELAY,
				))
				.map_err(|e| {
					Error::Ds(format!("Unable to set transaction max retry delay: {e}"))
				})?;
				Ok(Datastore {
					db,
					_fdbnet,
				})
			}
			Err(e) => Err(anyhow::Error::new(Error::Ds(e.to_string()))),
		}
	}

	/// Shutdown the database
	pub(crate) async fn shutdown(&self) -> Result<()> {
		// Nothing to do here
		Ok(())
	}

	/// Start a new transaction
	pub(crate) async fn transaction(
		&self,
		write: bool,
		lock: bool,
	) -> Result<Box<dyn crate::kvs::api::Transaction>> {
		// Specify the check level
		#[cfg(not(debug_assertions))]
		let check = Check::Warn;
		#[cfg(debug_assertions)]
		let check = Check::Error;
		// Create a new transaction
		match self.db.create_trx() {
			Ok(inner) => Ok(Box::new(Transaction {
				done: false,
				lock,
				check,
				write,
				inner: Some(inner),
				save_points: Default::default(),
			})),
			Err(e) => Err(anyhow::Error::new(Error::Tx(e.to_string()))),
		}
	}
}

impl Transaction {
	/// Each transaction uses `lock=true` to behave similarly to pessimistic
	/// locks in the same way that pessimistic transactions work in TiKV.
	/// Standard transactions in FoundationDB (where `snapshot=false`) behave
	/// behaves like a TiKV pessimistic transaction, by automatically retrying
	/// on commit conflicts at the client layer. In FoundationDB we assume
	/// that `lock=true` is effectively specifying that we should ensure
	/// transactions are serializable. If the transaction is writeable, we also
	/// assume that the user never wants to lose serializability, so we go with
	/// the standard FoundationDB serializable more in that scenario.
	#[inline(always)]
	fn snapshot(&self) -> bool {
		!self.write && !self.lock
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl super::api::Transaction for Transaction {
	fn kind(&self) -> &'static str {
		"fdb"
	}

	fn supports_reverse_scan(&self) -> bool {
		false
	}

	/// Behaviour if unclosed
	fn check_level(&mut self, check: Check) {
		self.check = check;
	}

	/// Check if closed
	fn closed(&self) -> bool {
		self.done
	}

	/// Check if writeable
	fn writeable(&self) -> bool {
		self.write
	}

	/// Cancel a transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn cancel(&mut self) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Mark this transaction as done
		self.done = true;
		// Cancel this transaction
		match self.inner.take() {
			Some(inner) => inner.cancel().reset(),
			None => fail!("Unable to cancel an already taken transaction"),
		};
		// Continue
		Ok(())
	}

	/// Commit a transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn commit(&mut self) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Mark this transaction as done
		self.done = true;
		// Commit this transaction
		match self.inner.take() {
			Some(inner) => inner.commit().await.map_err(Error::from)?,
			None => fail!("Unable to commit an already taken transaction"),
		};
		// Continue
		Ok(())
	}

	/// Check if a key exists
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists(&mut self, key: Key, version: Option<u64>) -> Result<bool> {
		// FoundationDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check the key
		let res = self.inner.as_ref().unwrap().get(&key, self.snapshot()).await?.is_some();
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get(&mut self, key: Key, version: Option<u64>) -> Result<Option<Val>> {
		// FoundationDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Get the key
		let res =
			self.inner.as_ref().unwrap().get(&key, self.snapshot()).await?.map(|v| v.to_vec());
		// Return result
		Ok(res)
	}

	/// Inserts or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&mut self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// FoundationDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Prepare the savepoint if any
		let prep = if self.save_points.is_some() {
			self.save_point_prepare(&key, version, SaveOperation::Set).await?
		} else {
			None
		};
		// Set the key
		self.inner.as_ref().unwrap().set(&key, &val);
		// Confirm the save point
		if let Some(prep) = prep {
			self.save_points.save(prep);
		}
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put(&mut self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// FoundationDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);

		// Hydrate the savepoint if any
		let prep = if self.save_points.is_some() {
			self.save_point_prepare(&key, version, SaveOperation::Put).await?
		} else {
			None
		};
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Get the existing value (if any)
		let key_exists = if let Some(SavePrepare::NewKey(_, sv)) = &prep {
			sv.get_val().is_some()
		} else {
			inner.get(&key, self.snapshot()).await?.is_some()
		};
		// If the key exists we return an error
		ensure!(!key_exists, Error::TxKeyAlreadyExists);
		// Set the key if empty
		inner.set(&key, &val);
		// Confirm the save point
		if let Some(prep) = prep {
			self.save_points.save(prep);
		}
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn putc(&mut self, key: Key, val: Val, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Hydrate the savepoint if any
		let prep = if self.save_points.is_some() {
			self.save_point_prepare(&key, None, SaveOperation::Put).await?
		} else {
			None
		};
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Get the existing value (if any)
		let current_val = if let Some(SavePrepare::NewKey(_, sv)) = &prep {
			sv.get_val().cloned()
		} else {
			inner.get(&key, self.snapshot()).await?.map(|v| v.to_vec())
		};
		// Set the key if valid
		match (current_val, chk) {
			(Some(v), Some(w)) if v == w => inner.set(&key, &val),
			(None, None) => inner.set(&key, &val),
			_ => bail!(Error::TxConditionNotMet),
		};
		// Confirm the save point
		if let Some(prep) = prep {
			self.save_points.save(prep);
		}
		// Return result
		Ok(())
	}

	/// Delete a key
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn del(&mut self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Hydrate the savepoint if any
		let prep = if self.save_points.is_some() {
			self.save_point_prepare(&key, None, SaveOperation::Del).await?
		} else {
			None
		};
		// Remove the key
		self.inner.as_ref().unwrap().clear(&key);
		// Confirm the save point
		if let Some(prep) = prep {
			self.save_points.save(prep);
		}
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Hydrate the savepoint if any
		let prep = if self.save_points.is_some() {
			self.save_point_prepare(&key, None, SaveOperation::Del).await?
		} else {
			None
		};
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Get the existing value (if any)
		let current_val = if let Some(SavePrepare::NewKey(_, sv)) = &prep {
			sv.get_val().cloned()
		} else {
			inner.get(&key, self.snapshot()).await?.map(|v| v.to_vec())
		};
		// Delete the key if valid
		match (current_val, chk) {
			(Some(v), Some(w)) if v == w => inner.clear(&key),
			(None, None) => inner.clear(&key),
			_ => bail!(Error::TxConditionNotMet),
		};
		// Confirm the save point
		if let Some(prep) = prep {
			self.save_points.save(prep);
		}
		// Return result
		Ok(())
	}

	/// Delete a range of keys from the databases
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn delr(&mut self, rng: Range<Key>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// TODO: Check if we need savepoint with ranges

		// Delete the key range
		self.inner.as_ref().unwrap().clear_range(&rng.start, &rng.end);
		// Return result
		Ok(())
	}

	/// Retrieve a range of keys from the databases
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		// FoundationDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();

		// Create result set
		let mut res = vec![];
		// Set the key range
		let opt = RangeOption {
			limit: Some(limit as usize),
			..RangeOption::from((rng.start.as_slice(), rng.end.as_slice()))
		};
		// Create the scan request
		let mut req = inner.get_ranges(opt, self.snapshot());
		// Scan the keys in the iterator
		while let Some(val) = req.next().await {
			for v in val?.into_iter() {
				res.push(Key::from(v.key()));
			}
		}
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the databases
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// FoundationDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Create result set
		let mut res = vec![];
		// Set the key range
		let opt = RangeOption {
			limit: Some(limit as usize),
			..RangeOption::from((rng.start.as_slice(), rng.end.as_slice()))
		};
		// Create the scan request
		let mut req = inner.get_ranges(opt, self.snapshot());
		// Scan the keys in the iterator
		while let Some(val) = req.next().await {
			for v in val?.into_iter() {
				res.push((Key::from(v.key()), Val::from(v.value())));
			}
		}
		// Return result
		Ok(res)
	}

	/// Obtain a new change timestamp for a key
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn get_timestamp(&mut self, _key: VsKey) -> Result<VersionStamp> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Get the current read version
		let res = self.inner.as_ref().unwrap().get_read_version().await?;
		// Convert to a version stamp
		let res = VersionStamp::from_u64(res as u64);
		// Return result
		Ok(res)
	}

	// Sets the value for a versionstamped key prefixed with the user-supplied key.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn set_versionstamp(
		&mut self,
		_ts_key: VsKey,
		prefix: Key,
		suffix: Key,
		val: Val,
	) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Build the key starting with the prefix
		let mut key: Key = prefix.clone();
		// Get the position of the timestamp
		let pos = key.len() as u32;
		// Append the timestamp placeholder
		key.extend_from_slice(&TIMESTAMP);
		// Append the suffix to the key
		key.extend(suffix);
		// Append the 4 byte placeholder position in little endian
		key.append(&mut pos.to_le_bytes().to_vec());
		// Set the versionstamp key
		self.inner.as_ref().unwrap().atomic_op(&key, &val, MutationType::SetVersionstampedKey);
		// Return result
		Ok(())
	}

	fn get_save_points(&mut self) -> &mut SavePoints {
		&mut self.save_points
	}
}
