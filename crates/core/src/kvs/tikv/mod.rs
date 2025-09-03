#![cfg(feature = "kv-tikv")]

mod cnf;

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail, ensure};
use tikv::{CheckLevel, Config, TimestampExt, TransactionClient, TransactionOptions};

use crate::err::Error;
use crate::key::database::vs::VsKey;
use crate::key::debug::Sprintable;
use crate::kvs::key::KVKey;
use crate::kvs::savepoint::{SaveOperation, SavePoints, SavePrepare};
use crate::kvs::{Check, Key, Val};
use crate::vs::VersionStamp;

const TARGET: &str = "surrealdb::core::kvs::tikv";

pub struct Datastore {
	db: Pin<Arc<TransactionClient>>,
}

pub struct Transaction {
	// Is the transaction complete?
	done: bool,
	// Is the transaction writeable?
	write: bool,
	/// Should we check unhandled transactions?
	check: Check,
	/// The underlying datastore transaction
	inner: tikv::Transaction,
	/// The save point implementation
	save_points: SavePoints,
	// The above, supposedly 'static transaction
	// actually points here, so we need to ensure
	// the memory is kept alive. This pointer must
	// be declared last, so that it is dropped last.
	db: Pin<Arc<TransactionClient>>,
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
	pub(crate) async fn new(path: &str) -> Result<Datastore> {
		// Configure the client and keyspace
		let config = match *cnf::TIKV_API_VERSION {
			2 => match *cnf::TIKV_KEYSPACE {
				Some(ref keyspace) => {
					info!(target: TARGET, "Connecting to keyspace with cluster API V2: {keyspace}");
					Config::default().with_keyspace(keyspace)
				}
				None => {
					info!(target: TARGET, "Connecting to default keyspace with cluster API V2");
					Config::default().with_default_keyspace()
				}
			},
			1 => {
				info!(target: TARGET, "Connecting with cluster API V1");
				Config::default()
			}
			_ => bail!(Error::Ds("Invalid TiKV API version".into())),
		};
		// Set the default request timeout
		let config = config.with_timeout(Duration::from_secs(*cnf::TIKV_REQUEST_TIMEOUT));
		// Set the max decoding message size
		let config =
			config.with_grpc_max_decoding_message_size(*cnf::TIKV_GRPC_MAX_DECODING_MESSAGE_SIZE);
		// Create the client with the config
		let client = TransactionClient::new_with_config(vec![path], config);
		// Check for errors with the client
		match client.await {
			Ok(db) => Ok(Datastore {
				db: Arc::pin(db),
			}),
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
		// Set whether this should be an optimistic or pessimistic transaction
		let mut opt = if lock {
			TransactionOptions::new_pessimistic()
		} else {
			TransactionOptions::new_optimistic()
		};
		// Use async commit to determine transaction state earlier
		opt = match *cnf::TIKV_ASYNC_COMMIT {
			true => opt.use_async_commit(),
			_ => opt,
		};
		// Try to use one-phase commit if writing to only one region
		opt = match *cnf::TIKV_ONE_PHASE_COMMIT {
			true => opt.try_one_pc(),
			_ => opt,
		};
		// Set the behaviour when dropping an unfinished transaction
		opt = opt.drop_check(CheckLevel::Warn);
		// Set this transaction as read only if possible
		if !write {
			opt = opt.read_only();
		}
		// Specify the check level
		#[cfg(not(debug_assertions))]
		let check = Check::Warn;
		#[cfg(debug_assertions)]
		let check = Check::Error;
		// Create a new transaction
		match self.db.begin_with_options(opt).await {
			Ok(inner) => Ok(Box::new(Transaction {
				done: false,
				check,
				write,
				inner,
				db: self.db.clone(),
				save_points: Default::default(),
			})),
			Err(e) => Err(anyhow::Error::new(Error::Tx(e.to_string()))),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl super::api::Transaction for Transaction {
	fn kind(&self) -> &'static str {
		"tikv"
	}

	fn supports_reverse_scan(&self) -> bool {
		true
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
		if self.write {
			self.inner.rollback().await?;
		}
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
		if let Err(err) = self.inner.commit().await {
			if let Err(inner_err) = self.inner.rollback().await {
				error!("Transaction commit failed {} and rollback failed: {}", err, inner_err);
			}
			return Err(err.into());
		}
		// Continue
		Ok(())
	}

	/// Check if a key exists
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists(&mut self, key: Key, version: Option<u64>) -> Result<bool> {
		// TiKV does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check the key
		let res = self.inner.key_exists(key).await?;
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get(&mut self, key: Key, version: Option<u64>) -> Result<Option<Val>> {
		// TiKV does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Get the key
		let res = self.inner.get(key).await?;
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&mut self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// TiKV does not support versioned queries.
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
		self.inner.put(key, val).await?;
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
		// TiKV does not support versioned queries.
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
		// Get the existing value (if any)
		let key_exists = if let Some(SavePrepare::NewKey(_, sv)) = &prep {
			sv.get_val().is_some()
		} else {
			self.inner.key_exists(key.clone()).await?
		};
		// If the key exists we return an error
		ensure!(!key_exists, Error::TxKeyAlreadyExists);
		// Set the key if empty
		self.inner.put(key, val).await?;
		// Confirm the save point
		if let Some(prep) = prep {
			self.save_points.save(prep);
		}
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition
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
		// Get the existing value (if any)
		let current_val = if let Some(SavePrepare::NewKey(_, sv)) = &prep {
			sv.get_val().cloned()
		} else {
			self.inner.get(key.clone()).await?
		};
		// Delete the key
		match (current_val, chk) {
			(Some(v), Some(w)) if v == w => self.inner.put(key, val).await?,
			(None, None) => self.inner.put(key, val).await?,
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
		// Delete the key
		self.inner.delete(key).await?;
		// Confirm the save point
		if let Some(prep) = prep {
			self.save_points.save(prep);
		}
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition
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
		// Get the existing value (if any)
		let current_val = if let Some(SavePrepare::NewKey(_, sv)) = &prep {
			sv.get_val().cloned()
		} else {
			self.inner.get(key.clone()).await?
		};
		// Delete the key
		match (current_val, chk) {
			(Some(v), Some(w)) if v == w => self.inner.delete(key).await?,
			(None, None) => self.inner.delete(key).await?,
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
		self.db.unsafe_destroy_range(rng.start..rng.end).await?;
		// Return result
		Ok(())
	}

	/// Retrieve a range of keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		let rng = self.prepare_scan(rng, version)?;
		// Scan the keys
		let res = self.inner.scan_keys(rng, limit).await?.map(Key::from).collect();
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keysr(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		let rng = self.prepare_scan(rng, version)?;
		// Scan the keys
		let res = self.inner.scan_keys_reverse(rng, limit).await?.map(Key::from).collect();
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		let rng = self.prepare_scan(rng, version)?;
		// Scan the keys
		let res = self.inner.scan(rng, limit).await?.map(|kv| (Key::from(kv.0), kv.1)).collect();
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the database in reverse order
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scanr(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		let rng = self.prepare_scan(rng, version)?;
		// Scan the keys
		let res =
			self.inner.scan_reverse(rng, limit).await?.map(|kv| (Key::from(kv.0), kv.1)).collect();
		// Return result
		Ok(res)
	}

	/// Obtain a new change timestamp for a key
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn get_timestamp(&mut self, key: VsKey) -> Result<VersionStamp> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Get the transaction version
		let ver = self.inner.current_timestamp().await?.version();
		let key_encoded = key.encode_key()?;
		// Calculate the previous version value
		if let Some(prev) = self.get(key_encoded.clone(), None).await? {
			let prev = VersionStamp::from_slice(prev.as_slice())?.try_into_u64()?;
			ensure!(prev < ver, Error::TxFailure);
		};
		// Convert the timestamp to a versionstamp
		let ver = VersionStamp::from_u64(ver);
		// Store the timestamp to prevent other transactions from committing
		self.set(key_encoded, ver.to_vec(), None).await?;
		// Return the uint64 representation of the timestamp as the result
		Ok(ver)
	}

	fn get_save_points(&mut self) -> &mut SavePoints {
		&mut self.save_points
	}
}

impl Transaction {
	fn prepare_scan(&self, rng: Range<Key>, version: Option<u64>) -> Result<Range<Key>> {
		// TiKV does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		Ok(rng)
	}
}
