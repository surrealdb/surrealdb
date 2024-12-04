#![cfg(feature = "kv-surrealcs")]

mod cnf;

use crate::err::Error;
use crate::key::debug::Sprintable;
use crate::kvs::{
	savepoint::{SaveOperation, SavePointImpl, SavePoints},
	Check, Key, Val, Version,
};
use futures::lock::Mutex;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use surrealcs::kernel::messages::server::interface::ServerTransactionMessage;
use surrealcs::kernel::messages::server::kv_operations::*;
use surrealcs::kernel::utils::generic::check_condition_not_met;
use surrealcs::kernel::utils::generic::check_key_already_exists;
use surrealcs::router::create_connection_pool;
use surrealcs::transactions::interface::bridge::BridgeHandle;
use surrealcs::transactions::interface::interface::{
	Any as AnyState, Transaction as SurrealCSTransaction,
};

pub struct Datastore {}

pub struct Transaction {
	/// Is the transaction complete?
	done: bool,
	/// Is the transaction writeable?
	write: bool,
	/// Should we check unhandled transactions?
	check: Check,
	/// Has the transaction been started?
	started: bool,
	/// The underlying datastore transaction
	inner: Arc<Mutex<SurrealCSTransaction<AnyState>>>,
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
	pub(crate) async fn new(path: &str) -> Result<Datastore, Error> {
		match create_connection_pool(path, Some(*cnf::SURREALCS_CONNECTION_POOL_SIZE)).await {
			Ok(_) => Ok(Datastore {}),
			Err(_) => {
				Err(Error::Ds("Cannot connect to the `surrealcs` storage engine".to_string()))
			}
		}
	}
	/// Shutdown the database
	pub(crate) async fn shutdown(&self) -> Result<(), Error> {
		// Nothing to do here
		Ok(())
	}
	/// Starts a new transaction.
	///
	/// # Arguments
	/// * `write`: is the transaction writable
	///
	/// # Returns
	/// the transaction
	pub(crate) async fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		// Create the underlying transaction
		let transaction = SurrealCSTransaction::new().await;
		let transaction = transaction.map_err(|e| Error::Tx(e.to_string()))?;
		let transaction = transaction.into_any();
		// Specify the check level
		#[cfg(not(debug_assertions))]
		let check = Check::Warn;
		#[cfg(debug_assertions)]
		let check = Check::Error;
		// Create a new transaction
		Ok(Transaction {
			done: false,
			check,
			write,
			started: false,
			inner: Arc::new(Mutex::new(transaction)),
			save_points: Default::default(),
		})
	}
}

impl Transaction {
	/// Sends a message to the SurrealCS server.
	///
	/// # Arguments
	/// * `message`: the message to be sent to the server
	///
	/// # Returns
	/// the response from the server
	async fn send_message(
		&mut self,
		message: ServerTransactionMessage,
	) -> Result<ServerTransactionMessage, Error> {
		let mut transaction = self.inner.lock().await;
		// Check to see if this transaction is started
		let started = self.started;
		// For any future calls, this transaction is started
		self.started = true;
		// If this is the first message to SurrealCS then
		// we need to start a transaction, by creating the
		// actor, and send the message with the request.
		let response = match started {
			false => transaction.begin::<BridgeHandle>(message).await,
			true => transaction.send::<BridgeHandle>(message).await,
		};
		// Return the result
		response.map_err(|e| match e {
			e if check_key_already_exists(&e) => Error::TxKeyAlreadyExists,
			e if check_condition_not_met(&e) => Error::TxConditionNotMet,
			e => Error::Tx(e.to_string()),
		})
	}
}

impl super::api::Transaction for Transaction {
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

	/// Cancels the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Mark this transaction as done
		self.done = true;
		// Cancel this transaction
		let mut transaction = self.inner.lock().await;
		transaction.rollback::<BridgeHandle>().await.map_err(|e| Error::Tx(e.to_string()))?;
		// Continue
		Ok(())
	}

	/// Commits the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Mark the transaction as done.
		self.done = true;
		// Commit the transaction
		let mut transaction = self.inner.lock().await;
		transaction.commit::<BridgeHandle>().await.map_err(|e| Error::Tx(e.to_string()))?;
		// Continue
		Ok(())
	}

	/// Checks if a key exists in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists<K>(&mut self, key: K, version: Option<u64>) -> Result<bool, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check the key
		let message = ServerTransactionMessage::Exists(MessageExists {
			key: key.into(),
			version,
		});
		let response = match self.send_message(message).await? {
			ServerTransactionMessage::ResponseExists(v) => v,
			_ => return Err(Error::Tx("Received an invalid response".to_string())),
		};
		// Return result
		Ok(response)
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get<K>(&mut self, key: K, version: Option<u64>) -> Result<Option<Val>, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Fetch the value from the database.
		let message = ServerTransactionMessage::Get(MessageGet {
			key: key.into(),
			version,
		});
		let response = match self.send_message(message).await? {
			ServerTransactionMessage::ResponseGet(v) => v,
			_ => return Err(Error::Tx("Received an invalid response".to_string())),
		};
		// Return result
		Ok(response.value)
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set<K, V>(&mut self, key: K, val: V, version: Option<u64>) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
		V: Into<Val> + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Extract the key
		let key = key.into();
		// Prepare the savepoint if any
		let prep = if self.save_points.is_some() {
			self.save_point_prepare(&key, version, SaveOperation::Set).await?
		} else {
			None
		};
		// Set the key
		let message = ServerTransactionMessage::Set(MessageSet {
			key,
			value: val.into(),
			version,
		});
		self.send_message(message).await?;
		// Confirm the save point
		if let Some(prep) = prep {
			self.save_points.save(prep);
		}
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put<K, V>(&mut self, key: K, val: V, version: Option<u64>) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
		V: Into<Val> + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Extract the key
		let key = key.into();
		// Hydrate the savepoint if any
		let prep = if self.save_points.is_some() {
			self.save_point_prepare(&key, version, SaveOperation::Put).await?
		} else {
			None
		};
		// Put the key
		let message = ServerTransactionMessage::Put(MessagePut {
			key,
			value: val.into(),
			version,
		});
		self.send_message(message).await?;
		// Confirm the save point
		if let Some(prep) = prep {
			self.save_points.save(prep);
		}
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
		V: Into<Val> + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the arguments
		let chk = chk.map(Into::into);
		// Extract the key
		let key = key.into();
		// Hydrate the savepoint if any
		let prep = if self.save_points.is_some() {
			self.save_point_prepare(&key, None, SaveOperation::Put).await?
		} else {
			None
		};
		// Set the key if valid
		let message = ServerTransactionMessage::Putc(MessagePutc {
			key,
			value: val.into(),
			expected_value: chk,
		});
		self.send_message(message).await?;
		// Confirm the save point
		if let Some(prep) = prep {
			self.save_points.save(prep);
		}
		// Return result
		Ok(())
	}

	/// Deletes a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Extract the key
		let key = key.into();
		// Hydrate the savepoint if any
		let prep = if self.save_points.is_some() {
			self.save_point_prepare(&key, None, SaveOperation::Del).await?
		} else {
			None
		};
		// Remove the key
		let message = ServerTransactionMessage::Del(MessageDel {
			key,
		});
		self.send_message(message).await?;
		// Confirm the save point
		if let Some(prep) = prep {
			self.save_points.save(prep);
		}
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
		V: Into<Val> + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the arguments
		let chk = chk.map(Into::into);
		// Extract the key
		let key = key.into();
		// Hydrate the savepoint if any
		let prep = if self.save_points.is_some() {
			self.save_point_prepare(&key, None, SaveOperation::Del).await?
		} else {
			None
		};
		// Delete the key if valid
		let message = ServerTransactionMessage::Delc(MessageDelc {
			key,
			expected_value: chk,
		});
		self.send_message(message).await?;
		// Confirm the save point
		if let Some(prep) = prep {
			self.save_points.save(prep);
		}
		// Return result
		Ok(())
	}

	/// Delete a range of keys from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn delr<K>(&mut self, rng: Range<K>) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Delete the scan range
		let message = ServerTransactionMessage::Delr(MessageDelr {
			begin: rng.start.into(),
			finish: rng.end.into(),
		});
		self.send_message(message).await?;
		// Return result
		Ok(())
	}

	/// Retrieves a range of key-value pairs from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys<K>(
		&mut self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Retrieve the scan range
		let message = ServerTransactionMessage::Keys(MessageKeys {
			begin: rng.start.into(),
			finish: rng.end.into(),
			limit,
			version,
		});
		// TODO: Check if save point needs to be implemented here
		let response = match self.send_message(message).await? {
			ServerTransactionMessage::ResponseKeys(v) => v,
			_ => return Err(Error::Tx("Received an invalid response".to_string())),
		};
		// Return result
		Ok(response.keys)
	}

	/// Retrieves a range of key-value pairs from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan<K>(
		&mut self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Retrieve the scan range
		let message = ServerTransactionMessage::Scan(MessageScan {
			begin: rng.start.into(),
			finish: rng.end.into(),
			limit,
			version,
		});
		let response = match self.send_message(message).await? {
			ServerTransactionMessage::ResponseScan(v) => v,
			_ => return Err(Error::Tx("Received an invalid response".to_string())),
		};
		// Return result
		Ok(response.values)
	}

	/// Retrieve all the versions from a range of keys from the databases
	/// This is a no-op for surrealcs.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan_all_versions<K>(
		&mut self,
		rng: Range<K>,
		limit: u32,
	) -> Result<Vec<(Key, Val, Version, bool)>, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		Err(Error::UnsupportedVersionedQueries)
	}
}

impl SavePointImpl for Transaction {
	fn get_save_points(&mut self) -> &mut SavePoints {
		&mut self.save_points
	}
}
