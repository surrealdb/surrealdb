#![cfg(feature = "kv-surrealcs")]

use crate::err::Error;
use crate::key::debug::Sprintable;
use crate::kvs::Check;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::Versionstamp;
use futures::lock::Mutex;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use surrealcs_client::router::create_connection;
use surrealcs_client::transactions::interface::bridge::BridgeHandle;
use surrealcs_client::transactions::interface::interface::{
	Any as AnyState, Transaction as SurrealCSTransaction,
};
use surrealcs_kernel::messages::server::message::{
	KeyValueOperationType::{self, *},
	Transaction as TransactionMessage,
};

/// simplifies code for mapping an error
macro_rules! safe_eject {
	($op: expr) => {
		$op.map_err(|e| Error::Ds(e.to_string()))
	};
}

/// The main struct that is used to interact with the database.
///
/// # Fields
/// * `db`: the database handle
#[derive(Clone)]
#[non_exhaustive]
pub struct Datastore {
	db: Pin<Arc<SurrealCSTransaction<AnyState>>>,
}

#[non_exhaustive]
pub struct Transaction {
	// is the transaction complete?
	done: bool,
	// is the transaction writable
	write: bool,
	// Should we check unhandled transactions
	check: Check,
	// Whether the transaction has been started
	started: bool,
	/// The underlying datastore transaction
	inner: Arc<Mutex<SurrealCSTransaction<AnyState>>>,
}

impl Drop for Transaction {
	fn drop(&mut self) {
		if !self.done && self.write {
			// Check if already panicking
			if std::thread::panicking() {
				return;
			}
			// Handle the behaviour
			match self.check {
				Check::None => {
					trace!("A transaction was dropped without being committed or cancelled");
				}
				Check::Warn => {
					warn!("A transaction was dropped without being committed or cancelled");
				}
				Check::Panic => {
					#[cfg(debug_assertions)]
					{
						let backtrace = std::backtrace::Backtrace::force_capture();
						if let std::backtrace::BacktraceStatus::Captured = backtrace.status() {
							println!("{}", backtrace);
						}
					}
					panic!("A transaction was dropped without being committed or cancelled");
				}
			}
		}
	}
}

impl Datastore {
	/// Open a new database
	pub(crate) async fn new(path: &str) -> Result<Datastore, Error> {
		let num = num_cpus::get();
		for _ in 0..num {
			let _ = create_connection(path).await.unwrap();
		}
		let transaction = SurrealCSTransaction::new().await.unwrap();
		let transaction = transaction.into_any();
		Ok(Datastore {
			db: Pin::new(Arc::new(transaction)),
		})
	}

	/// Starts a new transaction.
	///
	/// # Arguments
	/// * `write`: is the transaction writable
	///
	/// # Returns
	/// the transaction
	pub(crate) async fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		let transaction = safe_eject!(SurrealCSTransaction::new().await)?;
		let transaction = transaction.into_any();
		Ok(Transaction {
			done: false,
			check: Check::Warn,
			write,
			started: false,
			inner: Arc::new(Mutex::new(transaction)),
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
		message: TransactionMessage,
	) -> Result<TransactionMessage, Error> {
		let mut transaction = self.inner.lock().await;

		// This is the lazy evaluation, the transaction isn't registered with the SurrealCS server
		// unless we actually start a transaction
		match self.started {
			// Create a transaction actor for the first transaction
			false => {
				// let message = safe_eject!(&transaction.begin(message).await)?;
				let message = safe_eject!(transaction.begin::<BridgeHandle>(message).await)?;
				self.started = true;
				Ok(message)
			}
			// already started to hitting up an existing actor
			true => {
				let message = safe_eject!(transaction.send::<BridgeHandle>(message).await)?;
				Ok(message)
			}
		}
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
		let mut transaction = self.inner.lock().await;
		let _ = safe_eject!(transaction.rollback::<BridgeHandle>().await)?;
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
		let _ = safe_eject!(transaction.empty_commit::<BridgeHandle>().await)?;
		// Continue
		Ok(())
	}

	/// Checks if a key exists in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check the key
		let message = TransactionMessage::new(key.into(), Get);
		let response = self.send_message(message).await?;
		// Return result
		Ok(response.value.is_some())
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Fetch the value from the database.
		let message = TransactionMessage::new(key.into(), Get);
		let response = self.send_message(message).await?;
		// Return result
		Ok(response.value)
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
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
		// Set the key
		let message = TransactionMessage::new(key.into(), Set).with_value(val.into());
		let _ = self.send_message(message).await?;
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
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
		// Put the key
		let message = TransactionMessage::new(key.into(), Put).with_value(val.into());
		let _ = self.send_message(message).await?;
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
		// Set the key if valid
		let message = TransactionMessage::new(key.into(), Putc)
			.with_value(val.into())
			.with_expected_value(chk);
		let _ = self.send_message(message).await?;
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
		// Remove the key
		let message = TransactionMessage::new(key.into(), Delete);
		let _ = self.send_message(message).await?;
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
		// Delete the key if valid
		let message = TransactionMessage::new(key.into(), Delc).with_expected_value(chk);
		let _ = self.send_message(message).await?;
		// Return result
		Ok(())
	}

	/// Retrieves a range of key-value pairs from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<Key>, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Retrieve the scan range
		let message = TransactionMessage {
			key: "placeholder".to_string().into_bytes(),
			value: None,
			expected_value: None,
			begin: Some(rng.start.into()),
			finish: Some(rng.end.into()),
			values: None,
			kv_operation_type: KeyValueOperationType::Keys,
			limit: Some(limit),
			keys: None,
		};
		let response = self.send_message(message).await?;
		// Return result
		match response.keys {
			Some(keys) => {
				let mut result = Vec::new();
				for key in keys {
					result.push(key.into());
				}
				Ok(result)
			}
			None => Ok(vec![]),
		}
	}

	/// Retrieves a range of key-value pairs from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Retrieve the scan range
		let message = TransactionMessage {
			key: "placeholder".to_string().into_bytes(),
			value: None,
			expected_value: None,
			begin: Some(rng.start.into()),
			finish: Some(rng.end.into()),
			values: None,
			kv_operation_type: KeyValueOperationType::Scan,
			limit: Some(limit),
			keys: None,
		};
		let response = self.send_message(message).await?;
		// Return result
		match response.values {
			Some(values) => {
				let mut result = Vec::new();
				for (key, val) in values {
					result.push((key.into(), val.into()));
				}
				Ok(result)
			}
			None => Ok(vec![]),
		}
	}
}
