//! Defines a key-value interface for SurrealCS.
#![cfg(feature = "kv-surrealcs")]

use crate::err::Error;
use crate::kvs::Check;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::Versionstamp;
use futures::lock::Mutex;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use surrealcs_client::transactions::interface::interface::{
    Transaction as SurrealCSTransaction,
    Any as AnyState
};
use surrealcs_kernel::messages::server::message::{Transaction as TransactionMessage, KeyValueOperationType};
use surrealcs_client::transactions::interface::bridge::BridgeHandle;


/// simplifies code for returning an error
macro_rules! return_error {
    ($message: expr) => {
        return Err(
            Error::Ds(
                $message.to_string()
            )
        )
    };
}

/// simplifies code for mapping an error
macro_rules! safe_eject {
    ($op: expr) => {
        $op.map_err(|e| {
            Error::Ds(
                e.to_string()
            )
        })
    }
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
    inner: Arc<Mutex<SurrealCSTransaction<AnyState>>>,
    started: bool
}

// TODO => implement the drop through this might be better to implement the Drop in SurrealCS

impl Datastore {

    pub(crate) async fn new() -> Result<Datastore, Error> {
        let transaction = SurrealCSTransaction::new().await.unwrap();
        let transaction = transaction.into_any();
        Ok(Datastore{
            db: Pin::new(Arc::new(
                transaction
            ))
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
            inner: Arc::new(Mutex::new(transaction)),
            started: false
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
    async fn send_message(&mut self, message: TransactionMessage) -> Result<TransactionMessage, Error> {
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
            },
            // already started to hitting up an existing actor
            true => {
                let message = safe_eject!(transaction.send::<BridgeHandle>(message).await)?;
                Ok(message)
            }
        }
    }

}


impl super::api::Transaction for Transaction {

    fn check_level(&mut self, check: Check) {
        self.check = check;
    }

    fn closed(&self) -> bool {
		self.done
	}

    /// Check if writeable
	fn writeable(&self) -> bool {
		true
	}

    /// Rolls back the transaction.
    /// 
    /// # Notes
    /// 
    async fn cancel(&mut self) -> Result<(), Error> {
        let mut transaction = self.inner.lock().await;
        let _ = safe_eject!(transaction.rollback::<BridgeHandle>().await)?;
        Ok(())
    }

    /// Commits a transaction
    async fn commit(&mut self) -> Result<(), Error> {
        let mut transaction = self.inner.lock().await;
        if !self.write {
            return Err(Error::TxReadonly);
        }
        let _ = safe_eject!(transaction.empty_commit::<BridgeHandle>().await)?;
        Ok(())
    }

    /// Checks to see if the key exists.
    ///
    /// # Arguments
    /// * `key`: the key to be checked
    ///
    /// # Returns
    /// true if the key exists, false if not
    async fn exists<K>(&mut self, key: K) -> Result<bool, Error>
    where
        K: Into<Key>,
    {
        let message = TransactionMessage::new(
                key.into(), 
                KeyValueOperationType::Get
        );
        let response = self.send_message(message).await?;
        Ok(response.value.is_some())
    }

    async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
    where
        K: Into<Key>,
    {
        let message = TransactionMessage::new(
            key.into(), 
            KeyValueOperationType::Get
        );
        let response = self.send_message(message).await?;
        Ok(response.value)
    }

    /// Inserts or updates a key.
    ///
    /// # Arguments
    /// * `key`: the key that is to be set
    /// * `val`: the value that is going to be inserted under the key
    async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
    where
        K: Into<Key>,
        V: Into<Val>,
    {
        let message = TransactionMessage::new(
            key.into(), 
            KeyValueOperationType::Set
        ).with_value(val.into());
        let _ = self.send_message(message).await?;
        Ok(())
    }

    /// Inserts a key and value if it does not exist in the database.
    /// 
    /// # Arguments
    /// * `category`: the key structure for a particular layer or data structure that the value is
    /// * `key`: the key that is to be set
    /// * `val`: the value that is going to be inserted under the key
    async fn put<K, V>(
        &mut self,
        key: K,
        val: V,
    ) -> Result<(), Error>
    where
        K: Into<Key>,
        V: Into<Val>,
    {
        let message = TransactionMessage::new(
            key.into(), 
            KeyValueOperationType::Put
        ).with_value(val.into());
        let _ = self.send_message(message).await?;
        Ok(())
    }

    /// Inserts a key and value if the value corresponding to the key is expected.
    /// 
    /// # Arguments
    /// * `key`: the key that is going to be updated
    /// * `val`: the value that the key is going to be updated with
    /// * `chk`: the value that we are expecting for the key before we update
    async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
        let check_val = match chk {
            Some(value) => {
                value
            },
            None => {
                return Err(Error::Ds(
                    "expected value no supplied for putc operation".to_string()
                ))
            }
        };
        let check_val: Vec<u8> = check_val.into();
        let message = TransactionMessage::new(
            key.into(), 
            KeyValueOperationType::Putc
        )
        .with_value(val.into())
        .with_expected_value(check_val.into());
        let _ = self.send_message(message).await?;
        Ok(())
    }

    /// Deletes a key.
    /// 
    /// # Arguments
    /// * `key`: the key that is going to be deleted
    async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key>,
	{
        let message = TransactionMessage::new(
            key.into(), 
            KeyValueOperationType::Delete
        );
        let _ = self.send_message(message).await?;
		Ok(())
	}

    /// Deletes a key if the value corresponding to the key is expected.
    /// 
    /// # Arguments
    /// * `key`: the key that is going to be deleted
    /// * `chk`: the value that we are expecting for the key before we delete
    async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
        let check_val = match chk {
            Some(value) => {
                value
            },
            None => {
                return Err(Error::Ds(
                    "expected value no supplied for putc operation".to_string()
                ))
            }
        };
        let check_val: Vec<u8> = check_val.into();
        let message = TransactionMessage::new(
            key.into(), 
            KeyValueOperationType::Delc
        ).with_expected_value(check_val.into());
        let _ = self.send_message(message).await?;
        Ok(())
    }

    async fn keys<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<Key>, Error> {
        let message = TransactionMessage::new(
            "placeholder".to_string().into_bytes(), 
            KeyValueOperationType::Keys
        );
        let response = self.send_message(message).await?;
        match response.keys {
            Some(keys) => {
                let mut result = Vec::new();
                for key in keys {
                    result.push(key.into());
                }
                Ok(result)
            },
            None => {
                return_error!("no keys returned from keys operation")
            }
        }
    }

    /// Scans the database for keys within a range.
    /// 
    /// # Arguments
    /// * `rng`: the range of keys to be scanned
    /// * `limit`: the maximum number of keys to be returned
    /// 
    /// # Returns
    /// a vector of keys and values
    async fn scan<K>(
		&mut self,
		rng: Range<K>,
		limit: u32,
	) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
        let message = TransactionMessage {
            key: "placeholder".to_string().into_bytes(),
            value: None,
            expected_value: None,
            begin: Some(rng.start.into()),
            finish: Some(rng.end.into()),
            values: None,
            kv_operation_type: KeyValueOperationType::Scan,
            limit: Some(limit),
            keys: None
        };
        let response = self.send_message(message).await?;
        match response.values {
            Some(values) => {
                let mut result = Vec::new();
                for (key, val) in values {
                    result.push((key.into(), val.into()));
                }
                Ok(result)
            },
            None => {
                return_error!("no values returned from scan operation")
            }
        }
    }

    /// Gets the timestamp of a key.
    /// 
    /// # Arguments
    /// * `key`: the key to get the timestamp for
    /// 
    /// # Returns
    /// the timestamp of the key
    async fn get_timestamp<K>(&mut self, key: K) -> Result<Versionstamp, Error>
	where
		K: Into<Key>,
	{
        let message = TransactionMessage::new(
            key.into(), 
            KeyValueOperationType::GetTimeStamp
        );
        let response = self.send_message(message).await?;
        match response.value {
            Some(timestamp) => {
                let timestamp: [u8; 10] = match timestamp.try_into() {
                    Ok(value) => {
                        value
                    },
                    Err(_) => {
                        return_error!("timestamp returned from get timestamp operation is not 10 bytes")
                    }
                };
                Ok(timestamp)
            },
            None => {
                return_error!("no timestamp returned from get timestamp operation")
            }
        }
	}

    // async fn get_versionstamped_key<K>(
	// 	&mut self,
	// 	ts_key: K,
	// 	prefix: K,
	// 	suffix: K,
	// ) -> Result<Vec<u8>, Error>
	// where
	// 	K: Into<Key>,
	// {
    //     let response = self.get_timestamp(ts_key).await?;
    //     Ok(response.into())
    // }

}