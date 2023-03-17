#![cfg(feature = "kv-postgres")]

use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use futures::lock::Mutex;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Acquire, Executor, Pool};
use std::ops::Range;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone)]
pub struct Datastore {
	pool: Pin<Arc<Pool<sqlx::Postgres>>>,
}

pub struct Transaction {
	// Is the transaction complete?
	ok: bool,
	// Is the transaction read+write?
	rw: bool,
	tx: Arc<Mutex<Option<sqlx::Transaction<'static, sqlx::Postgres>>>>,
}

impl Datastore {
	/// Open a new database
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		let option = PgConnectOptions::from_str(format!("postgres://{}", &path).as_str())?;

		let res = PgPoolOptions::new()
			.max_connections(100)
			.connect_with(option)
			.await;
		match res {
			Ok(pool) => Ok(Datastore {
				pool: Arc::pin(pool),
			}),
			Err(e) => Err(Error::Ds(e.to_string())),
		}
	}
	/// Start a new transaction
	pub async fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		// Create a new distributed transaction
		match self.pool.begin().await {
			Ok(mut tx) => {
				tx.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").await?;

				Ok(Transaction {
					ok: false,
					rw: write,
					tx: Arc::new(Mutex::new(Some(tx))),
				})
			},
			Err(e) => Err(Error::Tx(e.to_string())),
		}
	}
}

impl Transaction {
	/// Check if closed
	pub fn closed(&self) -> bool {
		self.ok
	}
	/// Cancel a transaction
	pub async fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Mark this transaction as done
		self.ok = true;

		match self.tx.lock().await.take() {
			Some(tx) => tx.rollback().await?,
			None => unreachable!(),
		};

		// Continue
		Ok(())
	}
	/// Commit a transaction
	pub async fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Mark this transaction as done
		self.ok = true;

		match self.tx.lock().await.take() {
			Some(tx) => tx.commit().await?,
			None => unreachable!(),
		};

		// Continue
		Ok(())
	}
	/// Check if a key exists
	pub async fn exi<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}

		let key = key.into();
		trace!("exi: {:?}", key);
		let mut tx = self.tx.lock().await;
		let tx = tx.as_mut().unwrap().acquire().await?;

		// Return result

		let query = sqlx::query("SELECT 1 FROM db WHERE key = $1").bind(key);

		Ok(query.fetch_optional(tx).await?.is_some())
	}
	/// Fetch a key from the database
	pub async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		let mut tx = self.tx.lock().await;
		let tx = tx.as_mut().unwrap().acquire().await?;

		// Get the key
		let key = key.into();
		trace!("get: {:?}", key);
		Ok(sqlx::query_scalar("SELECT value FROM db where key = $1")
			.bind(key)
			.fetch_optional(tx)
			.await?)
	}
	/// Insert or update a key in the database
	pub async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}

		let mut tx = self.tx.lock().await;
		let tx = tx.as_mut().unwrap().acquire().await?;

		// Set the key
		let key = key.into();
		let val = val.into();
		trace!("set: {:?} {:?}", key, val);

		sqlx::query("INSERT INTO db VALUES($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2")
			.bind(key)
			.bind(val)
			.execute(tx)
			.await?;
		Ok(())
	}
	/// Insert a key if it doesn't exist in the database
	pub async fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		let mut tx = self.tx.lock().await;
		let tx = tx.as_mut().unwrap().acquire().await?;

		// Set the key
		let key = key.into();
		let val = val.into();
		trace!("put: {:?} {:?}", key, val);

		sqlx::query("INSERT INTO db VALUES($1, $2)").bind(key).bind(val).execute(tx).await?;
		// Return result
		Ok(())
	}
	/// Insert a key if it doesn't exist in the database
	pub async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		let mut tx = self.tx.lock().await;
		let tx = tx.as_mut().unwrap().acquire().await?;
		// todo!();
		// Get the key
		let key = key.into();
		// Get the val
		let val = val.into();
		// Get the check
		let chk = chk.map(Into::into);

		trace!("putc: {:?} {:?} {:?}", key, val, chk);

		let ok = {
			let tx = tx.acquire().await?;
			match chk {
				Some(chk) => sqlx::query("SELECT 1 FROM db WHERE key = $1 AND value = $2")
					.bind(key.to_vec())
					.bind(chk)
					.fetch_optional(tx)
					.await?
					.is_some(),
				None => sqlx::query("SELECT 1 FROM db WHERE key = $1")
					.bind(key.to_vec())
					.fetch_optional(tx)
					.await?
					.is_none(),
			}
		};

		if ok {
			sqlx::query("INSERT INTO db VALUES($1, $2) ON CONFLICT DO UPDATE SET value = $2")
				.bind(key)
				.bind(val)
				.execute(tx)
				.await?;
			Ok(())
		} else {
			Err(Error::TxConditionNotMet)
		}
	}
	/// Delete a key
	pub async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}

		let mut tx = self.tx.lock().await;
		let tx = tx.as_mut().unwrap().acquire().await?;

		// Delete the key
		let key = key.into();
		trace!("del: {:?}", key);

		sqlx::query("DELETE FROM db WHERE key = $1").bind(key).execute(tx).await?;
		// Return result
		Ok(())
	}
	/// Delete a key
	pub async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}

		let mut tx = self.tx.lock().await;
		let tx = tx.as_mut().unwrap().acquire().await?;

		// Get the key
		let key = key.into();
		// Get the check
		let chk = chk.map(Into::into);

		trace!("delc: {:?} {:?}", key, chk);

		let ok = {
			let tx = tx.acquire().await?;
			match chk {
				Some(chk) => sqlx::query("SELECT 1 FROM db WHERE key = $1 AND value = $2")
					.bind(key.to_vec())
					.bind(chk)
					.fetch_optional(tx)
					.await?
					.is_some(),
				None => sqlx::query("SELECT 1 FROM db WHERE key = $1")
					.bind(key.to_vec())
					.fetch_optional(tx)
					.await?
					.is_none(),
			}
		};

		if ok {
			sqlx::query("DELETE FROM db WHERE key = $1").bind(key).execute(tx).await?;
			Ok(())
		} else {
			Err(Error::TxConditionNotMet)
		}
	}
	/// Retrieve a range of keys from the databases
	pub async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Convert the range to bytes

		let mut tx = self.tx.lock().await;
		let tx = tx.as_mut().unwrap().acquire().await?;

		let start = rng.start.into();
		let end = rng.end.into();

		trace!("scan: {:?} {:?} {:?}", start, end, limit);

		Ok(sqlx::query_as(
			r#"SELECT key, value FROM db WHERE key = $1 OR (key >= $1 AND key < $2) ORDER BY key LIMIT $3"#,
		)
		.bind(start)
		.bind(end)
		.bind(limit as i64)
		.fetch_all(tx)
		.await?)
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::tests::transaction::verify_transaction_isolation;
	use test_log::test;

	#[test(tokio::test(flavor = "multi_thread", worker_threads = 3))]
	async fn postgres_transaction() {
		verify_transaction_isolation("postgres://localhost:5432/postgres?user=postgres&password=surrealdb").await;
	}
}
