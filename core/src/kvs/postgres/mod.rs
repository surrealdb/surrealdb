#![cfg(feature = "kv-postgres")]

use std::ops::Range;

use crate::err::Error;
use crate::key::error::KeyCategory;
use crate::kvs::Check;
use crate::kvs::Key;
use crate::kvs::Transactable;
use crate::kvs::Val;
use crate::vs::try_to_u64_be;
use crate::vs::u64_to_versionstamp;
use crate::vs::Versionstamp;
use sqlx::Executor;
use sqlx::PgPool;
#[derive(Clone)]
pub struct Datastore {
	pool: PgPool,
}

pub struct Transaction {
	/// Is the transaction writeable?
	write: bool,
	/// Should we check unhandled transactions?
	check: Check,
	/// The underlying datastore transaction
	inner: Option<sqlx::Transaction<'static, sqlx::Postgres>>,
}

impl Datastore {
	/// Open a new database
	pub(crate) async fn new(path: &str) -> Result<Datastore, Error> {
		let pool = PgPool::connect(path).await?;
		sqlx::query(
			r#"
			CREATE TABLE IF NOT EXISTS kvstore (
				key bytea PRIMARY KEY,
				value bytea
			);
			CREATE UNIQUE INDEX IF NOT EXISTS kvstore_sorted_pk ON kvstore(key ASC);
			"#,
		)
		.execute(&pool)
		.await?;
		Ok(Self {
			pool,
		})
	}

	/// Start a new transaction
	pub(crate) async fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		// Specify the check level
		#[cfg(not(debug_assertions))]
		let check = Check::Warn;
		#[cfg(debug_assertions)]
		let check = Check::Panic;
		// Create a new transaction
		match self.pool.begin().await {
			Ok(mut tx) => {
				tx.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;").await?;

				Ok(Transaction {
					check,
					write,
					inner: Some(tx),
				})
			}
			Err(e) => Err(Error::Tx(e.to_string())),
		}
	}
}

impl Transaction {
	/// Behaviour if unclosed
	pub(crate) fn check_level(&mut self, check: Check) {
		self.check = check;
	}
}

impl Transactable for Transaction {
	fn closed(&self) -> bool {
		self.inner.is_none()
	}

	async fn cancel(&mut self) -> Result<(), Error> {
		// If the transaction is already closed, return an error.
		if self.closed() {
			return Err(Error::TxFinished);
		}

		// Rollback the transaction.
		if let Some(tx) = self.inner.take() {
			tx.rollback().await?;
		}

		Ok(())
	}

	async fn commit(&mut self) -> Result<(), Error> {
		// If the transaction is already closed or is read-only, return an error.
		if !self.write {
			return Err(Error::TxReadonly);
		}

		// Commit the transaction.
		if let Some(tx) = self.inner.take() {
			tx.commit().await.map_err(Into::into)
		} else {
			Err(Error::TxFinished)
		}
	}

	async fn exi<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<crate::kvs::Key>,
	{
		if let Some(ref mut tx) = self.inner {
			Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM kvstore WHERE key = $1)")
				.bind(key.into())
				.fetch_one(&mut **tx)
				.await?)
		} else {
			Err(Error::TxFinished)
		}
	}

	async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<crate::kvs::Key>,
	{
		if let Some(ref mut tx) = self.inner {
			Ok(sqlx::query_scalar("SELECT value FROM kvstore WHERE key = $1")
				.bind(key.into())
				.fetch_optional(&mut **tx)
				.await?)
		} else {
			Err(Error::TxFinished)
		}
	}

	async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<crate::kvs::Key>,
		V: Into<crate::kvs::Val>,
	{
		if let Some(ref mut tx) = self.inner {
			sqlx::query(
				"INSERT INTO kvstore(key, value) VALUES($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
			)
			.bind(key.into())
			.bind(val.into())
			.execute(&mut **tx)
			.await?;
			Ok(())
		} else {
			Err(Error::TxFinished)
		}
	}

	async fn put<K, V>(
		&mut self,
		_: crate::key::error::KeyCategory,
		key: K,
		val: V,
	) -> Result<(), Error>
	where
		K: Into<crate::kvs::Key>,
		V: Into<crate::kvs::Val>,
	{
		if let Some(ref mut tx) = self.inner {
			sqlx::query(
				"INSERT INTO kvstore(key, value) VALUES($1, $2) ON CONFLICT (key) DO NOTHING",
			)
			.bind(key.into())
			.bind(val.into())
			.execute(&mut **tx)
			.await?;
			Ok(())
		} else {
			Err(Error::TxFinished)
		}
	}

	async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<crate::kvs::Key>,
		V: Into<crate::kvs::Val>,
	{
		if let Some(ref mut tx) = self.inner {
			match chk {
				Some(chk) => {
					let execution = sqlx::query("INSERT INTO kvstore(key, value) VALUES($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value WHERE value = $3")
						.bind(key.into())
						.bind(val.into())
						.bind(chk.into())
						.execute(&mut **tx)
						.await?;
					if execution.rows_affected() == 0 {
						Err(Error::TxConditionNotMet)
					} else {
						Ok(())
					}
				}
				None if self.exi(key).await? => return Err(Error::TxConditionNotMet),
				_ => Ok(()),
			}
		} else {
			Err(Error::TxFinished)
		}
	}

	async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<crate::kvs::Key>,
	{
		if let Some(ref mut tx) = self.inner {
			sqlx::query("DELETE FROM kvstore WHERE key = $1")
				.bind(key.into())
				.execute(&mut **tx)
				.await?;
			Ok(())
		} else {
			Err(Error::TxFinished)
		}
	}

	async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<crate::kvs::Key>,
		V: Into<crate::kvs::Val>,
	{
		if let Some(ref mut tx) = self.inner {
			match chk {
				Some(chk) => {
					let execution =
						sqlx::query("DELETE FROM kvstore WHERE key = $1 AND value = $2")
							.bind(key.into())
							.bind(chk.into())
							.execute(&mut **tx)
							.await?;
					if execution.rows_affected() == 0 {
						Err(Error::TxConditionNotMet)
					} else {
						Ok(())
					}
				}
				None if self.exi(key).await? => return Err(Error::TxConditionNotMet),
				_ => Ok(()),
			}
		} else {
			Err(Error::TxFinished)
		}
	}

	async fn delr<K>(&mut self, rng: Range<K>, limit: u32) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		if let Some(ref mut tx) = self.inner {
			sqlx::query("DELETE FROM kvstore WHERE ctid IN (SELECT ctid FROM kvstore WHERE key = $1 OR (key >= $1 AND key < $2) ORDER BY key ASC LIMIT $3)")
			.bind(rng.start.into())
			.bind(rng.end.into())
			// HACK: because sqlx, for some reason, do not have numeric encoding for unsigned values but do have implementations for signed values. 
			// So we are forced to cast to signed integer. Fortunately, we are converting from unsigned to signed,
			// we just need to make sure the casted type is big enough to not have integer overflow
			.bind(limit as i64)
			.execute(&mut **tx)
			.await?;
			Ok(())
		} else {
			Err(Error::TxFinished)
		}
	}

	async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<crate::kvs::Key>,
	{
		if let Some(ref mut tx) = self.inner {
			Ok(sqlx::query_scalar("SELECT key, value FROM kvstore WHERE key = $1 OR (key >= $1 AND key < $2) ORDER BY key ASC LIMIT $3")
			.bind(rng.start.into())
			.bind(rng.end.into())
			// HACK: because sqlx, for some reason, do not have numeric encoding for unsigned values but do have implementations for signed values. 
			// So we are forced to cast to signed integer. Fortunately, we are converting from unsigned to signed,
			// we just need to make sure the casted type is big enough to not have integer overflow
			.bind(limit as i64)
			.fetch_all(&mut **tx)
			.await?)
		} else {
			Err(Error::TxFinished)
		}
	}

	async fn get_timestamp<K>(&mut self, key: K, _lock: bool) -> Result<Versionstamp, Error>
	where
		K: Into<crate::kvs::Key>,
	{
		// Write the timestamp to the "last-write-timestamp" key
		// to ensure that no other transactions can commit with older timestamps.
		let k: Key = key.into();
		let prev = self.get(k.clone()).await?;
		let ver = match prev {
			Some(prev) => {
				let slice = prev.as_slice();
				let res: Result<[u8; 10], Error> = match slice.try_into() {
					Ok(ba) => Ok(ba),
					Err(e) => Err(Error::Ds(e.to_string())),
				};
				let array = res?;
				let prev: u64 = try_to_u64_be(array)?;
				prev + 1
			}
			None => 1,
		};

		let verbytes = u64_to_versionstamp(ver);

		self.put(KeyCategory::Unknown, k, verbytes.to_vec()).await?;
		// Return the uint64 representation of the timestamp as the result
		Ok(verbytes)
	}
}
