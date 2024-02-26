#![cfg(feature = "kv-mysql")]

use std::ops::Range;

use crate::err::Error;
use crate::key::error::KeyCategory;
use crate::kvs::Check;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::try_to_u64_be;
use crate::vs::u64_to_versionstamp;
use crate::vs::Versionstamp;

use sqlx::mysql::MySqlPoolOptions;
use sqlx::mysql::MySqlRow;
use sqlx::Executor;
use sqlx::MySqlPool;
use sqlx::Row;
use std::ops::DerefMut;

#[derive(Clone)]
pub struct Datastore {
	pool: MySqlPool,
}

pub struct Transaction {
	/// Is the transaction writeable?
	write: bool,
	/// Should we check unhandled transactions?
	check: Check,
	/// The underlying datastore transaction
	inner: Option<sqlx::Transaction<'static, sqlx::MySql>>,
}

impl Datastore {
	/// Open a new database
	pub(crate) async fn new(path: &str) -> Result<Datastore, Error> {
		let pool = MySqlPoolOptions::new().connect(path).await?;
		sqlx::query(
			r#"
			CREATE TABLE IF NOT EXISTS kvstore (
				`key` BLOB NOT NULL,
				`value` LONGBLOB NOT NULL,
				PRIMARY KEY (`key`(767)),
				UNIQUE INDEX (`key`(767) ASC, `value`(767))
			) DEFAULT CHARSET=utf8mb4;
			"#,
		)
		.execute(&pool)
		.await?;
		// Enables repeatable read, also known as "Snapshot Isolation" for the ivory tower folks
		sqlx::query("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
			.execute(&pool)
			.await?;
		sqlx::query("SET autocommit=0;").execute(&pool).await?;
		Ok(Self {
			pool,
		})
	}

	/// Start a new transaction
	pub(crate) async fn transaction(&self, write: bool, _lock: bool) -> Result<Transaction, Error> {
		// TODO: Explicit lock level: https://www.mysql.org/docs/current/explicit-locking.html
		// However, how to implement locking (we have row lock and table lock) is still subjectable to debate

		// Create a new transaction
		match self.pool.begin().await {
			Ok(tx) => Ok(Transaction {
				check: if cfg!(debug_assertions) {
					Check::Panic
				} else {
					Check::Warn
				},
				write,
				inner: Some(tx),
			}),
			Err(e) => Err(Error::Tx(e.to_string())),
		}
	}
}

impl Transaction {
	/// Behaviour if unclosed
	pub(crate) fn check_level(&mut self, check: Check) {
		self.check = check;
	}

	/// Checks if the key either matches the "check variable", or don't match at all
	/// In other word, this is "exclusive or"
	async fn key_either_matches_check_or_dont<V>(
		&mut self,
		key: Key,
		chk: Option<V>,
	) -> Result<bool, Error>
	where
		V: Into<Val>,
	{
		let tx = self.inner.as_mut().ok_or(Error::TxFinished)?.deref_mut();
		let check = match chk {
			Some(chk) => sqlx::query_scalar(
				"SELECT EXISTS(SELECT 1 FROM kvstore WHERE `key` = ? AND `value` = ? LIMIT 1)",
			)
			.bind(key)
			.bind(chk.into()),
			None => sqlx::query_scalar(
				"SELECT NOT EXISTS(SELECT 1 FROM kvstore WHERE `key` = ? LIMIT 1)",
			)
			.bind(key),
		};
		Ok(check.fetch_one(tx).await?)
	}
}

impl crate::kvs::api::Transaction for Transaction {
	fn closed(&self) -> bool {
		self.inner.is_none()
	}

	async fn cancel(&mut self) -> Result<(), Error> {
		// If the transaction is already closed, return an error.
		if self.closed() {
			Err(Error::TxFinished)
		} else {
			self.inner.take().ok_or(Error::TxFinished)?.rollback().await.map_err(Into::into)
		}
	}

	async fn commit(&mut self) -> Result<(), Error> {
		// If the transaction is already closed or is read-only, return an error.
		if !self.write {
			Err(Error::TxReadonly)
		} else {
			self.inner.take().ok_or(Error::TxFinished)?.commit().await.map_err(Into::into)
		}
	}

	async fn exi<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key>,
	{
		let tx = self.inner.as_mut().ok_or(Error::TxFinished)?.deref_mut();
		Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM kvstore WHERE `key` = ? LIMIT 1)")
			.bind(key.into())
			.fetch_one(tx)
			.await?)
	}

	async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<Key>,
	{
		let tx = self.inner.as_mut().ok_or(Error::TxFinished)?.deref_mut();
		Ok(sqlx::query_scalar("SELECT `value` FROM kvstore WHERE `key` = ? LIMIT 1")
			.bind(key.into())
			.fetch_optional(tx)
			.await?)
	}

	async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// If the transaction is already closed or is read-only, return an error.
		if !self.write {
			Err(Error::TxReadonly)
		} else {
			let tx = self.inner.as_mut().ok_or(Error::TxFinished)?.deref_mut();
			sqlx::query("INSERT INTO kvstore(`key`, `value`) VALUES(?, ?) ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)")
				.bind(key.into())
				.bind(val.into())
				.execute(tx)
				.await?;
			Ok(())
		}
	}

	async fn put<K, V>(&mut self, category: KeyCategory, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// If the transaction is already closed or is read-only, return an error.
		if !self.write {
			Err(Error::TxReadonly)
		} else {
			let tx = self.inner.as_mut().ok_or(Error::TxFinished)?.deref_mut();
			if let Err(e) = sqlx::query("INSERT INTO kvstore(`key`, `value`) VALUES(?, ?)")
				.bind(key.into())
				.bind(val.into())
				.execute(tx)
				.await
			{
				if let Some(true) = e.as_database_error().map(|x| x.is_unique_violation()) {
					Err(Error::TxKeyAlreadyExistsCategory(category))
				} else {
					Err(e.into())
				}
			} else {
				Ok(())
			}
		}
	}

	async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		let key: Key = key.into();
		// If the transaction is already closed or is read-only, return an error.
		if !self.write {
			Err(Error::TxReadonly)
		} else if self.key_either_matches_check_or_dont(key.clone(), chk).await? {
			self.set(key, val).await
		} else {
			Err(Error::TxConditionNotMet)
		}
	}

	async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		// If the transaction is already closed or is read-only, return an error.
		if !self.write {
			Err(Error::TxReadonly)
		} else {
			let tx = self.inner.as_mut().ok_or(Error::TxFinished)?.deref_mut();
			sqlx::query("DELETE FROM kvstore WHERE `key` = ? LIMIT 1")
				.bind(key.into())
				.execute(tx)
				.await?;
			Ok(())
		}
	}

	async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		let key = key.into();
		// If the transaction is already closed or is read-only, return an error.
		if !self.write {
			Err(Error::TxReadonly)
		} else if self.key_either_matches_check_or_dont(key.clone(), chk).await? {
			self.del(key).await
		} else {
			Err(Error::TxConditionNotMet)
		}
	}

	async fn delr<K>(&mut self, rng: Range<K>, limit: u32) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		// If the transaction is already closed or is read-only, return an error.
		if !self.write {
			Err(Error::TxReadonly)
		} else {
			let tx = self.inner.as_mut().ok_or(Error::TxFinished)?.deref_mut();
			let start = rng.start.into();
			sqlx::query("DELETE FROM kvstore WHERE `key` IN (SELECT `key` FROM kvstore WHERE `key` = ? OR (`key` >= ? AND `key` < ?) ORDER BY `key` ASC LIMIT ?)")
				.bind(start.clone())
				.bind(start)
				.bind(rng.end.into())
				// HACK: because sqlx, for some reason, do not have numeric encoding for unsigned values but do have implementations for signed values.
				// So we are forced to cast to signed integer. Fortunately, we are converting from unsigned to signed,
				// we just need to make sure the casted type is big enough to not have integer overflow
				.bind(limit as i64)
				.execute(tx)
				.await?;
			Ok(())
		}
	}

	async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		let tx = self.inner.as_mut().ok_or(Error::TxFinished)?.deref_mut();

		let start = rng.start.into();
		let result_set = sqlx::query("SELECT `key`, `value` FROM kvstore WHERE `key` = ? OR (`key` >= ? AND `key` < ?) ORDER BY `key` ASC LIMIT ?")
			.bind(start.clone())
			.bind(start)
			.bind(rng.end.into())
			// HACK: because sqlx, for some reason, do not have numeric encoding for unsigned values but do have implementations for signed values. 
			// So we are forced to cast to signed integer. Fortunately, we are converting from unsigned to signed,
			// we just need to make sure the casted type is big enough to not have integer overflow
			.bind(limit as i64)
			.map(|row: MySqlRow| {
				(row.get("key"), row.get("value"))
			})
			.fetch_all(tx)
			.await?;
		Ok(result_set)
	}

	async fn get_timestamp<K>(&mut self, key: K, _lock: bool) -> Result<Versionstamp, Error>
	where
		K: Into<Key>,
	{
		// If the transaction is already closed or is read-only, return an error.
		if !self.write {
			Err(Error::TxReadonly)
		} else {
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

			self.set(k, verbytes.to_vec()).await?;
			// Return the uint64 representation of the timestamp as the result
			Ok(verbytes)
		}
	}

	/// Obtain a new key that is suffixed with the change timestamp
	async fn get_versionstamped_key<K>(
		&mut self,
		ts_key: K,
		prefix: K,
		suffix: K,
	) -> Result<Vec<u8>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is writable
		if !self.write {
			Err(Error::TxReadonly)
		} else {
			let ts = self.get_timestamp(ts_key, false).await?;
			let mut k: Vec<u8> = prefix.into();
			k.append(&mut ts.to_vec());
			k.append(&mut suffix.into());
			Ok(k)
		}
	}
}
