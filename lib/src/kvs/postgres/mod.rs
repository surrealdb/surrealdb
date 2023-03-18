#![cfg(feature = "kv-postgres")]

use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use futures::lock::Mutex;
use sea_orm::prelude::*;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
	AccessMode, ActiveValue, Condition, ConnectOptions, Database, DatabaseConnection,
	DatabaseTransaction, IsolationLevel, QueryOrder, QuerySelect, TransactionTrait,
};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "db")]
pub struct Model {
	#[sea_orm(primary_key, indexed)]
	pub key: Key,
	pub value: Val,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Clone)]
pub struct Datastore {
	db: Pin<Arc<DatabaseConnection>>,
}

pub struct Transaction {
	// Is the transaction complete?
	ok: bool,
	// Is the transaction read+write?
	rw: bool,
	tx: Arc<Mutex<Option<DatabaseTransaction>>>,
}

impl Datastore {
	/// Open a new database
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		let mut opt = ConnectOptions::new(format!("postgres://{}", &path));
		opt.max_connections(100)
			.min_connections(5)
			.sqlx_logging(true)
			.sqlx_logging_level(log::LevelFilter::Trace);
		match Database::connect(opt).await {
			Ok(db) => Ok(Datastore {
				db: Arc::pin(db),
			}),
			Err(e) => Err(Error::Ds(e.to_string())),
		}
	}
	/// Start a new transaction
	pub async fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		// Create a new distributed transaction
		match self
			.db
			.begin_with_config(
				Some(IsolationLevel::RepeatableRead),
				Some(if write {
					AccessMode::ReadWrite
				} else {
					AccessMode::ReadOnly
				}),
			)
			.await
		{
			Ok(tx) => Ok(Transaction {
				ok: false,
				rw: write,
				tx: Arc::new(Mutex::new(Some(tx))),
			}),
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
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();

		Ok(Entity::find_by_id(key).one(tx).await?.is_some())
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
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();

		// Get the key
		let key = key.into();
		Ok(Entity::find_by_id(key).one(tx).await?.map(|x| x.value))
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

		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();

		// Set the key
		let key = key.into();
		let val = val.into();

		Entity::insert(ActiveModel {
			key: ActiveValue::set(key),
			value: ActiveValue::set(val),
		})
		.on_conflict(OnConflict::column(Column::Key).update_column(Column::Value).to_owned())
		.exec(tx)
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
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();

		// Set the key
		let key = key.into();
		let val = val.into();

		Entity::insert(ActiveModel {
			key: ActiveValue::set(key),
			value: ActiveValue::set(val),
		})
		.exec(tx)
		.await?;

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
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();

		// Get the key
		let key = key.into();
		// Get the val
		let val = val.into();
		// Get the check
		let chk = chk.map(Into::into);

		let mut select = Entity::find_by_id(key.to_vec());
		let ok = match chk {
			Some(chk) => {
				select = select.filter(Column::Value.eq(chk));
				select.one(tx).await?.is_some()
			}
			None => select.one(tx).await?.is_none(),
		};

		if ok {
			Entity::insert(ActiveModel {
				key: ActiveValue::set(key),
				value: ActiveValue::set(val),
			})
			.on_conflict(OnConflict::column(Column::Key).update_column(Column::Value).to_owned())
			.exec(tx)
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

		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();

		// Delete the key
		let key = key.into();
		Entity::delete_by_id(key).exec(tx).await?;
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

		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();

		// Get the key
		let key = key.into();
		// Get the check
		let chk = chk.map(Into::into);

		let mut select = Entity::find_by_id(key.to_vec());
		let ok = match chk {
			Some(chk) => {
				select = select.filter(Column::Value.eq(chk));
				select.one(tx).await?.is_some()
			}
			None => select.one(tx).await?.is_none(),
		};

		if ok {
			Entity::delete_by_id(key).exec(tx).await?;
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

		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();

		let start = rng.start.into();
		let end = rng.end.into();

		Ok(Entity::find()
			.filter(Condition::any().add(Column::Key.eq(start.to_vec())).add(
				Condition::all().add(Column::Key.gte(start)).add(Column::Key.lt(end.to_vec())),
			))
			.order_by_asc(Column::Key)
			.limit(Some(limit as u64))
			.all(tx)
			.await?
			.into_iter()
			.map(|x| (x.key, x.value))
			.collect())
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::tests::transaction::verify_transaction_isolation;
	use test_log::test;

	#[test(tokio::test(flavor = "multi_thread", worker_threads = 3))]
	async fn postgres_transaction() {
		verify_transaction_isolation(
			"postgres://localhost:5432/postgres?user=postgres&password=surrealdb",
		)
		.await;
	}
}
