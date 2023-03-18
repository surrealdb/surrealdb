#![cfg(feature = "kv-sqlite")]

use crate::err::Error;
use sea_orm::ConnectionTrait;

pub(crate) struct Datastore;

impl Datastore {
	/// Open a new database
	pub async fn new(path: &str) -> Result<super::seaorm::Datastore, Error> {
		let db = super::seaorm::Datastore::new(path).await?;
		db.db.execute_unprepared("PRAGMA journal_mode=WAL").await?;
		Ok(db)
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::tests::transaction::verify_transaction_isolation;
	use test_log::test;

	#[test(tokio::test(flavor = "multi_thread", worker_threads = 3))]
	async fn sqlite_transaction() {
		verify_transaction_isolation("sqlite://test.db?mode=rwc&cache=shared").await;
	}
}
