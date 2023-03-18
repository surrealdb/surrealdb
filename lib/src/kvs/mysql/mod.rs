#![cfg(feature = "kv-mysql")]

use sea_orm::ConnectionTrait;
use crate::err::Error;

pub(crate) struct Datastore;

impl Datastore {
	/// Open a new database
	pub async fn new(path: &str) -> Result<super::seaorm::Datastore, Error> {
		let db = super::seaorm::Datastore::new(path).await?;

		// HACK: workaround to blob key limit in MySQL and derivatives
		db.db.execute_unprepared(r#"
			CREATE TABLE IF NOT EXISTS kvstore
			(
				`key` LONGBLOB NOT NULL,
				value LONGBLOB NOT NULL,
				CONSTRAINT kvstore_pk
					PRIMARY KEY(`key`(3072)),
				INDEX idx_key (`key`(3072) ASC)
			);
		"#).await?;

		Ok(db)
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::tests::transaction::verify_transaction_isolation;
	use test_log::test;

	#[test(tokio::test(flavor = "multi_thread", worker_threads = 3))]
	async fn mysql_transaction() {
		verify_transaction_isolation("mysql://localhost:3306").await;
	}
}
