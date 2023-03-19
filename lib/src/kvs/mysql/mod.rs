#![cfg(feature = "kv-mysql")]

use crate::err::Error;
use sea_orm::ConnectionTrait;

pub(crate) struct Datastore;

impl Datastore {
	/// Open a new database
	pub async fn new(path: &str) -> Result<super::seaorm::Datastore, Error> {
		let db = super::seaorm::Datastore::new(path).await?;

		// HACK: workaround to blob key limit in MySQL and derivatives
		db.db
			.execute_unprepared(
				r#"
			CREATE TABLE IF NOT EXISTS kvstore
			(
				`key` LONGBLOB NOT NULL,
				value LONGBLOB NOT NULL,
				CONSTRAINT kvstore_pk
					PRIMARY KEY(`key`(3072)),
				INDEX idx_key (`key`(3072) ASC)
			);
		"#,
			)
			.await?;

		Ok(db)
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::tests::transaction::verify_transaction_isolation;
	use std::env;
	use test_log::test;

	const ENV_CONN_STR: &str = "TEST_MYSQL_CONN_STR";

	const DEFAULT_CONN_STR: &str = "root:surrealdb@localhost:3306/surrealdb";

	#[test(tokio::test(flavor = "multi_thread", worker_threads = 3))]
	async fn mysql_transaction() {
		verify_transaction_isolation(&format!(
			"mysql://{}",
			env::var(ENV_CONN_STR).unwrap_or_else(|_| DEFAULT_CONN_STR.to_string())
		))
		.await;
	}
}
