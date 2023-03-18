#![cfg(feature = "kv-mysql")]

use sea_orm::ConnectionTrait;
use crate::err::Error;

pub(crate) struct Datastore;

impl Datastore {
	/// Open a new database
	pub async fn new(path: &str) -> Result<super::seaorm::Datastore, Error> {
		let db = super::seaorm::Datastore::new(path).await?;

		// Unfortunately I have to do this
		db.db.execute_unprepared(r#"
			create table kvstore
			(
				`key` longblob not null,
				value longblob not null,
				constraint kvstore_pk
					primary key (`key`(3072))
			);
		"#).await?;

		db.ensure_indices_exists().await?;

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
