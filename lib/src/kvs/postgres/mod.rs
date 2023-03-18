#![cfg(feature = "kv-postgres")]

use crate::err::Error;

pub(crate) struct Datastore;

impl Datastore {
	/// Open a new database
	pub async fn new(path: &str) -> Result<super::seaorm::Datastore, Error> {
		let db = super::seaorm::Datastore::new(path).await?;
		db.ensure_table_exists().await?;
		db.ensure_indices_exists().await?;
		Ok(db)
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
