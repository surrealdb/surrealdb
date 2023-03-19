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
	use std::env;
	use test_log::test;

	const ENV_CONN_STR: &str = "TEST_POSTGRES_CONN_STR";

	const DEFAULT_CONN_STR: &str = "localhost:5432/postgres?user=postgres&password=surrealdb";

	#[test(tokio::test(flavor = "multi_thread", worker_threads = 3))]
	async fn postgres_transaction() {
		verify_transaction_isolation(&format!(
			"postgres://{}",
			env::var(ENV_CONN_STR).unwrap_or_else(|_| DEFAULT_CONN_STR.to_string())
		))
		.await;
	}
}
